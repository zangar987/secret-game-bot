import os
import time
import random
import asyncio
import logging
import sqlite3
from collections import Counter
from typing import Dict, Any

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError, Forbidden, RetryAfter
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes,
    JobQueue, AIORateLimiter
)

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DB_PATH = os.getenv("SECRET_GAME_DB", "secret_game.sqlite3")
LOBBY_TTL = 300
MAX_PLAYERS = 30
MIN_PLAYERS = 5
MAX_ROUNDS = 5

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
log = logging.getLogger("secret-game")

# Active rooms are in memory; persistent leaderboard/statistics are in SQLite.
GAMES: Dict[int, Dict[str, Any]] = {}


def connect_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS player_stats (
            user_id INTEGER PRIMARY KEY,
            display_name TEXT NOT NULL,
            games_played INTEGER NOT NULL DEFAULT 0,
            wins INTEGER NOT NULL DEFAULT 0,
            votes_received INTEGER NOT NULL DEFAULT 0,
            sabotage_successes INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.commit()
    return conn


def init_db():
    conn = connect_db()
    conn.close()


def ensure_stat(user_id: int, name: str):
    conn = connect_db()
    conn.execute(
        "INSERT INTO player_stats(user_id, display_name) VALUES(?, ?) "
        "ON CONFLICT(user_id) DO UPDATE SET display_name=excluded.display_name",
        (user_id, name)
    )
    conn.commit()
    conn.close()


def display_name(user) -> str:
    return user.full_name or user.username or str(user.id)


def new_game(chat_id: int, host_id: int, host_name: str):
    return {
        "id": f"{time.time_ns()}-{random.randint(1000,9999)}",
        "chat_id": chat_id,
        "host_id": host_id,
        "players": {host_id: {"name": host_name, "role": None, "task": None, "dm_ok": False}},
        "phase": "lobby",
        "round": 1,
        "created_at": time.time(),
        "lobby_job": None,
        "phase_job": None,
        "votes": {},
        "sabotage": set(),
        "missions": [],
        "vote_history": [],
        "round_log": [],
        "task_points": Counter(),
        "finished": False,
    }


def role_counts(n: int):
    # Saboteurs: 1 for 5–7 players, 2 for 8–15, 3 for 16–23, 4 for 24–30.
    saboteurs = max(1, min(4, (n - 1) // 8 + 1))
    roles = ["saboteur"] * saboteurs + ["agent"] * (n - saboteurs)
    # Special roles replace agents, never saboteurs.
    if n >= 5:
        roles[roles.index("agent")] = "analyst"
    if n >= 7:
        roles[roles.index("agent")] = "trickster"
    return roles


ROLE_INFO = {
    "agent": ("🕵️ Агент", "Сенің мақсатың — миссияларды сәтті аяқтап, саботажшыларды табу."),
    "saboteur": ("💣 Саботажшы", "Сенің мақсатың — миссияларды бұзу және өз рөліңді жасыру."),
    "analyst": ("🔎 Тергеуші", "Сен агенттер жағындасың. Әр раундта бір ойыншыны тексеріп, нәтижені жеке аласың."),
    "trickster": ("🎭 Айлакер", "Сен агенттер жағындасың. Құпия тапсырмаларды орындап, қосымша ұпай жинауға тырыс.")
}

FUN_TASKS = [
    "Келесі 2 минутта кемінде екі хабарламаңа 😎 қос.",
    "Талқылауда бір рет «Мұнда бір құпия бар...» деп жаз.",
    "Бір ойыншыға күлкілі, бірақ сыпайы лақап ат ойлап тап.",
    "Келесі дауыс беруге дейін үш түрлі эмодзи қолдан.",
    "Чатқа өзіңді детектив ретінде таныстыратын бір сөйлем жаз.",
    "Бір рет «Мен ештеңе білмеймін 👀» деп жаз.",
    "Бір ойыншыны күдікті деп ата да, себебін ойлап тап.",
    "Келесі 2 минутта бір хабарламаңды жұмбақ түрінде жаз.",
    "Топқа қысқа құпия ұран ойлап тап.",
    "Миссия жоспарына бір күлкілі кодтық атау бер."
]

MISSIONS = [
    ("🧩 Құпия код", "Команда 30 секунд ішінде ортақ кодтық сөз ойлап табуы керек."),
    ("🕵️ Күдіктілер тізімі", "Топ ең күдікті үш ойыншыны атап, әрқайсысына бір дәлел келтірсін."),
    ("🤝 Бір шешім", "Барлық ойыншы бір ортақ шешімге келуге тырыссын."),
    ("🔐 Құпия жоспар", "Миссияға арналған үш қадамдық жоспар құрастырыңдар."),
    ("🎭 Сенім сынағы", "Әр ойыншы бір сөйлеммен неліктен оған сенуге болатынын айтсын."),
    ("🧠 Логика раунды", "Топ 60 секунд ішінде бір жұмбақ шешсін: ең бастысы — ортақ жауап.")
]


def cancel_job(job):
    if job:
        try:
            job.schedule_removal()
        except Exception:
            log.exception("Could not cancel job")


def is_group(update: Update) -> bool:
    return bool(update.effective_chat and update.effective_chat.type in ("group", "supergroup"))


async def safe_send(bot, chat_id: int, text: str, **kwargs) -> bool:
    try:
        await bot.send_message(chat_id=chat_id, text=text, **kwargs)
        return True
    except RetryAfter as exc:
        wait = float(getattr(exc, "retry_after", 1))
        await asyncio.sleep(min(wait, 5))
        try:
            await bot.send_message(chat_id=chat_id, text=text, **kwargs)
            return True
        except TelegramError:
            log.warning("Retry send failed for chat %s", chat_id)
            return False
    except (Forbidden, TelegramError):
        log.info("Message could not be sent to %s", chat_id)
        return False


async def safe_edit(query, text: str, **kwargs):
    try:
        await query.edit_message_text(text=text, **kwargs)
    except TelegramError:
        try:
            await query.answer("Бұл батырма ескірген болуы мүмкін.", show_alert=True)
        except TelegramError:
            pass


def game_for(update: Update):
    if not update.effective_chat:
        return None
    return GAMES.get(update.effective_chat.id)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ensure_stat(user.id, display_name(user))
    text = (
        "🎭 SECRET GAME-ге қош келдің!\n\n"
        "Құпия рөлдер, жасырын тапсырмалар, миссиялар және сатқындық.\n"
        "Ойын топ ішінде өтеді; рөлді алу үшін алдымен осы жеке чатта /start бас.\n\n"
        "Топ командалары: /newgame, /join, /players, /startgame, /mission, "
        "/inspect, /task, /vote, /next, /endgame\n"
        "Жеке чатта: /rules, /leaderboard"
    )
    await update.effective_message.reply_text(text)


async def rules(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "📜 SECRET GAME ережелері\n\n"
        "1. Топта /newgame жазып бөлме аш.\n"
        "2. Ойыншылар /join арқылы қосылады (5–30 адам).\n"
        "3. Бөлме 5 минуттан кейін автоматты жабылады, егер ойын басталмаса.\n"
        "4. /startgame рөлдерді жеке хабарламамен таратады.\n"
        "5. /mission миссияны ашады; саботажшылар құпия /sabotage қолданады.\n"
        "6. /inspect — тергеуші бір ойыншыны тексереді.\n"
        "7. /task — жеке күлкілі тапсырмаңды көрсетеді; орындалғаны ойыншылардың келісімімен есептеледі.\n"
        "8. /vote батырмасымен дауыс бер. Бір адам бір раундта бір рет дауыс береді.\n"
        "9. /next раундты аяқтап, нәтижесін шығарады.\n\n"
        "Агенттер 3 сәтті миссиямен жеңеді. Саботажшылар 3 сәтсіз миссиямен жеңеді. "
        "5 раундта ешкім жеңбесе, миссия нәтижесі басым команда жеңеді; тең болса, ойын тең аяқталады."
    )


async def newgame(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_group(update):
        await update.effective_message.reply_text("Ойынды Telegram тобының ішінде аш.")
        return
    chat_id = update.effective_chat.id
    old = GAMES.get(chat_id)
    if old and not old["finished"]:
        await update.effective_message.reply_text("Бұл топта ойын бар. Оны аяқтау үшін /endgame жаз.")
        return
    game = new_game(chat_id, update.effective_user.id, display_name(update.effective_user))
    GAMES[chat_id] = game
    game_id = game["id"]
    game["lobby_job"] = context.job_queue.run_once(
        lobby_expired, LOBBY_TTL, data={"chat_id": chat_id, "game_id": game_id},
        name=f"lobby-{chat_id}-{game_id}"
    )
    await update.effective_message.reply_text(
        "🎭 SECRET GAME лоббиі ашылды!\n\n"
        "Ойынға қосылу үшін төмендегі батырманы басыңдар.\n"
        "👥 5–30 ойыншы | ⏳ 5 минут ішінде бастау керек.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ Ойынға қосылу", callback_data=f"join:{chat_id}:{game_id}")],
            [InlineKeyboardButton("👥 Ойыншылар", callback_data=f"players:{chat_id}:{game_id}")]
        ])
    )


async def lobby_expired(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data
    chat_id, game_id = data["chat_id"], data["game_id"]
    game = GAMES.get(chat_id)
    if not game or game["id"] != game_id or game["phase"] != "lobby":
        return
    GAMES.pop(chat_id, None)
    await safe_send(context.bot, chat_id, "⌛ Лобби уақыты аяқталды. Ойын басталмағандықтан бөлме жабылды. Қайта бастау: /newgame")


async def join_player(game, user):
    if game["phase"] != "lobby":
        return False, "Ойын басталып кетті."
    if user.id in game["players"]:
        return False, "Сен ойынға бұрыннан қосылғансың."
    if len(game["players"]) >= MAX_PLAYERS:
        return False, "Ойыншылар саны 30-ға жетті."
    game["players"][user.id] = {
        "name": display_name(user), "role": None, "task": None,
        "dm_ok": False, "inspected": False, "task_claimed": False
    }
    ensure_stat(user.id, display_name(user))
    return True, f"✅ {display_name(user)} ойынға қосылды! ({len(game['players'])}/{MAX_PLAYERS})"


async def join(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_group(update):
        await update.effective_message.reply_text("Ойынға қосылу үшін топта /join жаз.")
        return
    game = game_for(update)
    if not game:
        await update.effective_message.reply_text("Алдымен /newgame арқылы лобби аш.")
        return
    ok, msg = await join_player(game, update.effective_user)
    await update.effective_message.reply_text(msg)


async def lobby_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    try:
        action, chat_raw, game_id = q.data.split(":", 2)
        chat_id = int(chat_raw)
    except (ValueError, AttributeError):
        return
    game = GAMES.get(chat_id)
    if not game or game["id"] != game_id:
        await q.answer("Бұл лобби жабылған.", show_alert=True)
        return
    if action == "join":
        ok, msg = await join_player(game, q.from_user)
        await safe_send(context.bot, chat_id, msg)
    elif action == "players":
        lines = [f"👥 Ойыншылар: {len(game['players'])}/{MAX_PLAYERS}"]
        lines += [f"{i}. {p['name']}" for i, p in enumerate(game["players"].values(), 1)]
        await safe_send(context.bot, chat_id, "\n".join(lines))


async def players(update: Update, context: ContextTypes.DEFAULT_TYPE):
    game = game_for(update)
    if not game:
        await update.effective_message.reply_text("Белсенді ойын жоқ.")
        return
    lines = [f"👥 Ойыншылар: {len(game['players'])}/{MAX_PLAYERS}"]
    for i, p in enumerate(game["players"].values(), 1):
        lines.append(f"{i}. {p['name']}" + (" — ойында" if p["role"] else " — лобби"))
    await update.effective_message.reply_text("\n".join(lines))


async def startgame(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_group(update):
        await update.effective_message.reply_text("Ойынды топта баста.")
        return
    chat_id = update.effective_chat.id
    game = GAMES.get(chat_id)
    if not game or game["phase"] != "lobby":
        await update.effective_message.reply_text("Белсенді лобби жоқ. /newgame жаз.")
        return
    if len(game["players"]) < MIN_PLAYERS:
        await update.effective_message.reply_text(f"Кемінде {MIN_PLAYERS} ойыншы қажет. Қазір: {len(game['players'])}.")
        return
    cancel_job(game.get("lobby_job"))
    ids = list(game["players"])
    role_list = role_counts(len(ids))
    random.shuffle(role_list)
    for uid, role in zip(ids, role_list):
        player = game["players"][uid]
        player["role"] = role
        player["task"] = random.choice(FUN_TASKS)
    game["phase"] = "mission"
    game["round"] = 1
    game["successes"] = 0
    game["failures"] = 0
    await update.effective_message.reply_text(
        "🚨 ОЙЫН БАСТАЛДЫ!\n\n"
        "Құпия рөлдер жеке хабарламаға жіберілуде. Әр раундта /mission → талқылау → /vote → /next.\n"
        "Рөл келмесе, боттың жеке чатына кіріп /start басып, ұйымдастырушыдан қайта таратуын сұра."
    )
    for uid, p in game["players"].items():
        role, description = ROLE_INFO[p["role"]]
        private = (
            f"🎭 ҚҰПИЯ РӨЛІҢ: {role}\n\n{description}\n\n"
            f"🎯 Жасырын тапсырма: {p['task']}\n\n"
            "Рөліңді басқа ойыншыларға айтпа!"
        )
        sent = await safe_send(context.bot, uid, private)
        p["dm_ok"] = sent
        if not sent:
            await safe_send(context.bot, chat_id,
                f"⚠️ {p['name']} ботқа жеке хабарлама ала алмайды. Боттың жеке чатына кіріп /start басуы керек.")
    await safe_send(context.bot, chat_id, "Алғашқы миссияға дайын болғанда /mission жаз.")


async def mission(update: Update, context: ContextTypes.DEFAULT_TYPE):
    game = game_for(update)
    if not game or game["phase"] not in ("mission", "discussion"):
        await update.effective_message.reply_text("Қазір миссия бастау мүмкін емес.")
        return
    if game["phase"] == "discussion":
        await update.effective_message.reply_text("Бұл раундтың миссиясы ашылған. Талқылауды жалғастырыңдар.")
        return
    title, description = random.choice(MISSIONS)
    game["current_mission"] = (title, description)
    game["sabotage"] = set()
    game["votes"] = {}
    game["phase"] = "discussion"
    game["round_log"].append({"round": game["round"], "mission": title, "sabotage": 0, "votes": {}})
    await update.effective_message.reply_text(
        f"🎯 {game['round']}-РАУНД: {title}\n\n{description}\n\n"
        "Талқылаңдар! Саботажшылар /sabotage қолдана алады. Дауыс беру үшін /vote жазыңдар."
    )


async def sabotage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    game = game_for(update)
    if not game or game["phase"] != "discussion":
        await update.effective_message.reply_text("Алдымен /mission баста.")
        return
    uid = update.effective_user.id
    player = game["players"].get(uid)
    if not player or player["role"] != "saboteur":
        await update.effective_message.reply_text("Бұл әрекет тек саботажшыларға қолжетімді 😉")
        return
    if uid in game["sabotage"]:
        await update.effective_message.reply_text("Бұл раундта саботаж жасап қойдың.")
        return
    game["sabotage"].add(uid)
    await update.effective_message.reply_text("🤫 Саботаж тіркелді. Нәтиже раунд соңында белгілі болады.")


async def inspect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    game = game_for(update)
    if not game or game["phase"] != "discussion":
        await update.effective_message.reply_text("Тергеу үшін алдымен /mission басталсын.")
        return
    uid = update.effective_user.id
    player = game["players"].get(uid)
    if not player or player["role"] != "analyst":
        await update.effective_message.reply_text("Бұл команда тек тергеушіге арналған.")
        return
    if player["inspected"]:
        await update.effective_message.reply_text("Сен тексеруді осы ойында қолданып қойдың.")
        return
    targets = [(pid, p) for pid, p in game["players"].items() if pid != uid]
    buttons = [[InlineKeyboardButton(p["name"], callback_data=f"inspect:{game['chat_id']}:{game['id']}:{pid}")]
               for pid, p in targets]
    await update.effective_message.reply_text("Қай ойыншыны тексересің?", reply_markup=InlineKeyboardMarkup(buttons))


async def inspect_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    try:
        _, chat_raw, game_id, target_raw = q.data.split(":")
        chat_id, target_id = int(chat_raw), int(target_raw)
    except Exception:
        return
    game = GAMES.get(chat_id)
    if not game or game["id"] != game_id or game["phase"] != "discussion":
        await q.answer("Бұл ойын аяқталған немесе раунд өзгерген.", show_alert=True)
        return
    analyst = game["players"].get(q.from_user.id)
    target = game["players"].get(target_id)
    if not analyst or analyst["role"] != "analyst" or analyst["inspected"]:
        await q.answer("Тексеру мүмкін емес.", show_alert=True)
        return
    if not target:
        await q.answer("Ойыншы табылмады.", show_alert=True)
        return
    analyst["inspected"] = True
    result = "💣 Саботажшы" if target["role"] == "saboteur" else "🕵️ Агенттер жағында"
    await safe_send(context.bot, q.from_user.id, f"🔎 Тексеру нәтижесі: {target['name']} — {result}.")
    await safe_edit(q, "✅ Тексеру аяқталды. Нәтиже жеке хабарламаға жіберілді.")


async def task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    game = game_for(update)
    if not game or update.effective_user.id not in game["players"]:
        await update.effective_message.reply_text("Бұл топтағы ойыншы емессің.")
        return
    p = game["players"][update.effective_user.id]
    await update.effective_message.reply_text(
        f"🎯 Сенің құпия тапсырмаң:\n{p['task']}\n\n"
        "Орындалған соң /taskdone жаз. Бұл — достардың келісімімен берілетін honor-system ұпай."
    )


async def taskdone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    game = game_for(update)
    if not game or update.effective_user.id not in game["players"]:
        await update.effective_message.reply_text("Белсенді ойында жоқсың.")
        return
    p = game["players"][update.effective_user.id]
    if p["task_claimed"]:
        await update.effective_message.reply_text("Бұл ойында тапсырма ұпайын белгілеп қойдың.")
        return
    p["task_claimed"] = True
    game["task_points"][update.effective_user.id] += 1
    await update.effective_message.reply_text("🎉 Тапсырма орындалды деп белгіленді! Ұпай жүйесі ойыншылардың адал келісіміне негізделеді.")


async def vote(update: Update, context: ContextTypes.DEFAULT_TYPE):
    game = game_for(update)
    if not game or game["phase"] != "discussion":
        await update.effective_message.reply_text("Дауыс беру үшін белсенді миссия керек.")
        return
    uid = update.effective_user.id
    if uid not in game["players"]:
        await update.effective_message.reply_text("Бұл ойынға қосылмағансың.")
        return
    if uid in game["votes"]:
        await update.effective_message.reply_text("Сен бұл раундта дауыс беріп қойдың.")
        return
    buttons = [
        [InlineKeyboardButton(p["name"], callback_data=f"vote:{game['chat_id']}:{game['id']}:{target_id}")]
        for target_id, p in game["players"].items() if target_id != uid
    ]
    await update.effective_message.reply_text("🗳 Кімге күдіктенесің? Бір ойыншыны таңда:", reply_markup=InlineKeyboardMarkup(buttons))


async def vote_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    try:
        _, chat_raw, game_id, target_raw = q.data.split(":")
        chat_id, target_id = int(chat_raw), int(target_raw)
    except Exception:
        return
    game = GAMES.get(chat_id)
    if not game or game["id"] != game_id or game["phase"] != "discussion":
        await q.answer("Дауыс беру жабық.", show_alert=True)
        return
    voter_id = q.from_user.id
    if voter_id not in game["players"] or target_id not in game["players"]:
        await q.answer("Бұл ойынға қатыспайсың.", show_alert=True)
        return
    if target_id == voter_id:
        await q.answer("Өзіңе дауыс бере алмайсың.", show_alert=True)
        return
    if voter_id in game["votes"]:
        await q.answer("Бір раундта бір рет қана дауыс беруге болады.", show_alert=True)
        return
    game["votes"][voter_id] = target_id
    game["round_log"][-1]["votes"][voter_id] = target_id
    await safe_edit(q, "✅ Дауысың жасырын түрде қабылданды.")


def persist_finished_game(game, winner_side):
    conn = connect_db()
    for uid, p in game["players"].items():
        win = 1 if ((winner_side == "agents" and p["role"] != "saboteur") or
                    (winner_side == "saboteurs" and p["role"] == "saboteur")) else 0
        votes_received = sum(1 for target in game["votes"].values() if target == uid)
        sabotage_successes = len(game["sabotage"]) if p["role"] == "saboteur" else 0
        conn.execute("""
            INSERT INTO player_stats(user_id, display_name, games_played, wins, votes_received, sabotage_successes)
            VALUES(?, ?, 1, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
              display_name=excluded.display_name,
              games_played=games_played+1,
              wins=wins+excluded.wins,
              votes_received=votes_received+excluded.votes_received,
              sabotage_successes=sabotage_successes+excluded.sabotage_successes
        """, (uid, p["name"], win, votes_received, sabotage_successes))
    conn.commit()
    conn.close()


async def finish_game(context, game, reason="Ойын аяқталды"):
    if game.get("finished"):
        return
    game["finished"] = True
    cancel_job(game.get("lobby_job"))
    cancel_job(game.get("phase_job"))
    agents, saboteurs = game["successes"], game["failures"]
    if agents >= 3:
        winner_side, winner_text = "agents", "🕵️ АГЕНТТЕР"
    elif saboteurs >= 3:
        winner_side, winner_text = "saboteurs", "💣 САБОТАЖШЫЛАР"
    elif agents > saboteurs:
        winner_side, winner_text = "agents", "🕵️ АГЕНТТЕР"
    elif saboteurs > agents:
        winner_side, winner_text = "saboteurs", "💣 САБОТАЖШЫЛАР"
    else:
        winner_side, winner_text = "draw", "🤝 ТЕҢ НӘТИЖЕ"
    if winner_side != "draw":
        persist_finished_game(game, winner_side)
    lines = [
        "🏁 SECRET GAME — ОЙЫН ҚОРЫТЫНДЫСЫ",
        f"📌 Себебі: {reason}",
        f"🏆 Нәтиже: {winner_text}",
        f"✅ Сәтті миссиялар: {agents}",
        f"💣 Саботаж болған миссиялар: {saboteurs}",
        f"🎯 Ойын раундтары: {len(game['round_log'])}",
        "",
        "🎭 ОЙЫНШЫЛАРДЫҢ РӨЛДЕРІ:"
    ]
    for p in game["players"].values():
        role_name = ROLE_INFO[p["role"]][0] if p["role"] else "Рөл берілмеді"
        lines.append(f"• {p['name']} — {role_name} | тапсырма ұпайы: {game['task_points'].get(next((uid for uid, item in game['players'].items() if item is p), -1), 0)}")
    lines += ["", "🗳 СОҢҒЫ РАУНД ДАУЫСЫ:"]
    last_votes = game["votes"]
    if last_votes:
        counts = Counter(last_votes.values())
        for target_id, count in counts.most_common():
            target = game["players"].get(target_id)
            if target:
                lines.append(f"• {target['name']} — {count} дауыс")
    else:
        lines.append("Бұл раундта дауыс берілмеді.")
    lines += ["", "📊 Әр раунд:"]
    for entry in game["round_log"]:
        status = "💣 саботаж" if entry["sabotage"] else "—"
        lines.append(f"• {entry['round']}-раунд: {entry['mission']} ({status})")
    lines.append("\\nЖаңа ойын бастау үшін /newgame жазыңдар.")
    await safe_send(context.bot, game["chat_id"], "\\n".join(lines))
    GAMES.pop(game["chat_id"], None)


async def next_round(update: Update, context: ContextTypes.DEFAULT_TYPE):
    game = game_for(update)
    if not game or game["phase"] != "discussion":
        await update.effective_message.reply_text("Қазір аяқтайтын белсенді раунд жоқ.")
        return
    sabotaged = bool(game["sabotage"])
    if sabotaged:
        game["failures"] += 1
    else:
        game["successes"] += 1
    game["round_log"][-1]["sabotage"] = len(game["sabotage"])
    game["round_log"][-1]["vote_counts"] = dict(Counter(game["votes"].values()))
    vote_counts = Counter(game["votes"].values())
    vote_text = "Дауыс берілген жоқ."
    if vote_counts:
        vote_text = "🗳 Дауыс нәтижесі: " + ", ".join(
            f"{game['players'][pid]['name']} — {count}" for pid, count in vote_counts.most_common()
        )
    result = "💣 Миссия саботажға ұшырады!" if sabotaged else "✅ Миссия сәтті өтті!"
    await update.effective_message.reply_text(
        f"{result}\\n{vote_text}\\n\\n📊 Сәтті: {game['successes']} | Сәтсіз: {game['failures']}"
    )
    if game["successes"] >= 3:
        await finish_game(context, game, "Агенттер 3 сәтті миссия орындады.")
        return
    if game["failures"] >= 3:
        await finish_game(context, game, "Саботажшылар 3 миссияны бұзды.")
        return
    if game["round"] >= MAX_ROUNDS:
        await finish_game(context, game, "5 раунд аяқталды.")
        return
    game["round"] += 1
    game["phase"] = "mission"
    game["votes"] = {}
    game["sabotage"] = set()
    await update.effective_message.reply_text(f"➡️ {game['round']}-раундқа дайынсыңдар. /mission жазыңдар.")


async def endgame(update: Update, context: ContextTypes.DEFAULT_TYPE):
    game = game_for(update)
    if not game:
        await update.effective_message.reply_text("Белсенді ойын жоқ.")
        return
    if game["phase"] == "lobby":
        cancel_job(game.get("lobby_job"))
        GAMES.pop(game["chat_id"], None)
        await update.effective_message.reply_text("🛑 Лобби жабылды.")
        return
    await finish_game(context, game, "Ойын ұйымдастырушының пәрменімен тоқтатылды.")


async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = connect_db()
    rows = conn.execute(
        "SELECT display_name, games_played, wins, votes_received, sabotage_successes "
        "FROM player_stats ORDER BY wins DESC, games_played DESC LIMIT 10"
    ).fetchall()
    conn.close()
    if not rows:
        await update.effective_message.reply_text("Әзірге рейтинг бос. Ойынды бастап, алғашқы жеңісіңді ал!")
        return
    lines = ["🏆 SECRET GAME — ТОП 10"]
    for i, (name, games_played, wins, votes, sabotages) in enumerate(rows, 1):
        lines.append(f"{i}. {name} — 🏆 {wins} жеңіс / 🎮 {games_played} ойын / 🗳 {votes} дауыс алған / 💣 {sabotages} саботаж")
    await update.effective_message.reply_text("\\n".join(lines))


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    log.error("Unhandled exception: %s", context.error, exc_info=context.error)
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text("⚠️ Қате пайда болды. Қайта әрекет жасап көр немесе /rules оқы.")
        except TelegramError:
            pass


def main():
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN табылмады. Хостингтің Environment Variables бөлімінде орнат.")
    init_db()
    app = (
        Application.builder()
        .token(TOKEN)
        .rate_limiter(AIORateLimiter())
        .build()
    )
    handlers = [
        CommandHandler("start", start),
        CommandHandler("rules", rules),
        CommandHandler("newgame", newgame),
        CommandHandler("join", join),
        CommandHandler("players", players),
        CommandHandler("startgame", startgame),
        CommandHandler("mission", mission),
        CommandHandler("sabotage", sabotage),
        CommandHandler("inspect", inspect),
        CommandHandler("task", task),
        CommandHandler("taskdone", taskdone),
        CommandHandler("vote", vote),
        CommandHandler("next", next_round),
        CommandHandler("endgame", endgame),
        CommandHandler("leaderboard", leaderboard),
    ]
    for handler in handlers:
        app.add_handler(handler)
    app.add_handler(CallbackQueryHandler(lobby_callback, pattern=r"^(join|players):"))
    app.add_handler(CallbackQueryHandler(vote_callback, pattern=r"^vote:"))
    app.add_handler(CallbackQueryHandler(inspect_callback, pattern=r"^inspect:"))
    app.add_error_handler(error_handler)
    log.info("SECRET GAME бот іске қосылды.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
