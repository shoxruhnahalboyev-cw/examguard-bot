import asyncio
import hashlib
import html
import logging
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, time as dtime, timedelta
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import aiosqlite
import pytz
from aiohttp import web
from aiogram import BaseMiddleware, Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    ChatMemberUpdated,
    FSInputFile,
    InputMediaDocument,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# ============================================================================
# SOZLAMALAR
# ============================================================================
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    sys.exit("❌ BOT_TOKEN topilmadi. .env faylga BOT_TOKEN=... deb yozing.")

CREATOR_ID = int(os.getenv("CREATOR_ID", "7439126820"))

DB_NAME = os.getenv("DB_PATH", "examguard.db")   # VPS da: /var/lib/examguard/examguard.db
TZ = pytz.timezone("Asia/Tashkent")
UTC = pytz.utc
CHECK_INTERVAL = 2            # skeyuler tekshiruv oralig'i (soniya)
BAN_DAYS = 3
WARN_MINUTES = 30             # tugashiga necha daqiqa qolganda ogohlantirish
DURATION_TOLERANCE = 0        # 0 = aynan bir xil soniya; 1 = ±1 soniya ham bir xil hisoblanadi
ALLOW_FORWARDED_VOICE = False
ADMIN_CACHE_TTL = 60          # adminlik tekshiruvi keshi (soniya)
GROUP_TYPES = ("group", "supergroup")

# --- Zaxira nusxa (Telegram orqali) va ishga tushish sozlamalari ---
BACKUP_ENABLED = os.getenv("BACKUP_ENABLED", "1") != "0"
BACKUP_CHAT_ID = int(os.getenv("BACKUP_CHAT_ID", str(CREATOR_ID)))  # odatda yaratuvchining shaxsiy chati
BACKUP_INTERVAL = 300         # o'zgarish bo'lmasa ham har 5 daqiqada tekshirib ko'radi
BACKUP_MIN_GAP = 20          # ketma-ket yuklashlar orasidagi minimal pauza (soniya)
STARTUP_GRACE = 60           # qayta yoqilgandan keyin skeyuler kutish vaqti: navbatdagi javoblar ulgurib qolsin
BACKUP_FILENAME = "examguard_backup.db"

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

TASK_RE = re.compile(r"^(?:(\d+)\s+)?(\d{1,2}):(\d{2})\s+(.+)$", re.S)
ANSWER_RE = re.compile(r"^/answer(?:@\w+)?(?:\s+(.*))?$", re.I | re.S)
TASK_CAPTION_RE = re.compile(r"^/task(?:@\w+)?(?:\s|$)", re.I)

esc = html.escape

# ============================================================================
# BAZA: bitta doimiy ulanish + keshlar
# ============================================================================
_db_conn: Optional[aiosqlite.Connection] = None
_db_lock = asyncio.Lock()
_approved_groups_cache: set = set()
_seen: set = set()
_admin_cache: Dict[Tuple[int, int], Tuple[bool, float]] = {}

SCHEMA_COLUMNS: Dict[str, List[Tuple[str, str]]] = {
    "groups": [
        ("teacher_id", "INTEGER"),
        ("teacher_username", "TEXT"),
        ("teacher_name", "TEXT"),
        ("is_approved", "INTEGER DEFAULT 0"),
    ],
    "tasks": [
        ("chat_id", "INTEGER"),
        ("task_text", "TEXT"),
        ("deadline", "TIMESTAMP"),
        ("is_active", "INTEGER DEFAULT 1"),
        ("warned_30", "INTEGER DEFAULT 0"),
        ("created_at", "TIMESTAMP"),
        ("photo_id", "TEXT"),
    ],
    "group_members": [
        ("full_name", "TEXT"),
    ],
    "answers": [
        ("chat_id", "INTEGER"),
        ("user_id", "INTEGER"),
        ("task_id", "INTEGER"),
        ("full_name", "TEXT"),
        ("question_num", "TEXT"),
        ("answer_type", "TEXT"),
        ("content", "TEXT"),
        ("file_unique_id", "TEXT"),
        ("duration", "INTEGER DEFAULT 0"),
        ("submitted_at", "TIMESTAMP"),
    ],
    "banned_users": [
        ("full_name", "TEXT"),
        ("reason", "TEXT"),
        ("banned_at", "TIMESTAMP"),
        ("unban_at", "TIMESTAMP"),
    ],
}


async def get_db() -> aiosqlite.Connection:
    global _db_conn
    if _db_conn is None:
        os.makedirs(os.path.dirname(os.path.abspath(DB_NAME)), exist_ok=True)
        _db_conn = await aiosqlite.connect(DB_NAME)
        await _db_conn.execute("PRAGMA journal_mode=WAL;")
        await _db_conn.execute("PRAGMA synchronous=NORMAL;")
        await _db_conn.execute("PRAGMA temp_store=MEMORY;")
    return _db_conn


async def q_one(sql: str, params: tuple = ()):
    db = await get_db()
    async with db.execute(sql, params) as cur:
        return await cur.fetchone()


async def q_all(sql: str, params: tuple = ()):
    db = await get_db()
    async with db.execute(sql, params) as cur:
        return await cur.fetchall()


async def run(sql: str, params: tuple = ()) -> int:
    db = await get_db()
    cur = await db.execute(sql, params)
    await db.commit()
    request_backup()
    return cur.rowcount


async def table_columns(table: str) -> List[str]:
    return [row[1] for row in await q_all(f"PRAGMA table_info({table})")]


async def ensure_column(table: str, column: str, ddl: str) -> bool:
    if column in await table_columns(table):
        return False
    db = await get_db()
    await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
    logging.info(f"Migratsiya: {table}.{column} qo'shildi")
    return True


async def init_db():
    db = await get_db()

    await db.execute("""
        CREATE TABLE IF NOT EXISTS groups (
            chat_id INTEGER PRIMARY KEY,
            teacher_id INTEGER,
            teacher_username TEXT,
            teacher_name TEXT,
            is_approved INTEGER DEFAULT 0
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            task_text TEXT,
            deadline TIMESTAMP,
            is_active INTEGER DEFAULT 1,
            warned_30 INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            photo_id TEXT
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS group_members (
            chat_id INTEGER,
            user_id INTEGER,
            full_name TEXT,
            PRIMARY KEY (chat_id, user_id)
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS answers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            task_id INTEGER,
            user_id INTEGER,
            full_name TEXT,
            question_num TEXT,
            answer_type TEXT,
            content TEXT,
            file_unique_id TEXT,
            duration INTEGER DEFAULT 0,
            submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS banned_users (
            chat_id INTEGER,
            user_id INTEGER,
            full_name TEXT,
            reason TEXT,
            banned_at TIMESTAMP,
            unban_at TIMESTAMP,
            PRIMARY KEY (chat_id, user_id)
        )
    """)

    for table, columns in SCHEMA_COLUMNS.items():
        for column, ddl in columns:
            added = await ensure_column(table, column, ddl)
            if added and table == "groups" and column == "is_approved":
                await db.execute("UPDATE groups SET is_approved = 1")

    await db.execute("UPDATE tasks SET created_at = '2000-01-01 00:00:00' WHERE created_at IS NULL")
    await db.execute("UPDATE answers SET submitted_at = datetime('now') WHERE submitted_at IS NULL")

    for table in ("group_members", "answers"):
        if "username" in await table_columns(table):
            await db.execute(
                f"UPDATE {table} SET full_name = username "
                f"WHERE (full_name IS NULL OR full_name = '') AND username IS NOT NULL AND username != ''"
            )
        await db.execute(f"UPDATE {table} SET full_name = 'O''quvchi' WHERE full_name IS NULL OR full_name = ''")

    await db.execute("CREATE INDEX IF NOT EXISTS idx_tasks_active ON tasks(is_active, deadline)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_tasks_chat ON tasks(chat_id, is_active)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_answers_task_user ON answers(task_id, user_id)")
    await db.execute("CREATE INDEX IF NOT EXISTS idx_answers_unique ON answers(task_id, file_unique_id)")
    await db.commit()

    for (chat_id,) in await q_all("SELECT chat_id FROM groups WHERE is_approved = 1"):
        _approved_groups_cache.add(chat_id)

    print("📁 Ma'lumotlar bazasi eskisini to'liq saqlagan holda xavfsiz yangilandi!")


async def set_bot_commands():
    await bot.set_my_commands([
        BotCommand(command="start", description="Botni ishga tushirish"),
        BotCommand(command="help", description="Qo'llanma"),
        BotCommand(command="rules", description="Qoidalar"),
        BotCommand(command="task", description="[Ustoz] Vazifa berish"),
        BotCommand(command="cancel_task", description="[Ustoz] Bekor qilish"),
        BotCommand(command="tasks", description="Faol vazifa"),
        BotCommand(command="answer", description="[O'quvchi] Voice javob"),
        BotCommand(command="submissions", description="Topshirganlar"),
        BotCommand(command="banned_list", description="[Ustoz] Ban qilinganlar"),
        BotCommand(command="extend", description="[Ustoz] 1 soat qo'shish"),
    ])


# ============================================================================
# YORDAMCHI FUNKSIYALAR
# ============================================================================
def now_local() -> datetime:
    return datetime.now(TZ)


def fmt_local(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M")


def utc_str() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def parse_deadline(deadline_str: str) -> datetime:
    return TZ.localize(datetime.strptime(deadline_str, "%Y-%m-%d %H:%M"))


def parse_utc(value: str) -> Optional[datetime]:
    try:
        return UTC.localize(datetime.strptime(value, "%Y-%m-%d %H:%M:%S"))
    except Exception:
        return None


def raw_name(user) -> str:
    if not user:
        return "O'quvchi"
    name = (user.first_name or "").strip()
    if user.last_name:
        name = f"{name} {user.last_name}".strip()
    if name:
        return name
    return f"@{user.username}" if user.username else "O'quvchi"


def label(user) -> str:
    return esc(raw_name(user))


def is_forwarded(msg: Message) -> bool:
    return bool(getattr(msg, "forward_origin", None) or getattr(msg, "forward_date", None))


async def is_user_admin(message: Message) -> bool:
    if message.chat.type == "private":
        return False
    if message.sender_chat and message.sender_chat.id == message.chat.id:
        return True
    if not message.from_user:
        return False

    key = (message.chat.id, message.from_user.id)
    cached = _admin_cache.get(key)
    if cached and cached[1] > time.monotonic():
        return cached[0]

    try:
        member = await bot.get_chat_member(message.chat.id, message.from_user.id)
        result = member.status in ("administrator", "creator")
    except Exception as e:
        logging.error(f"Adminlikni tekshirishda xatolik: {e}")
        return False
    _admin_cache[key] = (result, time.monotonic() + ADMIN_CACHE_TTL)
    return result


async def is_group_approved(chat_id: int) -> bool:
    if chat_id in _approved_groups_cache:
        return True
    row = await q_one("SELECT is_approved FROM groups WHERE chat_id = ?", (chat_id,))
    if row and row[0] == 1:
        _approved_groups_cache.add(chat_id)
        return True
    return False


async def get_active_task(chat_id: int):
    return await q_one(
        "SELECT id, task_text, deadline, created_at, photo_id FROM tasks "
        "WHERE chat_id = ? AND is_active = 1 ORDER BY id DESC LIMIT 1",
        (chat_id,),
    )


async def group_guard(message: Message, admin_only: bool = False, allow_private: bool = False) -> bool:
    if message.chat.type == "private":
        if allow_private:
            return True
        await message.answer("⚠️ Bu buyruq faqat guruhda ishlaydi.")
        return False

    if not await is_group_approved(message.chat.id):
        await message.reply(
            "⚠️ Bu guruh bot yaratuvchisi tomonidan hali tasdiqlanmagan. Tasdiq kelguncha buyruqlar ishlamaydi."
        )
        return False

    if admin_only and not await is_user_admin(message):
        await message.reply(
            "⛔️ Bu buyruq faqat guruh <b>Ustoziga</b> (administratorga) tegishli. "
            "Ustoz stuliga o'tirish uchun avval imtihondan o'tish kerak 😉"
        )
        return False
    return True


async def send_chunks(message: Message, header: str, lines: List[str]):
    chunk = header
    for line in lines:
        if len(chunk) + len(line) + 2 > 3800:
            await message.answer(chunk)
            chunk = ""
        chunk = f"{chunk}\n{line}" if chunk else line
    if chunk:
        await message.answer(chunk)


async def send_task_card(chat_id: int, header: str, task_text: str, deadline_str: str,
                         photo_id: Optional[str], footer: str):
    text = (
        f"{header}\n\n{esc(task_text)}\n\n"
        f"⏳ <b>Topshirish muddati:</b> <code>{deadline_str}</code>\n{footer}"
    )
    if photo_id:
        if len(text) <= 1024:
            await bot.send_photo(chat_id, photo_id, caption=text)
            return
        await bot.send_photo(chat_id, photo_id)
    await bot.send_message(chat_id, text)


def task_question_numbers(task_text: str) -> set:
    return {str(int(n)) for n in re.findall(r"(?:^|\s)(\d{1,2})\s*[.)]", task_text)}


# ============================================================================
# ANTICHEAT + JAVOBNI SAQLASH
# ============================================================================
async def save_answer(chat_id: int, task_id: int, user, question_num: str, voice) -> Tuple[bool, str]:
    db = await get_db()
    async with _db_lock:
        if await q_one("SELECT 1 FROM tasks WHERE id = ? AND is_active = 1", (task_id,)) is None:
            return False, "no_task"

        rows = await q_all(
            "SELECT question_num, content, file_unique_id, duration FROM answers "
            "WHERE chat_id = ? AND task_id = ? AND user_id = ?",
            (chat_id, task_id, user.id),
        )

        same_question = False
        for q_num, content, unique_id, dur in rows:
            if q_num == question_num:
                same_question = True
                continue
            if content == voice.file_id or (unique_id and unique_id == voice.file_unique_id):
                return False, "duplicate_file"
            if dur and abs(dur - voice.duration) <= DURATION_TOLERANCE:
                return False, "duplicate_duration"

        if await q_one(
            "SELECT 1 FROM answers WHERE chat_id = ? AND task_id = ? AND user_id != ? AND file_unique_id = ?",
            (chat_id, task_id, user.id, voice.file_unique_id),
        ):
            return False, "other_user_file"

        if same_question:
            await db.execute(
                "UPDATE answers SET content = ?, file_unique_id = ?, duration = ?, full_name = ?, submitted_at = ? "
                "WHERE chat_id = ? AND task_id = ? AND user_id = ? AND question_num = ?",
                (voice.file_id, voice.file_unique_id, voice.duration, raw_name(user), utc_str(),
                 chat_id, task_id, user.id, question_num),
            )
            await db.commit()
            request_backup()
            return True, "updated"

        await db.execute(
            "INSERT INTO answers (chat_id, task_id, user_id, full_name, question_num, answer_type, content, "
            "file_unique_id, duration, submitted_at) VALUES (?, ?, ?, ?, ?, 'voice', ?, ?, ?, ?)",
            (chat_id, task_id, user.id, raw_name(user), question_num,
             voice.file_id, voice.file_unique_id, voice.duration, utc_str()),
        )
        await db.commit()
    request_backup()
    return True, "success"


async def process_answer(message: Message, args_text: str):
    user = message.from_user
    name = label(user)
    args = (args_text or "").split()

    if not args or not re.fullmatch(r"[0-9]{1,2}", args[0]):
        await message.reply(
            f"⚠️ Hurmatli <b>{name}</b>, qaysi savolga javob berayotganingiz ko'rsatilmagan.\n"
            "<b>To'g'ri format:</b> o'zingizning ovozli xabaringizga reply qilib <code>/answer 1</code> yuboring."
        )
        return

    question_num = str(int(args[0]))
    task = await get_active_task(message.chat.id)
    if not task:
        await message.reply(f"⚠️ <b>{name}</b>, hozirda faol vazifa yo'q. Shoshilmang, Ustoz e'lon qiladi.")
        return
    task_id, task_text, deadline_str, created_at, _ = task

    numbers = task_question_numbers(task_text)
    if numbers and question_num not in numbers:
        await message.reply(
            f"⚠️ <b>{name}</b>, vazifada <b>{question_num}-savol</b> yo'q. "
            f"Mavjud savollar: <b>{', '.join(sorted(numbers, key=int))}</b>."
        )
        return

    try:
        deadline_dt = parse_deadline(deadline_str)
    except ValueError:
        deadline_dt = None
    msg_date = message.date
    if msg_date and msg_date.tzinfo is None:
        msg_date = UTC.localize(msg_date)
    if deadline_dt and msg_date and msg_date > deadline_dt + timedelta(seconds=5):
        await message.reply(f"⛔️ <b>{name}</b>, javob muddatdan keyin yuborilgan, qabul qilinmaydi.")
        return

    voice_msg = None
    if message.voice:
        voice_msg = message
    elif message.reply_to_message and message.reply_to_message.voice:
        voice_msg = message.reply_to_message

    if not voice_msg:
        await message.reply(
            f"⚠️ <b>{name}</b>, imtihonda matn emas, ovoz baholanadi! 🎙\n"
            f"Ovozli xabar yuboring va unga reply qilib <code>/answer {question_num}</code> deb yozing."
        )
        return

    voice = voice_msg.voice

    if voice_msg.from_user and voice_msg.from_user.id != user.id:
        await message.reply(f"⛔️ <b>{name}</b>, faqat o'zingizning ovozli xabaringizga reply qilishingiz mumkin.")
        return

    if not ALLOW_FORWARDED_VOICE and is_forwarded(voice_msg):
        await message.reply(
            f"⛔️ <b>{name}</b>, forward qilingan ovozli xabar qabul qilinmaydi. Iltimos, o'zingiz yangidan yozing."
        )
        return

    created = parse_utc(created_at) if created_at else None
    voice_date = voice_msg.date
    if voice_date and voice_date.tzinfo is None:
        voice_date = UTC.localize(voice_date)
    if created and voice_date and voice_date < created - timedelta(seconds=5):
        await message.reply(
            f"⛔️ <b>{name}</b>, bu ovoz vazifa e'lon qilinishidan oldin yozilgan. Vazifadan keyin yangi ovoz yozing."
        )
        return

    success, err = await save_answer(message.chat.id, task_id, user, question_num, voice)
    if success and err == "updated":
        await message.reply(
            f"🔄 <b>{name}</b>, <b>{question_num}-savol</b> javobingiz yangisiga almashtirildi ({voice.duration} sek)."
        )
    elif success:
        await message.reply(
            f"✅ <b>{name}</b>, <b>{question_num}-savol</b> javobingiz qabul qilindi ({voice.duration} sek). Davom eting!"
        )
    elif err in ("duplicate_file", "duplicate_duration"):
        await message.reply(
            f"⚠️ <b>{name}</b>, bu ovozli xabarni (yoki bu soniyadagi xabarni) allaqachon boshqa savolga ishlatgansiz, "
            f"har bir savolga alohida har xil voice yuboring!"
        )
    elif err == "other_user_file":
        await message.reply(f"⛔️ <b>{name}</b>, bu ovoz boshqa o'quvchi tomonidan yuborilgan. Faqat o'zingiznikini yuboring.")
    elif err == "no_task":
        await message.reply("⚠️ Vazifa muddati tugagan yoki bekor qilingan. Javob qabul qilinmadi.")
    else:
        await message.reply("⚠️ Xatolik yuz berdi, qaytadan urinib ko'ring.")


# ============================================================================
# BOT GURUHGA QO'SHILISHI / CHIQISHI (Yaratuvchi o'zi qo'shsa ruxsat so'ralmaydi)
# ============================================================================
def intro_text() -> str:
    return (
        "🛡 <b>ExamGuard AI guruhga ulandi va faollashdi!</b>\n\n"
        "🤖 <b>Vazifamiz:</b> CEFR Speaking va Writing tayyorgarligi hamda qat'iy intizomni nazorat qilish.\n\n"
        "👨‍🏫 <b>Ustoz uchun:</b> <code>/task 21:00 1. What is your hobby?</code>\n"
        "(yoki rasm yuborib, izohiga shuni yozing)\n"
        "👩‍🎓 <b>O'quvchilar uchun:</b> o'zingizning ovozli xabaringizga reply qilib "
        "<code>/answer &lt;raqam&gt;</code> yuboring.\n"
        "⚠️ <b>Anticheat:</b> har bir savolga alohida va har xil soniyadagi ovoz yuboring.\n"
        f"⚠️ <i>Qoida qat'iy: muddatida topshirmaganlar {BAN_DAYS} kunga chetlashtiriladi.</i>\n\n"
        "Batafsil: /help"
    )


@dp.my_chat_member()
async def bot_status_changed(event: ChatMemberUpdated):
    if event.chat.type not in GROUP_TYPES:
        return

    chat_id = event.chat.id
    old_status = event.old_chat_member.status
    new_status = event.new_chat_member.status

    if new_status in ("left", "kicked"):
        _approved_groups_cache.discard(chat_id)
        await run("UPDATE groups SET is_approved = 0 WHERE chat_id = ?", (chat_id,))
        return

    if not (old_status in ("left", "kicked") and new_status in ("member", "administrator")):
        return

    chat_title = event.chat.title or "Noma'lum guruh"
    adder = event.from_user
    adder_id = adder.id if adder else 0
    adder_name = raw_name(adder) if adder else "Noma'lum"
    adder_username = f"@{adder.username}" if adder and adder.username else "yo'q"

    _approved_groups_cache.discard(chat_id)

    # 1-TALABING: AGAR YARATUVCHI (CREATOR_ID) O'ZI GURUHGAGA QO'SHSA — RUXSAT SO'RALMAYDI, DARHOL TASDIQLANADI!
    if adder_id == CREATOR_ID:
        await run(
            "INSERT INTO groups (chat_id, teacher_id, teacher_username, teacher_name, is_approved) "
            "VALUES (?, ?, ?, ?, 1) "
            "ON CONFLICT(chat_id) DO UPDATE SET is_approved = 1",
            (chat_id, adder_id, adder_username, adder_name),
        )
        _approved_groups_cache.add(chat_id)
        try:
            await bot.send_message(chat_id, intro_text())
        except Exception as e:
            logging.error(f"Guruhga intro yuborishda xato: {e}")
        return

    # AGAR BOSHQA ODAM QO'SHSA — RUXSAT SO'RALIB TUGMA YUBORILADI
    await run(
        "INSERT INTO groups (chat_id, teacher_id, teacher_username, teacher_name, is_approved) "
        "VALUES (?, 0, 'Ustoz', 'Ustoz', 0) "
        "ON CONFLICT(chat_id) DO UPDATE SET is_approved = 0",
        (chat_id,),
    )

    try:
        await bot.send_message(
            chat_id,
            "🛡 <b>ExamGuard AI</b> guruhga qo'shildi.\n\n"
            "⚠️ <i>Xavfsizlik talabiga ko'ra, botni ishlatish uchun yaratuvchining tasdig'i kutilmoqda.</i>",
        )
    except Exception as e:
        logging.error(f"Guruhga xabar yuborishda xato: {e}")

    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Ruxsat berish", callback_data=f"approve_{chat_id}"),
        InlineKeyboardButton(text="❌ Rad etish", callback_data=f"reject_{chat_id}"),
    ]])
    try:
        await bot.send_message(
            CREATOR_ID,
            f"🔔 <b>Yangi guruhdan botni qo'shish so'rovi!</b>\n\n"
            f"👤 <b>Qo'shgan shaxs:</b> {esc(adder_name)} (ID: <code>{adder_id}</code>, {esc(adder_username)})\n"
            f"📌 <b>Guruh nomi:</b> {esc(chat_title)}\n"
            f"🆔 <b>Chat ID:</b> <code>{chat_id}</code>\n\n"
            f"Tasdiqlaysizmi?",
            reply_markup=keyboard,
        )
    except Exception as e:
        logging.error(f"Yaratuvchiga xabar yuborib bo'lmadi: {e}")
        try:
            await bot.send_message(
                chat_id,
                "⚠️ Yaratuvchiga so'rov yetib bormadi. Yaratuvchi avval botga shaxsiy chatda /start yuborishi kerak.",
            )
        except Exception:
            pass


@dp.callback_query(F.data.startswith("approve_"))
async def approve_group(call: CallbackQuery):
    if call.from_user.id != CREATOR_ID:
        await call.answer("Bu tugma faqat bot yaratuvchisiga tegishli.", show_alert=True)
        return

    chat_id = int(call.data.split("_", 1)[1])
    changed = await run("UPDATE groups SET is_approved = 1 WHERE chat_id = ?", (chat_id,))
    await call.answer()

    if not changed:
        await call.message.edit_text(f"⚠️ Guruh (ID: {chat_id}) topilmadi. Bot allaqachon chiqib ketgan bo'lishi mumkin.")
        return

    _approved_groups_cache.add(chat_id)
    await call.message.edit_text(f"✅ Guruh (ID: {chat_id}) tasdiqlandi!")
    try:
        await bot.send_message(chat_id, intro_text())
    except Exception as e:
        logging.error(f"Guruhga intro yuborishda xato: {e}")


@dp.callback_query(F.data.startswith("reject_"))
async def reject_group(call: CallbackQuery):
    if call.from_user.id != CREATOR_ID:
        await call.answer("Bu tugma faqat bot yaratuvchisiga tegishli.", show_alert=True)
        return

    chat_id = int(call.data.split("_", 1)[1])
    _approved_groups_cache.discard(chat_id)
    await run("DELETE FROM groups WHERE chat_id = ?", (chat_id,))
    await call.answer()
    await call.message.edit_text(f"❌ Guruh (ID: {chat_id}) so'rovi rad etildi.")

    try:
        await bot.send_message(
            chat_id,
            "❌ Yaratuvchi tomonidan bu guruhga ruxsat berilmadi. Bot guruhni tark etmoqda.",
        )
    except Exception:
        pass
    try:
        await bot.leave_chat(chat_id)
    except Exception as e:
        logging.warning(f"Guruhdan chiqib bo'lmadi {chat_id}: {e}")


# ============================================================================
# A'ZOLARNI KUZATISH
# ============================================================================
async def remember_member(chat_id: int, user):
    key = (chat_id, user.id)
    if key in _seen:
        return
    await run(
        "INSERT OR REPLACE INTO group_members (chat_id, user_id, full_name) VALUES (?, ?, ?)",
        (chat_id, user.id, raw_name(user)),
    )
    _seen.add(key)


async def forget_member(chat_id: int, user_id: int):
    _seen.discard((chat_id, user_id))
    await run("DELETE FROM group_members WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))


@dp.chat_member()
async def member_status_changed(event: ChatMemberUpdated):
    if event.chat.type not in GROUP_TYPES:
        return
    user = event.new_chat_member.user
    if user.is_bot:
        return
    status = event.new_chat_member.status
    chat_id = event.chat.id
    _admin_cache.pop((chat_id, user.id), None)
    try:
        is_member = status == "member" or (
            status == "restricted" and getattr(event.new_chat_member, "is_member", False)
        )
        if is_member:
            if await is_group_approved(chat_id):
                await remember_member(chat_id, user)
                await run("DELETE FROM banned_users WHERE chat_id = ? AND user_id = ?", (chat_id, user.id))
        elif status in ("left", "kicked"):
            await forget_member(chat_id, user.id)
    except Exception:
        logging.exception("chat_member yangilanishida xatolik")


class TrackMembersMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]],
        event: Message,
        data: Dict[str, Any],
    ) -> Any:
        try:
            if event.chat.type in GROUP_TYPES and await is_group_approved(event.chat.id):
                if event.from_user and not event.from_user.is_bot:
                    await remember_member(event.chat.id, event.from_user)
                for u in event.new_chat_members or []:
                    if not u.is_bot:
                        await remember_member(event.chat.id, u)
                if event.left_chat_member:
                    await forget_member(event.chat.id, event.left_chat_member.id)
        except Exception:
            logging.exception("A'zolarni kuzatishda xatolik")
        return await handler(event, data)


dp.message.outer_middleware(TrackMembersMiddleware())


# ============================================================================
# UMUMIY BUYRUQLAR
# ============================================================================
@dp.message(Command("start"))
async def cmd_start(message: Message):
    if message.chat.type == "private":
        await message.answer(
            "🛡 <b>ExamGuard AI</b> tizimiga xush kelibsiz.\n\n"
            "Meni o'quv guruhiga qo'shib administrator qilsangiz, vazifalar va intizomni nazorat qilishni boshlayman. "
            "Muddatlar bilan hazillashmayman, lekin muloyimligimni saqlayman 🙂"
        )
        return

    if not await is_group_approved(message.chat.id):
        await message.reply("⚠️ Bu guruh hali tasdiqlanmagan.")
        return

    if await is_user_admin(message):
        await message.reply("🛡 <b>ExamGuard AI</b> guruhda to'liq nazorat rejimida ishlamoqda.")
    else:
        await message.reply(
            f"Hurmatli <b>{label(message.from_user)}</b>, /start bosish vazifani bajarmaydi 😉 "
            "Yaxshisi /tasks ni oching va ishga kirishing."
        )


@dp.message(Command("help"))
async def cmd_help(message: Message):
    if not await group_guard(message, allow_private=True):
        return
    await message.answer(
        "🤖 <b>ExamGuard AI — Qo'llanma</b> 📖\n\n"
        "👨‍🏫 <b>Ustoz uchun (Adminlar):</b>\n"
        "• <code>/task &lt;soat:min&gt; &lt;matn&gt;</code> — bugungi muddatga vazifa.\n"
        "• <code>/task &lt;kun&gt; &lt;soat:min&gt; &lt;matn&gt;</code> — kelgusi kunlarga vazifa.\n"
        "• Rasmli vazifa: rasm yuborib, izohiga <code>/task 21:00 1. ...</code> yozing.\n"
        "• <code>/extend</code> — muddatga 1 soat qo'shish.\n"
        "• <code>/cancel_task</code> — vazifani bekor qilish.\n"
        "• <code>/banned_list</code> — ban qilinganlar ro'yxati.\n\n"
        "👩‍🎓 <b>O'quvchilar uchun:</b>\n"
        "• O'zingizning ovozli xabaringizga reply qilib: <code>/answer &lt;raqam&gt;</code>.\n"
        "• <code>/tasks</code> — faol vazifa. <code>/submissions</code> — topshirganlar.\n\n"
        "⚠️ <b>Anticheat qoidalari:</b>\n"
        "• Har bir savolga alohida va har xil soniyadagi (davomiylikdagi) ovoz yuboring. "
        "Bir ovozni yoki bitta soniyadagi ovozni boshqa savolga ishlatib bo'lmaydi.\n"
        "• Forward qilingan, boshqa o'quvchiga tegishli yoki vazifadan oldin yozilgan ovoz qabul qilinmaydi.\n"
        "• Shu savolga qayta ovoz yuborsangiz, eskisi yangisiga almashadi.\n"
        f"• Muddatida topshirmaganlar {BAN_DAYS} kunga chetlashtiriladi."
    )


@dp.message(Command("rules"))
async def cmd_rules(message: Message):
    if not await group_guard(message, allow_private=True):
        return
    await message.answer(
        f"📜 <b>Intizom qoidalari:</b>\n"
        f"1. Vazifa muddatida, ovozli xabar bilan topshiriladi.\n"
        f"2. Muddatida topshirmagan o'quvchi {BAN_DAYS} kunga guruhdan chetlashtiriladi.\n"
        f"3. Ovozni almashtirish, takrorlash yoki forward qilish qabul qilinmaydi.\n"
        f"Tartib hammaga bir xil. Ustoz vaqtni uzaytirsa, bu faqat Ustozning qarori."
    )


# ============================================================================
# USTOZ BUYRUQLARI
# ============================================================================
TASK_FORMAT_HELP = (
    "⚠️ Format noto'g'ri. Namuna:\n"
    "1) Bugun uchun: <code>/task 21:00 1. What is your hobby? 2. Your dream job?</code>\n"
    "2) Kun bilan: <code>/task 1 21:00 1. What is your hobby?</code>\n"
    "🖼 Rasmli vazifa: rasm yuborib, izohiga shu formatda yozing."
)


async def create_task_from_message(message: Message, args_text: str):
    m = TASK_RE.match((args_text or "").strip())
    if not m:
        await message.answer(TASK_FORMAT_HELP)
        return

    days = int(m.group(1)) if m.group(1) else 0
    hours, minutes = int(m.group(2)), int(m.group(3))
    task_text = m.group(4).strip()

    if hours > 23 or minutes > 59:
        await message.answer("⚠️ Soat 0-23, daqiqa 0-59 oralig'ida bo'lishi kerak.")
        return
    if days > 30:
        await message.answer("⚠️ Kun soni 30 dan oshmasligi kerak.")
        return

    now = now_local()
    target_date = (now + timedelta(days=days)).date()
    deadline = TZ.localize(datetime.combine(target_date, dtime(hours, minutes)))
    if deadline <= now:
        await message.answer("⚠️ O'tib ketgan vaqtni belgilab bo'lmaydi. Kelajakdagi vaqtni kiriting.")
        return

    deadline_str = fmt_local(deadline)
    warned_flag = 1 if (deadline - now) <= timedelta(minutes=WARN_MINUTES) else 0
    user = message.from_user
    chat_id = message.chat.id
    photo_id = message.photo[-1].file_id if message.photo else None

    db = await get_db()
    async with _db_lock:
        if user:
            teacher_username = f"@{user.username}" if user.username else raw_name(user)
            await db.execute(
                "UPDATE groups SET teacher_id = ?, teacher_username = ?, teacher_name = ? WHERE chat_id = ?",
                (user.id, teacher_username, raw_name(user), chat_id),
            )
        await db.execute("UPDATE tasks SET is_active = 0 WHERE chat_id = ?", (chat_id,))
        await db.execute(
            "INSERT INTO tasks (chat_id, task_text, deadline, is_active, warned_30, created_at, photo_id) "
            "VALUES (?, ?, ?, 1, ?, ?, ?)",
            (chat_id, task_text, deadline_str, warned_flag, utc_str(), photo_id),
        )
        await db.commit()

    request_backup()
    await send_task_card(
        chat_id,
        "📢 <b>YANGI CEFR VAZIFASI E'LON QILINDI!</b> 🎯",
        task_text,
        deadline_str,
        photo_id,
        "🎙 O'zingizning ovozli xabaringizga reply qilib <code>/answer &lt;raqam&gt;</code> yuboring.\n"
        f"⚠️ <i>Muddatida topshirmaganlar {BAN_DAYS} kunga chetlashtiriladi.</i>",
    )


@dp.message(Command("task"))
async def cmd_task(message: Message, command: CommandObject):
    if not await group_guard(message, admin_only=True):
        return
    await create_task_from_message(message, command.args or "")


@dp.message(F.photo, F.caption.regexp(TASK_CAPTION_RE))
async def task_with_photo(message: Message):
    if not await group_guard(message, admin_only=True):
        return
    await create_task_from_message(message, TASK_CAPTION_RE.sub("", message.caption or "", count=1))


@dp.message(Command("cancel_task"))
async def cmd_cancel_task(message: Message):
    if not await group_guard(message, admin_only=True):
        return
    await run("UPDATE tasks SET is_active = 0 WHERE chat_id = ?", (message.chat.id,))
    await message.answer("ℹ️ <b>Joriy vazifa Ustoz tomonidan bekor qilindi.</b> Yangi vazifa kutamiz.")


@dp.message(Command("extend"))
async def cmd_extend(message: Message):
    if not await group_guard(message, admin_only=True):
        return

    task = await get_active_task(message.chat.id)
    if not task:
        await message.answer("⚠️ Hozirda faol vazifa mavjud emas.")
        return

    task_id, _, old_deadline_str, _, _ = task
    new_deadline_str = fmt_local(parse_deadline(old_deadline_str) + timedelta(hours=1))
    await run("UPDATE tasks SET deadline = ?, warned_30 = 0 WHERE id = ?", (new_deadline_str, task_id))

    await message.answer(
        "⏳ <b>Vazifa muddati uzaytirildi.</b>\n"
        "Ustoz qo'shimcha <b>1 soat</b> berdi. Vaqtdan oqilona foydalaning.\n"
        f"⏰ <b>Yangi oxirgi muddat:</b> <code>{new_deadline_str}</code>"
    )


@dp.message(Command("banned_list"))
async def cmd_banned_list(message: Message):
    if not await group_guard(message, admin_only=True):
        return

    rows = await q_all(
        "SELECT COALESCE(full_name, CAST(user_id AS TEXT)), COALESCE(reason, '-'), banned_at, unban_at "
        "FROM banned_users WHERE chat_id = ? AND unban_at > ? ORDER BY banned_at DESC",
        (message.chat.id, fmt_local(now_local())),
    )
    if not rows:
        await message.answer("🛡 Hozirda ban qilingan o'quvchilar yo'q. Intizom a'lo darajada.")
        return

    lines = [
        f"• <b>{esc(str(full_name))}</b>\n"
        f"  └ 📌 <i>Sabab:</i> {esc(str(reason))}\n"
        f"  └ ⏰ <i>Vaqti:</i> {banned_at}\n"
        f"  └ 🔓 <i>Qaytish muddati:</i> {unban_at}"
        for full_name, reason, banned_at, unban_at in rows
    ]
    await send_chunks(message, "📋 <b>Chetlashtirilgan o'quvchilar ro'yxati:</b>\n", lines)


# ============================================================================
# O'QUVCHI BUYRUQLARI
# ============================================================================
@dp.message(Command("tasks"))
async def cmd_tasks(message: Message):
    if not await group_guard(message):
        return

    task = await get_active_task(message.chat.id)
    if not task:
        await message.answer("📂 Hozircha faol vazifa yo'q. Ustozdan yangisini kutamiz.")
        return

    _, task_text, deadline_str, _, photo_id = task
    await send_task_card(
        message.chat.id,
        "📌 <b>Hozirgi faol vazifa:</b>",
        task_text,
        deadline_str,
        photo_id,
        "🎙 Javob: o'zingizning ovozli xabaringizga reply qilib <code>/answer &lt;raqam&gt;</code> yuboring.",
    )


@dp.message(Command("answer"))
async def cmd_answer(message: Message, command: CommandObject):
    if message.chat.type == "private":
        await message.answer("⚠️ Javobingizni guruhga yuboring.")
        return
    if not await group_guard(message):
        return
    await process_answer(message, command.args or "")


@dp.message(F.voice, F.caption.regexp(ANSWER_RE))
async def voice_with_caption(message: Message):
    if message.chat.type == "private" or not await group_guard(message):
        return
    m = ANSWER_RE.match(message.caption or "")
    await process_answer(message, (m.group(1) if m else "") or "")


@dp.message(Command("submissions"))
async def cmd_submissions(message: Message):
    if not await group_guard(message):
        return

    chat_id = message.chat.id
    task_row = await q_one(
        "SELECT id FROM tasks WHERE chat_id = ? ORDER BY is_active DESC, id DESC LIMIT 1", (chat_id,)
    )
    if not task_row:
        await message.answer("📂 Guruhda hozircha vazifa berilmagan.")
        return

    rows = await q_all(
        "SELECT user_id, COALESCE(NULLIF(full_name, ''), CAST(user_id AS TEXT)), question_num, duration "
        "FROM answers WHERE chat_id = ? AND task_id = ? ORDER BY submitted_at ASC, id ASC",
        (chat_id, task_row[0]),
    )
    if not rows:
        await message.answer("📂 Hozircha hech kim vazifa topshirmagan.")
        return

    users: Dict[int, Tuple[str, List[Tuple[int, str]]]] = {}
    for user_id, full_name, q_num, dur in rows:
        entry = users.setdefault(user_id, (str(full_name), []))
        try:
            order = int(q_num)
        except (TypeError, ValueError):
            order = 0
        entry[1].append((order, f"{q_num}-savol ({dur or 0}s)"))

    lines = [
        f"• <b>{esc(name)}</b> — {', '.join(text for _, text in sorted(items))}"
        for name, items in users.values()
    ]
    await send_chunks(message, f"📋 <b>Topshirganlar ro'yxati — {len(users)} ta:</b>\n", lines)


# ============================================================================
# SKEYULER: 30 daqiqalik ogohlantirish va muddat tugaganda ban
# ============================================================================
async def finish_task(task_id: int, chat_id: int):
    db = await get_db()
    async with _db_lock:
        cur = await db.execute("UPDATE tasks SET is_active = 0 WHERE id = ? AND is_active = 1", (task_id,))
        await db.commit()
        if cur.rowcount == 0:
            return  # allaqachon yopilgan
    request_backup()

    submitted = {r[0] for r in await q_all("SELECT DISTINCT user_id FROM answers WHERE task_id = ?", (task_id,))}
    members = await q_all(
        "SELECT user_id, COALESCE(NULLIF(full_name, ''), CAST(user_id AS TEXT)) FROM group_members WHERE chat_id = ?",
        (chat_id,),
    )
    candidates = [(uid, str(name)) for uid, name in members if uid not in submitted]

    sem = asyncio.Semaphore(10)

    async def check(uid: int, name: str):
        async with sem:
            try:
                info = await bot.get_chat_member(chat_id, uid)
            except Exception as e:
                logging.warning(f"[{chat_id}] a'zo {uid} tekshirilmadi: {e}")
                return ("error", uid, name)
        if info.status in ("administrator", "creator", "left", "kicked"):
            return ("skip", uid, name)
        return ("ban", uid, name)

    results = await asyncio.gather(*(check(uid, name) for uid, name in candidates))
    to_ban = [(uid, name) for status, uid, name in results if status == "ban"]
    errors = sum(1 for status, _, _ in results if status == "error")

    if candidates and errors == len(candidates):
        logging.warning(f"[{chat_id}] a'zolarni tekshirib bo'lmadi (bot guruhda yo'q yoki admin emas)")
        return

    if not to_ban:
        if errors == 0:
            try:
                await bot.send_message(
                    chat_id,
                    "✅ Vazifa topshirish vaqti tugadi. Hamma muddatida topshirgan, intizom a'lo. Rahmat!",
                )
            except Exception as e:
                logging.warning(f"[{chat_id}] xabar yuborilmadi: {e}")
        return

    now_dt = now_local()
    ban_until = now_dt + timedelta(days=BAN_DAYS)
    banned: List[Tuple[int, str]] = []
    failed = 0
    for uid, name in to_ban:
        try:
            await bot.ban_chat_member(chat_id=chat_id, user_id=uid, until_date=ban_until)
            banned.append((uid, name))
        except Exception as e:
            failed += 1
            logging.warning(f"[{chat_id}] {uid} ban qilinmadi: {e}")
        await asyncio.sleep(0.05)

    if banned:
        async with _db_lock:
            await db.executemany(
                "INSERT OR REPLACE INTO banned_users (chat_id, user_id, full_name, reason, banned_at, unban_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [
                    (chat_id, uid, name, "Belgilangan muddatda vazifa topshirmagani uchun",
                     fmt_local(now_dt), fmt_local(ban_until))
                    for uid, name in banned
                ],
            )
            await db.executemany(
                "DELETE FROM group_members WHERE chat_id = ? AND user_id = ?",
                [(chat_id, uid) for uid, _ in banned],
            )
            await db.commit()
        for uid, _ in banned:
            _seen.discard((chat_id, uid))
        request_backup()

        try:
            await bot.send_message(
                chat_id,
                "⛔️ <b>Vazifa topshirish vaqti tugadi.</b>\n"
                f"Muddatida topshirmagan o'quvchilar qoidaga muvofiq {BAN_DAYS} kunga chetlashtirildi. "
                f"Intizom — natija kaliti. {BAN_DAYS} kundan so'ng ko'rishguncha!",
            )
        except Exception as e:
            logging.warning(f"[{chat_id}] xabar yuborilmadi: {e}")

    if failed:
        try:
            await bot.send_message(
                chat_id,
                f"⚠️ {failed} ta o'quvchini chetlashtira olmadim. Menga adminlikda "
                f"<b>a'zolarni cheklash (ban)</b> huquqini bering.",
            )
        except Exception:
            pass


async def warn_task(task_id: int, chat_id: int):
    changed = await run("UPDATE tasks SET warned_30 = 1 WHERE id = ? AND warned_30 = 0", (task_id,))
    if not changed:
        return
    try:
        await bot.send_message(
            chat_id,
            "⏰ <b>OGOHLANTIRISH!</b>\n"
            f"Topshirish muddatiga <b>{WARN_MINUTES} daqiqa</b> qoldi. Hali topshirmaganlar — hozir yozing, "
            f"keyin \"internet o'chib qoldi\" ketmaydi 😉\n"
            f"Muddatida topshirmaganlar {BAN_DAYS} kunga chetlashtiriladi.",
        )
    except Exception as e:
        logging.warning(f"[{chat_id}] ogohlantirish yuborilmadi: {e}")


async def check_deadlines():
    tick = 0
    await asyncio.sleep(STARTUP_GRACE)
    while True:
        try:
            now = now_local()
            now_str = fmt_local(now)
            warn_str = fmt_local(now + timedelta(minutes=WARN_MINUTES))

            for task_id, chat_id in await q_all(
                "SELECT id, chat_id FROM tasks WHERE is_active = 1 AND deadline <= ?", (now_str,)
            ):
                try:
                    await finish_task(task_id, chat_id)
                except Exception:
                    logging.exception(f"Vazifa {task_id} (guruh {chat_id}) ni yopishda xatolik")

            for task_id, chat_id in await q_all(
                "SELECT id, chat_id FROM tasks WHERE is_active = 1 AND warned_30 = 0 "
                "AND deadline > ? AND deadline <= ?",
                (now_str, warn_str),
            ):
                try:
                    await warn_task(task_id, chat_id)
                except Exception:
                    logging.exception(f"Vazifa {task_id} (guruh {chat_id}) ogohlantirishida xatolik")

            tick += 1
            if tick % 30 == 0:
                await run("DELETE FROM banned_users WHERE unban_at <= ?", (now_str,))
                t = time.monotonic()
                for key in [k for k, v in _admin_cache.items() if v[1] <= t]:
                    _admin_cache.pop(key, None)
        except Exception:
            logging.exception("Scheduler xatoligi")

        await asyncio.sleep(CHECK_INTERVAL)


# ============================================================================
# ZAXIRA NUSXA (Telegram orqali): deploy / restart bo'lsa ham ma'lumot yo'qolmaydi
# ============================================================================
_backup_event: Optional[asyncio.Event] = None
_backup_msg_id: Optional[int] = None
_last_backup_hash: Optional[str] = None


def request_backup():
    if _backup_event is not None:
        _backup_event.set()


def _snapshot_db(src_path: str, dst_path: str):
    if os.path.exists(dst_path):
        os.remove(dst_path)
    src = sqlite3.connect(src_path, timeout=30)
    try:
        dst = sqlite3.connect(dst_path)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()


def _file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _validate_db(path: str) -> bool:
    try:
        conn = sqlite3.connect(path)
        try:
            ok = conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        finally:
            conn.close()
        return ok and "groups" in tables
    except Exception:
        return False


async def restore_db_if_missing():
    global _backup_msg_id
    if not BACKUP_ENABLED:
        return
    if os.path.exists(DB_NAME) and os.path.getsize(DB_NAME) > 0:
        return

    try:
        me = await bot.me()
        chat = await bot.get_chat(BACKUP_CHAT_ID)
        pinned = getattr(chat, "pinned_message", None)
        if not (pinned and pinned.document and pinned.from_user and pinned.from_user.id == me.id
                and pinned.document.file_name == BACKUP_FILENAME):
            logging.info("Tiklash uchun zaxira nusxa topilmadi (birinchi ishga tushirish bo'lishi mumkin).")
            return

        os.makedirs(os.path.dirname(os.path.abspath(DB_NAME)), exist_ok=True)
        tmp_path = DB_NAME + ".restore"
        await bot.download(pinned.document.file_id, destination=tmp_path)
        if not await asyncio.to_thread(_validate_db, tmp_path):
            logging.error("Yuklab olingan zaxira nusxa buzilgan, tiklanmadi!")
            os.remove(tmp_path)
            return

        for suffix in ("-wal", "-shm"):
            if os.path.exists(DB_NAME + suffix):
                os.remove(DB_NAME + suffix)
        os.replace(tmp_path, DB_NAME)
        _backup_msg_id = pinned.message_id
        print("♻️ Baza Telegram'dagi zaxira nusxadan tiklandi!")
    except Exception as e:
        logging.error(f"Zaxira nusxadan tiklab bo'lmadi: {e}")


async def backup_db(force: bool = False):
    global _backup_msg_id, _last_backup_hash
    if not BACKUP_ENABLED or not os.path.exists(DB_NAME):
        return

    snap = DB_NAME + ".snapshot"
    await asyncio.to_thread(_snapshot_db, DB_NAME, snap)
    digest = await asyncio.to_thread(_file_sha256, snap)
    if digest == _last_backup_hash and not force:
        return

    caption = f"💾 ExamGuard zaxira nusxa\n🕒 {fmt_local(now_local())}\n#{digest[:8]}"

    if _backup_msg_id:
        try:
            await bot.edit_message_media(
                chat_id=BACKUP_CHAT_ID,
                message_id=_backup_msg_id,
                media=InputMediaDocument(media=FSInputFile(snap, filename=BACKUP_FILENAME), caption=caption),
            )
            _last_backup_hash = digest
            return
        except Exception as e:
            logging.warning(f"Zaxira xabarini yangilab bo'lmadi, yangisi yuboriladi: {e}")
            _backup_msg_id = None

    msg = await bot.send_document(
        BACKUP_CHAT_ID,
        FSInputFile(snap, filename=BACKUP_FILENAME),
        caption=caption,
        disable_notification=True,
    )
    _backup_msg_id = msg.message_id
    try:
        await bot.pin_chat_message(BACKUP_CHAT_ID, msg.message_id, disable_notification=True)
    except Exception as e:
        logging.warning(f"Zaxira xabarini qadab bo'lmadi (tiklash ishlamasligi mumkin): {e}")
    _last_backup_hash = digest


async def backup_loop():
    while True:
        try:
            await asyncio.wait_for(_backup_event.wait(), timeout=BACKUP_INTERVAL)
        except asyncio.TimeoutError:
            pass
        _backup_event.clear()
        try:
            await backup_db()
        except Exception as e:
            logging.error(
                f"Zaxira nusxa yuborilmadi: {e}. Yaratuvchi botga shaxsiy chatda /start yuborganmi?"
            )
        await asyncio.sleep(BACKUP_MIN_GAP)


@dp.message(Command("backup"))
async def cmd_backup(message: Message):
    if message.chat.type != "private" or not message.from_user or message.from_user.id != CREATOR_ID:
        return
    try:
        await backup_db(force=True)
        await message.answer("💾 Zaxira nusxa yangilandi va qadab (pin) qo'yildi.")
    except Exception as e:
        await message.answer(f"⚠️ Zaxira nusxa yuborilmadi: {esc(str(e))}")


# ============================================================================
# SOG'LIQNI TEKSHIRISH SERVERI (Render web service + UptimeRobot uchun)
# ============================================================================
async def start_health_server() -> Optional[web.AppRunner]:
    port = os.getenv("PORT")
    if not port:
        return None

    async def ok(_request):
        return web.Response(text="ExamGuard AI is running")

    app = web.Application()
    app.router.add_get("/", ok)
    app.router.add_get("/health", ok)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", int(port)).start()
    print(f"🌐 Health-server {port}-portda ishlayapti")
    return runner


# ============================================================================
# ISHGA TUSHIRISH
# ============================================================================
async def main():
    global _backup_event
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    _backup_event = asyncio.Event()

    health_runner = await start_health_server()
    await restore_db_if_missing()
    await init_db()
    await set_bot_commands()

    scheduler = asyncio.create_task(check_deadlines())
    backup_task = asyncio.create_task(backup_loop())
    request_backup()
    print("🛡 ExamGuard AI qat'iy intizom va anticheat rejimida ishga tushdi!")
    try:
        await dp.start_polling(
            bot,
            allowed_updates=["message", "callback_query", "my_chat_member", "chat_member"],
        )
    finally:
        scheduler.cancel()
        backup_task.cancel()
        try:
            await asyncio.wait_for(backup_db(), timeout=20)
        except Exception as e:
            logging.error(f"Oxirgi zaxira nusxa yuborilmadi: {e}")
        if _db_conn:
            await _db_conn.close()
        if health_runner:
            await health_runner.cleanup()
        await bot.session.close()


def run_app():
    try:
        import uvloop  # type: ignore

        if sys.version_info >= (3, 11):
            with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
                runner.run(main())
            return
    except ImportError:
        pass
    asyncio.run(main())


if __name__ == "__main__":
    try:
        run_app()
    except KeyboardInterrupt:
        print("Bot to'xtatildi!")