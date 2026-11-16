import asyncio
import hashlib
import html
import logging
import os
import re
import shutil
import sqlite3
import sys
import time
import uuid
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
# Token kodda emas: .env fayliga  BOT_TOKEN=...  deb yozing
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    sys.exit("❌ BOT_TOKEN topilmadi. .env faylga BOT_TOKEN=... deb yozing.")

CREATOR_ID = int(os.getenv("CREATOR_ID", "7439126820"))

DB_NAME = os.getenv("DB_PATH", "examguard.db")   # VPS da: /var/lib/examguard/examguard.db
TZ = pytz.timezone("Asia/Tashkent")
UTC = pytz.utc
CHECK_INTERVAL = 2            # skeyuler tekshiruv oralig'i (soniya)
BAN_DAYS = 3
WARN_MINUTES = int(os.getenv("WARN_MINUTES", "30"))   # tugashiga necha daqiqa qolganda ogohlantirish
DURATION_TOLERANCE = 0        # 0 = aynan bir xil soniya; 1 = ±1 soniya ham bir xil hisoblanadi
ALLOW_FORWARDED_VOICE = False
ADMIN_CACHE_TTL = 60          # adminlik tekshiruvi keshi (soniya)
GROUP_TYPES = ("group", "supergroup")

# --- Zaxira nusxa (Telegram orqali) va ishga tushish sozlamalari ---
IS_RENDER = bool(os.getenv("RENDER"))                      # Render buni o'zi o'rnatadi
EPHEMERAL_DISK = IS_RENDER or os.getenv("EPHEMERAL_DISK") == "1"   # disk deploy'da tozalanadimi?
# Render'da zaxira avtomatik yoqiladi. VPS da BACKUP_ENABLED=1 deb yozing (tavsiya etiladi).
# Kompyuteringizda (VS Code) sinab ko'rganda zaxira O'CHIQ turadi: ishlab turgan botning zaxirasini buzmaydi.
BACKUP_ENABLED = os.getenv("BACKUP_ENABLED", "1" if IS_RENDER else "0") == "1"
BACKUP_CHAT_ID = int(os.getenv("BACKUP_CHAT_ID", str(CREATOR_ID)))  # tavsiya: shaxsiy KANAL (DEPLOY.md ga qarang)
BACKUP_INTERVAL = 120        # yuklash muvaffaqiyatsiz bo'lsa, shuncha soniyadan keyin qayta uriniladi
BACKUP_MIN_GAP = 20          # ketma-ket yuklashlar orasidagi minimal pauza (soniya)
ARCHIVE_EVERY = 24 * 3600    # kuniga bitta "arxiv" nusxa o'chirilmaydi (qo'lda tiklash uchun)
RESTORE_RETRIES = 6          # tiklashda Telegram xatolariga necha marta qayta uriniladi
RETRY_BASE_DELAY = 2.0
APPROVAL_COOLDOWN = 600      # bir guruh uchun ruxsat so'rovi orasidagi minimal vaqt (soniya)
STARTUP_GRACE = 60           # qayta yoqilgandan keyin skeyuler kutish vaqti: navbatdagi javoblar ulgurib qolsin
BACKUP_FILENAME = "examguard_backup.db"
BACKUP_TABLES = ("groups", "tasks", "answers", "group_members", "banned_users", "payments")

# --- Obuna (to'lov) tizimi ---
# --- Obuna (to'lov) tizimi ---
SUBSCRIPTION_REQUIRED = os.getenv("SUBSCRIPTION_REQUIRED", "1") == "1"   # 0 = obuna talab qilinmaydi
ADMIN_USERNAME = "sh12shoh12"
PAYMENT_CARD = "9860 1701 0938 4678"
PAYMENT_CARD_OWNER = "Moxena Ochilova"
TRIAL_DAYS = int(os.getenv("TRIAL_DAYS", "3"))                            # yangi tasdiqlangan guruhga sinov muddati
MIGRATION_GRACE_DAYS = int(os.getenv("MIGRATION_GRACE_DAYS", "7"))        # mavjud guruhlarga bir martalik imtiyoz
PLANS: Dict[str, Dict[str, Any]] = {
    "1m": {"title": "1 oy", "days": 30, "price": 15000, "icon": "📅"},
    "1y": {"title": "1 yil", "days": 365, "price": 150000, "icon": "📆"},
}

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
_conn_lock = asyncio.Lock()
_db_gate = asyncio.Event()   # /restore paytida bazaga murojaatlar vaqtincha kutadi
_db_gate.set()
_approved_groups_cache: set = set()
_seen: set = set()
_admin_cache: Dict[Tuple[int, int], Tuple[bool, float]] = {}

# Yangi ustun kerak bo'lsa, shu yerga yozing: eski baza o'zi yangilanadi
SCHEMA_COLUMNS: Dict[str, List[Tuple[str, str]]] = {
    "groups": [
        ("teacher_id", "INTEGER"),
        ("teacher_username", "TEXT"),
        ("teacher_name", "TEXT"),
        ("is_approved", "INTEGER DEFAULT 0"),
        ("sub_until", "TEXT"),
        ("sub_plan", "TEXT"),
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
    "payments": [
        ("chat_id", "INTEGER"),
        ("user_id", "INTEGER"),
        ("user_name", "TEXT"),
        ("plan", "TEXT"),
        ("amount", "INTEGER"),
        ("status", "TEXT DEFAULT 'awaiting'"),
        ("proof_file_id", "TEXT"),
        ("proof_type", "TEXT"),
        ("created_at", "TEXT"),
        ("decided_at", "TEXT"),
        ("approved_plan", "TEXT"),
    ],
}


async def get_db() -> aiosqlite.Connection:
    global _db_conn
    await _db_gate.wait()
    if _db_conn is None:
        async with _conn_lock:
            if _db_conn is None:
                os.makedirs(os.path.dirname(os.path.abspath(DB_NAME)), exist_ok=True)
                conn = await aiosqlite.connect(DB_NAME)
                await conn.execute("PRAGMA journal_mode=WAL;")
                await conn.execute("PRAGMA synchronous=NORMAL;")
                await conn.execute("PRAGMA temp_store=MEMORY;")
                _db_conn = conn
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
    if cur.rowcount:
        request_backup()
    return cur.rowcount


async def table_columns(table: str) -> List[str]:
    return [row[1] for row in await q_all(f"PRAGMA table_info({table})")]


async def ensure_column(table: str, column: str, ddl: str) -> bool:
    existing = await table_columns(table)
    if not existing:
        return False  # jadval hali yaratilmagan (u CREATE TABLE bilan to'liq yaratiladi)
    if column in existing:
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
            is_approved INTEGER DEFAULT 0,
            sub_until TEXT,
            sub_plan TEXT
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

    # --- Xavfsiz migratsiya: yetishmayotgan ustunlarni qo'shadi, ma'lumotni o'chirmaydi ---
    for table, columns in SCHEMA_COLUMNS.items():
        for column, ddl in columns:
            added = await ensure_column(table, column, ddl)
            if added and table == "groups" and column == "is_approved":
                # Eski guruhlar ruxsatsiz qolib ketmasligi uchun bir marta tasdiqlanadi
                await db.execute("UPDATE groups SET is_approved = 1")
            if added and table == "groups" and column == "sub_until" and MIGRATION_GRACE_DAYS > 0:
                # Obuna tizimi birinchi marta yoqilganda mavjud guruhlar birdan qulflanib qolmasligi uchun
                grace_until = (datetime.now(UTC) + timedelta(days=MIGRATION_GRACE_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
                await db.execute(
                    "UPDATE groups SET sub_until = ?, sub_plan = 'grace' WHERE is_approved = 1", (grace_until,)
                )

    await db.execute("UPDATE tasks SET created_at = '2000-01-01 00:00:00' WHERE created_at IS NULL")
    await db.execute("UPDATE answers SET submitted_at = datetime('now') WHERE submitted_at IS NULL")

    # NULL ismlarni to'ldirish: avval eski username ustunidan, qolganini "O'quvchi" bilan
    for table in ("group_members", "answers"):
        if "username" in await table_columns(table):
            await db.execute(
                f"UPDATE {table} SET full_name = username "
                f"WHERE (full_name IS NULL OR full_name = '') AND username IS NOT NULL AND username != ''"
            )
        await db.execute(f"UPDATE {table} SET full_name = 'O''quvchi' WHERE full_name IS NULL OR full_name = ''")

    # Tezlik uchun indekslar
    await db.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            user_id INTEGER,
            user_name TEXT,
            plan TEXT,
            amount INTEGER,
            status TEXT DEFAULT 'awaiting',
            proof_file_id TEXT,
            proof_type TEXT,
            created_at TEXT,
            decided_at TEXT,
            approved_plan TEXT
        )
    """)

    # Zaxira tizimi uchun: baza "nasl-nasabi" (lineage) va zaxira raqami (seq)
    await db.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
    await db.execute("INSERT OR IGNORE INTO meta (key, value) VALUES ('lineage', ?)", (uuid.uuid4().hex,))
    await db.execute("INSERT OR IGNORE INTO meta (key, value) VALUES ('backup_seq', '0')")

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
        BotCommand(command="members", description="[Ustoz] Bot taniydigan a'zolar"),
        BotCommand(command="obuna", description="Guruh obunasi (to'lov)"),
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
    # pytz bilan replace(tzinfo=...) ISHLATILMAYDI (noto'g'ri offset beradi), localize ishlatiladi
    return TZ.localize(datetime.strptime(deadline_str, "%Y-%m-%d %H:%M"))


def parse_utc(value: str) -> Optional[datetime]:
    try:
        return UTC.localize(datetime.strptime(value, "%Y-%m-%d %H:%M:%S"))
    except Exception:
        return None


def raw_name(user) -> str:
    """Bazaga yoziladigan toza ism-familiya (HTML escape QILINMAGAN)."""
    if not user:
        return "O'quvchi"
    name = (user.first_name or "").strip()
    if user.last_name:
        name = f"{name} {user.last_name}".strip()
    if name:
        return name
    return f"@{user.username}" if user.username else "O'quvchi"


def label(user) -> str:
    """Xabarda ko'rsatish uchun (html.escape qilingan)."""
    return esc(raw_name(user))


def is_forwarded(msg: Message) -> bool:
    return bool(getattr(msg, "forward_origin", None) or getattr(msg, "forward_date", None))


async def is_user_admin(message: Message) -> bool:
    if message.chat.type == "private":
        return False
    if message.sender_chat and message.sender_chat.id == message.chat.id:
        return True  # anonim admin
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
    """(id, task_text, deadline, created_at, photo_id)"""
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
        user = message.from_user
        if user and user.id == CREATOR_ID:
            # Yaratuvchining o'zi yozdi: hech narsa so'ralmaydi, guruh darhol tasdiqlanadi
            await approve_chat(message.chat.id, user)
        else:
            # Bot bu guruhni tanimasa (masalan ma'lumot yo'qolgan bo'lsa), yaratuvchiga tasdiq so'rovi ketadi
            await ensure_approval_request(message)
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
    async with _db_lock:  # bir vaqtda kelgan ikki ovoz "tekshir-keyin-yoz" poygasiga tushmasin
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
            # BOSHQA savollarga berilgan javoblar bilan solishtiriladi (almashtirishda ham!)
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
    task_id, task_text, deadline_str, _, _ = task

    numbers = task_question_numbers(task_text)
    if numbers and question_num not in numbers:
        await message.reply(
            f"⚠️ <b>{name}</b>, vazifada <b>{question_num}-savol</b> yo'q. "
            f"Mavjud savollar: <b>{', '.join(sorted(numbers, key=int))}</b>."
        )
        return

    # Bot o'chiq turganda yig'ilib qolgan xabarlar: muddatdan KEYIN yuborilgani qabul qilinmaydi
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
# BOT GURUHGA QO'SHILISHI / CHIQISHI (yaratuvchi ruxsati)
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
        f"⚠️ <i>Qoida qat'iy: muddatida topshirmaganlar {BAN_DAYS} kunga chetlashtiriladi.</i>\n"
        "💎 Obuna va to'lov: /obuna\n\n"
        "Batafsil: /help"
    )


_approval_asked: Dict[int, float] = {}


async def notify_creator(text: str, **kwargs) -> bool:
    try:
        await bot.send_message(CREATOR_ID, text, **kwargs)
        return True
    except Exception as e:
        logging.warning(f"Yaratuvchiga xabar yuborib bo'lmadi: {e}")
        return False


async def approve_chat(chat_id: int, by_user=None, send_intro: bool = True):
    """Guruhni tasdiqlaydi (is_approved = 1), keshga qo'shadi va guruhga intro yuboradi."""
    teacher_id = by_user.id if by_user else 0
    teacher_username = f"@{by_user.username}" if by_user and by_user.username else "Ustoz"
    teacher_name = raw_name(by_user) if by_user else "Ustoz"
    await run(
        "INSERT INTO groups (chat_id, teacher_id, teacher_username, teacher_name, is_approved) "
        "VALUES (?, ?, ?, ?, 1) "
        "ON CONFLICT(chat_id) DO UPDATE SET is_approved = 1",
        (chat_id, teacher_id, teacher_username, teacher_name),
    )
    _approved_groups_cache.add(chat_id)
    await grant_trial_if_new(chat_id)
    if send_intro:
        try:
            await bot.send_message(chat_id, intro_text())
        except Exception as e:
            logging.error(f"Guruhga intro yuborishda xato: {e}")


async def send_approval_request(chat_id: int, chat_title: str, who, who_label: str = "Qo'shgan shaxs",
                                note: str = "") -> bool:
    who_id = who.id if who else 0
    who_name = raw_name(who) if who else "Noma'lum"
    who_username = f"@{who.username}" if who and who.username else "yo'q"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Ruxsat berish", callback_data=f"approve_{chat_id}"),
        InlineKeyboardButton(text="❌ Rad etish", callback_data=f"reject_{chat_id}"),
    ]])
    text = (
        f"🔔 <b>Yangi guruhdan botni qo'shish so'rovi!</b>\n\n"
        f"👤 <b>{who_label}:</b> {esc(who_name)} (ID: <code>{who_id}</code>, {esc(who_username)})\n"
        f"📌 <b>Guruh nomi:</b> {esc(chat_title)}\n"
        f"🆔 <b>Chat ID:</b> <code>{chat_id}</code>\n"
    )
    if note:
        text += f"\nℹ️ <i>{esc(note)}</i>\n"
    text += "\nTasdiqlaysizmi?"
    return await notify_creator(text, reply_markup=keyboard)


async def ensure_approval_request(message: Message):
    """Bot guruhni tasdiqlangan holda topa olmasa (masalan ma'lumot yo'qolgan bo'lsa), yaratuvchiga so'rov yuboradi."""
    chat_id = message.chat.id
    now = time.monotonic()
    if now - _approval_asked.get(chat_id, -APPROVAL_COOLDOWN) < APPROVAL_COOLDOWN:
        return
    _approval_asked[chat_id] = now
    await run(
        "INSERT OR IGNORE INTO groups (chat_id, teacher_id, teacher_username, teacher_name, is_approved) "
        "VALUES (?, 0, 'Ustoz', 'Ustoz', 0)",
        (chat_id,),
    )
    await send_approval_request(
        chat_id,
        message.chat.title or "Noma'lum guruh",
        message.from_user,
        who_label="Buyruq yuborgan shaxs",
        note="Bot bu guruhni tasdiqlangan holda topa olmadi (ma'lumot yo'qolgan yoki guruh yangilangan bo'lishi mumkin).",
    )


@dp.my_chat_member()
async def bot_status_changed(event: ChatMemberUpdated):
    chat_id = event.chat.id
    old_status = event.old_chat_member.status
    new_status = event.new_chat_member.status

    # Zaxira KANALI: bot kanalga admin qilinganda yaratuvchiga kanal ID'si yuboriladi
    if event.chat.type == "channel":
        if (event.from_user and event.from_user.id == CREATOR_ID
                and new_status == "administrator" and old_status != "administrator"):
            await notify_creator(
                f"📣 Kanal: <b>{esc(event.chat.title or '')}</b>\n🆔 ID: <code>{chat_id}</code>\n\n"
                f"Zaxira nusxalar shu kanalda saqlanishi uchun Render → Environment ga "
                f"<code>BACKUP_CHAT_ID={chat_id}</code> qo'shing va botni qayta ishga tushiring."
            )
        return

    if event.chat.type not in GROUP_TYPES:
        return

    # Bot chiqarildi: kesh va ruxsat bekor qilinadi (keyingi qo'shilishda yana so'raladi)
    if new_status in ("left", "kicked"):
        _approved_groups_cache.discard(chat_id)
        await run("UPDATE groups SET is_approved = 0 WHERE chat_id = ?", (chat_id,))
        return

    # Faqat YANGI qo'shilish. Botga admin berilganda (member -> administrator) tasdiq o'chmaydi
    if not (old_status in ("left", "kicked") and new_status in ("member", "administrator")):
        return

    chat_title = event.chat.title or "Noma'lum guruh"
    adder = event.from_user

    # Eski ruxsat keshda qolib ketmasin
    _approved_groups_cache.discard(chat_id)

    # 1) Yaratuvchining o'zi qo'shdi: hech narsa so'ralmaydi, darhol tasdiqlanadi
    if adder and adder.id == CREATOR_ID:
        await approve_chat(chat_id, adder)
        return

    # 2) Begona odam qo'shdi: yaratuvchiga tugmali so'rov ketadi
    await run(
        "INSERT INTO groups (chat_id, teacher_id, teacher_username, teacher_name, is_approved) "
        "VALUES (?, 0, 'Ustoz', 'Ustoz', 0) "
        "ON CONFLICT(chat_id) DO UPDATE SET is_approved = 0",
        (chat_id,),
    )
    _approval_asked[chat_id] = time.monotonic()

    try:
        await bot.send_message(
            chat_id,
            "🛡 <b>ExamGuard AI</b> guruhga qo'shildi.\n\n"
            "⚠️ <i>Xavfsizlik talabiga ko'ra, botni ishlatish uchun yaratuvchining tasdig'i kutilmoqda.</i>",
        )
    except Exception as e:
        logging.error(f"Guruhga xabar yuborishda xato: {e}")

    if not await send_approval_request(chat_id, chat_title, adder):
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
    await grant_trial_if_new(chat_id)
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
    """Guruhga qo'shilgan/chiqqan a'zolarni (yozmagan bo'lsa ham) kuzatadi. Bot guruhda admin bo'lishi shart."""
    if event.chat.type not in GROUP_TYPES:
        return
    user = event.new_chat_member.user
    if user.is_bot:
        return
    status = event.new_chat_member.status
    chat_id = event.chat.id
    # Adminlik o'zgargan bo'lsa (tayinlandi/olib tashlandi), kesh darhol yangilansin
    _admin_cache.pop((chat_id, user.id), None)
    try:
        is_member = status == "member" or (
            status == "restricted" and getattr(event.new_chat_member, "is_member", False)
        )
        if is_member:
            if await is_group_approved(chat_id):
                await remember_member(chat_id, user)
                # ban muddati tugab qaytgan bo'lsa, ro'yxatdan olib tashlanadi
                await run("DELETE FROM banned_users WHERE chat_id = ? AND user_id = ?", (chat_id, user.id))
        elif status in ("left", "kicked"):
            await forget_member(chat_id, user.id)
    except Exception:
        logging.exception("chat_member yangilanishida xatolik")


async def migrate_chat(old_id: int, new_id: int):
    """Oddiy guruh supergroup'ga aylanganda chat ID o'zgaradi: barcha ma'lumot yangi ID'ga ko'chiriladi."""
    if not old_id or not new_id or old_id == new_id:
        return
    db = await get_db()
    async with _db_lock:
        row = await q_one("SELECT is_approved FROM groups WHERE chat_id = ?", (old_id,))
        if row is None:
            return  # bu guruh bazada yo'q
        approved = row[0] == 1
        await db.execute("DELETE FROM groups WHERE chat_id = ?", (new_id,))
        await db.execute("UPDATE groups SET chat_id = ? WHERE chat_id = ?", (new_id, old_id))
        await db.execute("UPDATE tasks SET chat_id = ? WHERE chat_id = ?", (new_id, old_id))
        await db.execute("UPDATE answers SET chat_id = ? WHERE chat_id = ?", (new_id, old_id))
        await db.execute("UPDATE OR REPLACE group_members SET chat_id = ? WHERE chat_id = ?", (new_id, old_id))
        await db.execute("UPDATE OR REPLACE banned_users SET chat_id = ? WHERE chat_id = ?", (new_id, old_id))
        await db.execute("UPDATE payments SET chat_id = ? WHERE chat_id = ?", (new_id, old_id))
        await db.commit()
    _approved_groups_cache.discard(old_id)
    if approved:
        _approved_groups_cache.add(new_id)
    for key in [k for k in _seen if k[0] == old_id]:
        _seen.discard(key)
    request_backup()
    logging.info(f"Guruh migratsiyasi: {old_id} -> {new_id}")


@dp.message(F.migrate_to_chat_id)
async def on_migrate_to(message: Message):
    await migrate_chat(message.chat.id, message.migrate_to_chat_id)


@dp.message(F.migrate_from_chat_id)
async def on_migrate_from(message: Message):
    await migrate_chat(message.migrate_from_chat_id, message.chat.id)


class TrackMembersMiddleware(BaseMiddleware):
    """A'zolarni kuzatadi. Handlerlarni BLOKLAMAYDI, xabar boshiga bazaga murojaat qilmaydi (keshlangan)."""

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
async def cmd_start(message: Message, command: CommandObject):
    user = message.from_user

    if message.chat.type == "private":
        arg = (command.args or "").strip()
        if arg.startswith("pay_"):
            await begin_payment(message, arg[4:])
            return
        if user and user.id == CREATOR_ID:
            await message.answer(
                "🛡 <b>ExamGuard AI</b> ishga tushdi, yaratuvchi!\n\n"
                "💾 /backup — zaxira nusxani hoziroq yuborish va tekshirish\n"
                "♻️ /restore ha — zaxira faylidan qo'lda tiklash\n"
                "💎 /subs — barcha guruhlar obunasi\n"
                "🎁 /grant 1m — guruhga obuna berish (guruhda yozing yoki: /grant 1m &lt;chat_id&gt;)"
            )
        else:
            await message.answer(
                "🛡 <b>ExamGuard AI</b> tizimiga xush kelibsiz!\n\n"
                "Meni o'quv guruhiga qo'shing va administrator qiling: vazifalar, ovozli javoblar va "
                "intizomni men nazorat qilaman. Obuna haqida: guruhda /obuna"
            )
        return

    chat_id = message.chat.id
    if not await is_group_approved(chat_id):
        if user and user.id == CREATOR_ID:
            # Faqat yaratuvchi guruhni o'zi tasdiqlay oladi (oddiy admin emas)
            await approve_chat(chat_id, user)
            await message.reply("✅ Guruh tasdiqlandi va faollashdi!")
        else:
            await ensure_approval_request(message)
            await message.reply("⚠️ Bu guruh hali yaratuvchi tomonidan tasdiqlanmagan.")
        return

    if await is_user_admin(message):
        await message.reply("🛡 <b>ExamGuard AI</b> guruhda to'liq nazorat rejimida ishlamoqda, Ustoz!")
    else:
        await message.reply(
            f"Hurmatli <b>{label(user)}</b>, /start bosish vazifani bajarmaydi 😉 "
            "Yaxshisi /tasks buyrug'ini ochib, darsga kirishing."
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
        "• <code>/banned_list</code> — ban qilinganlar ro'yxati.\n"
        "• <code>/members</code> — bot nechta a'zoni taniydi.\n"
        "• <code>/obuna</code> — guruh obunasi va to'lov.\n\n"
        "👩‍🎓 <b>O'quvchilar uchun:</b>\n"
        "• O'zingizning ovozli xabaringizga reply qilib: <code>/answer &lt;raqam&gt;</code>.\n"
        "• <code>/tasks</code> — faol vazifa. <code>/submissions</code> — topshirganlar.\n\n"
        "⚠️ <b>Anticheat qoidalari:</b>\n"
        "• Har bir savolga alohida va har xil soniyadagi (davomiylikdagi) ovoz yuboring. "
        "Bir ovozni yoki bitta soniyadagi ovozni boshqa savolga ishlatib bo'lmaydi.\n"
        "• Forward qilingan yoki boshqa o'quvchiga tegishli ovoz qabul qilinmaydi. Guruhda oldin yuborgan "
        "o'zingizning ovozingizga ham reply qilib javob berishingiz mumkin.\n"
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
        f"3. Ovozni boshqadan olish, takrorlash yoki forward qilish qabul qilinmaydi.\n"
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
    # Muddat 30 daqiqadan qisqa bo'lsa, ogohlantirish darhol yuborilib ketmasligi uchun
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
    if not await require_subscription(message):
        return
    await create_task_from_message(message, command.args or "")


# Rasmli vazifa (Command filteri caption'ni ushlamagan holat uchun zaxira)
@dp.message(F.photo, F.caption.regexp(TASK_CAPTION_RE))
async def task_with_photo(message: Message):
    if not await group_guard(message, admin_only=True):
        return
    if not await require_subscription(message):
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
    if not await require_subscription(message):
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


# Voice caption'iga /answer yozilgan bo'lsa (Command filteri ushlamagan holat uchun zaxira)
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
    try:
        await punish_non_submitters(task_id, chat_id)
    finally:
        await notify_unknown_members(chat_id)


async def member_stats(chat_id: int) -> Optional[Dict[str, int]]:
    """Bot guruhdagi a'zolarning nechtasini taniydi. Telegram a'zolar ro'yxatini bermaydi: bot faqat xabar yozgan
    yoki guruhga qo'shilganini ko'rgan a'zolarni biladi."""
    try:
        total = await bot.get_chat_member_count(chat_id)
        admin_ids = {a.user.id for a in await bot.get_chat_administrators(chat_id)}
        me = await bot.me()
        known = {r[0] for r in await q_all("SELECT user_id FROM group_members WHERE chat_id = ?", (chat_id,))}
        unknown = max(0, total - len(admin_ids | known | {me.id}))
        return {"total": total, "known": len(known - admin_ids), "unknown": unknown}
    except Exception as e:
        logging.warning(f"[{chat_id}] a'zolar statistikasi olinmadi: {e}")
        return None


async def notify_unknown_members(chat_id: int):
    stats = await member_stats(chat_id)
    if not stats or stats["unknown"] <= 0:
        return
    try:
        await bot.send_message(
            chat_id,
            f"ℹ️ Guruhda <b>{stats['unknown']}</b> nafar a'zo botga hali tanish emas (ular hali xabar yozmagan), "
            f"shuning uchun ular tekshirilmadi. Ularga ham guruhga bitta xabar yozish tavsiya qilinadi.",
        )
    except Exception as e:
        logging.warning(f"[{chat_id}] xabar yuborilmadi: {e}")


async def punish_non_submitters(task_id: int, chat_id: int):
    db = await get_db()
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
    # Bayroq xabar yuborishdan OLDIN o'rnatiladi: ogohlantirish faqat bir marta ketadi
    changed = await run("UPDATE tasks SET warned_30 = 1 WHERE id = ? AND warned_30 = 0", (task_id,))
    if not changed:
        return

    pending: List[Tuple[int, str]] = []
    try:
        submitted = {r[0] for r in await q_all("SELECT DISTINCT user_id FROM answers WHERE task_id = ?", (task_id,))}
        members = await q_all(
            "SELECT user_id, COALESCE(NULLIF(full_name, ''), CAST(user_id AS TEXT)) FROM group_members WHERE chat_id = ?",
            (chat_id,),
        )
        admin_ids: set = set()
        try:
            admin_ids = {a.user.id for a in await bot.get_chat_administrators(chat_id)}
        except Exception:
            pass
        pending = [(uid, str(name)) for uid, name in members if uid not in submitted and uid not in admin_ids]
    except Exception:
        logging.exception(f"[{chat_id}] ogohlantirish ro'yxati tuzilmadi")

    text = (
        "⏰ <b>OGOHLANTIRISH!</b>\n"
        f"Topshirish muddatiga <b>{WARN_MINUTES} daqiqa</b> qoldi. Hali javob yubormaganlar — hozir yozing, "
        f"keyin \"internet o'chib qoldi\" ketmaydi 😉\n"
        f"Muddatida topshirmaganlar {BAN_DAYS} kunga chetlashtiriladi."
    )
    if pending:
        mentions: List[str] = []
        for uid, name in pending[:40]:
            piece = f'<a href="tg://user?id={uid}">{esc(name)}</a>'
            if len(text) + sum(len(m) + 2 for m in mentions) + len(piece) > 3800:
                break
            mentions.append(piece)
        more = len(pending) - len(mentions)
        text += f"\n\n👥 <b>Hali topshirmaganlar ({len(pending)}):</b> " + ", ".join(mentions)
        if more > 0:
            text += f" va yana {more} nafar"
    try:
        await bot.send_message(chat_id, text)
    except Exception as e:
        logging.warning(f"[{chat_id}] ogohlantirish yuborilmadi: {e}")


async def check_deadlines():
    tick = 0
    # Qayta yoqilganda Telegram'da yig'ilib qolgan javoblar ulgurib qolishi uchun ozgina kutamiz
    await asyncio.sleep(STARTUP_GRACE)
    while True:
        try:
            now = now_local()
            now_str = fmt_local(now)
            warn_str = fmt_local(now + timedelta(minutes=WARN_MINUTES))

            # 1) Muddati tugaganlar (bitta SQL so'rov, indeks bilan)
            for task_id, chat_id in await q_all(
                "SELECT id, chat_id FROM tasks WHERE is_active = 1 AND deadline <= ?", (now_str,)
            ):
                # Har bir guruh alohida: bittasidagi xato siklni to'xtatmaydi
                try:
                    await finish_task(task_id, chat_id)
                except Exception:
                    logging.exception(f"Vazifa {task_id} (guruh {chat_id}) ni yopishda xatolik")

            # 2) 30 daqiqa qolganlar
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
            if tick % 30 == 0:  # taxminan har daqiqada muddati o'tgan banlarni va eski keshlarni tozalash
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
# Asosiy qoidalar:
#  1. Tiklash MUVAFFAQIYATLI hal bo'lmaguncha bot hech qachon zaxira yuklamaydi (bo'sh baza yaxshi zaxiraning
#     ustidan yozilib ketmasligi uchun). Tiklashda Telegram xatosi bo'lsa, bot bo'sh baza bilan ishga TUSHMAYDI.
#  2. Har bir zaxira YANGI xabar sifatida yuboriladi va qadab (pin) qo'yiladi; qadalgani getChat orqali tekshiriladi.
#  3. Bo'sh baza hech qachon yuklanmaydi.
#  4. Kuniga bitta "ARXIV" nusxa o'chirilmaydi. Qo'lda tiklash: faylni botga forward qilib, /restore ha deb reply qiling.
_backup_event: Optional[asyncio.Event] = None
_backup_ready = False             # tiklash hal bo'lgandan keyingina True
_dirty = False                    # yuklanmagan o'zgarish bormi
_backup_lock = asyncio.Lock()
_prev_msg_id: Optional[int] = None
_prev_is_archive = False
_upload_failures = 0
_discovery_alert_sent = False


def request_backup():
    global _dirty
    if _backup_ready:
        _dirty = True
        if _backup_event is not None:
            _backup_event.set()


def _safe_remove(path: str):
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
    except Exception as e:
        logging.warning(f"{path} o'chirilmadi: {e}")


def _snapshot_db(src_path: str, dst_path: str):
    """SQLite'ning rasmiy backup API'si: bot ishlab turganda ham izchil nusxa oladi."""
    _safe_remove(dst_path)
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


def _read_meta(path: str) -> Dict[str, str]:
    try:
        conn = sqlite3.connect(path)
        try:
            return {str(k): str(v) for k, v in conn.execute("SELECT key, value FROM meta")}
        finally:
            conn.close()
    except Exception:
        return {}


def _count_rows(path: str) -> int:
    total = 0
    try:
        conn = sqlite3.connect(path)
        try:
            for table in BACKUP_TABLES:
                try:
                    total += conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                except sqlite3.Error:
                    pass
        finally:
            conn.close()
    except Exception:
        return 0
    return total


def _install_db_file(src: str, dst: str, keep_old: bool = True):
    """Yangi baza faylini joyiga qo'yadi. Eski baza .before_restore nomi bilan saqlab qo'yiladi."""
    os.makedirs(os.path.dirname(os.path.abspath(dst)), exist_ok=True)
    if keep_old and os.path.exists(dst) and os.path.getsize(dst) > 0:
        try:
            _snapshot_db(dst, dst + ".before_restore")
        except Exception as e:
            logging.warning(f"Eski bazaning nusxasi olinmadi: {e}")
    _safe_remove(dst + "-wal")
    _safe_remove(dst + "-shm")
    os.replace(src, dst)


def _to_int(value, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _is_permanent_error(e: Exception) -> bool:
    text = str(e).lower()
    name = type(e).__name__.lower()
    return ("chat not found" in text or "forbidden" in name or "bot was blocked" in text
            or "not enough rights" in text or "can't initiate" in text)


async def _retry(factory, attempts: int = RESTORE_RETRIES, what: str = "so'rov"):
    last_exc: Optional[Exception] = None
    for i in range(1, attempts + 1):
        try:
            return await factory()
        except Exception as e:
            last_exc = e
            if _is_permanent_error(e):
                raise
            delay = getattr(e, "retry_after", None) or min(RETRY_BASE_DELAY * i, 15)
            logging.warning(f"{what}: {i}/{attempts}-urinish muvaffaqiyatsiz: {e}")
            if i < attempts:
                await asyncio.sleep(delay)
    raise last_exc  # type: ignore[misc]


async def _live_row_count() -> int:
    total = 0
    for table in BACKUP_TABLES:
        row = await q_one(f"SELECT COUNT(*) FROM {table}")
        total += row[0] if row else 0
    return total


# ---------------------------------------------------------------- TIKLASH
async def find_remote_backup():
    """Qadalgan zaxira xabarini qaytaradi.
    None  -> Telegram javob berdi va zaxira YO'Q (yoki chatga kirib bo'lmaydi: birinchi ishga tushirish).
    Xato  -> Telegram bilan bog'lanib bo'lmadi: chaqiruvchi botni BO'SH bazada ishga tushirmasligi kerak."""
    me = await bot.me()
    try:
        chat = await _retry(lambda: bot.get_chat(BACKUP_CHAT_ID), what="getChat")
    except Exception as e:
        if _is_permanent_error(e):
            logging.warning(f"Zaxira chatiga kirib bo'lmadi ({e}). Zaxira hali yaratilmagan deb hisoblanadi.")
            return None
        raise

    pinned = getattr(chat, "pinned_message", None)
    if pinned is None:
        logging.info("Zaxira chatida qadalgan xabar yo'q.")
        return None
    doc = getattr(pinned, "document", None)
    if not doc or doc.file_name != BACKUP_FILENAME:
        logging.warning("Zaxira chatida qadalgan xabar bor, lekin u zaxira fayli emas. Zaxira topilmadi.")
        return None
    # Kanal xabarlarida from_user bo'lmaydi; guruh/shaxsiy chatda esa bot yuborgan bo'lishi kerak
    if pinned.from_user is not None and pinned.from_user.id != me.id:
        logging.warning("Qadalgan zaxira fayli bot tomonidan yuborilmagan. E'tiborga olinmadi.")
        return None
    return pinned


async def restore_db_if_needed() -> str:
    """Holatlar: 'disabled' | 'kept_local' | 'no_backup' | 'restored' | 'unsafe'.
    Telegram bilan bog'lanib bo'lmasa istisno ko'taradi (bot bo'sh baza bilan ishga tushmaydi)."""
    global _prev_msg_id, _prev_is_archive
    if not BACKUP_ENABLED:
        return "disabled"

    local_exists = os.path.exists(DB_NAME) and os.path.getsize(DB_NAME) > 0
    if local_exists and not EPHEMERAL_DISK:
        return "kept_local"  # doimiy disk (VPS): mahalliy baza ishonchli

    pinned = await find_remote_backup()
    if pinned is None:
        return "kept_local" if local_exists else "no_backup"

    os.makedirs(os.path.dirname(os.path.abspath(DB_NAME)), exist_ok=True)
    tmp = DB_NAME + ".restore"
    _safe_remove(tmp)
    await _retry(lambda: bot.download(pinned.document.file_id, destination=tmp), what="zaxirani yuklab olish")

    if not await asyncio.to_thread(_validate_db, tmp):
        logging.error("Telegram'dagi zaxira fayli buzilgan! Tiklab bo'lmadi.")
        _safe_remove(tmp)
        return "unsafe"

    _prev_msg_id = pinned.message_id
    _prev_is_archive = "ARXIV" in (pinned.caption or "")

    remote_meta = await asyncio.to_thread(_read_meta, tmp)
    remote_rows = await asyncio.to_thread(_count_rows, tmp)

    if local_exists:
        local_meta = await asyncio.to_thread(_read_meta, DB_NAME)
        same_lineage = bool(local_meta.get("lineage")) and local_meta.get("lineage") == remote_meta.get("lineage")
        local_seq = _to_int(local_meta.get("backup_seq"), -1)
        remote_seq = _to_int(remote_meta.get("backup_seq"), 0)
        # Mahalliy baza faqat shu zaxiraning o'zi (yoki undan yangisi) bo'lsa saqlanadi.
        # Aks holda (masalan GitHub'dan kelgan eski baza fayli) Telegram'dagi zaxira ustun turadi.
        if remote_rows == 0 or (same_lineage and local_seq >= remote_seq):
            _safe_remove(tmp)
            return "kept_local"

    await asyncio.to_thread(_install_db_file, tmp, DB_NAME, True)
    logging.info(f"Baza zaxiradan tiklandi (#{remote_meta.get('backup_seq', '?')}, {remote_rows} ta yozuv).")
    return "restored"


async def arm_backups(status: str):
    """Tiklash hal bo'lgandan KEYIN zaxira yuklashni yoqadi."""
    global _backup_ready, _dirty
    if not BACKUP_ENABLED:
        _backup_ready = False
        logging.info("Zaxira nusxa o'chirilgan (BACKUP_ENABLED=0).")
        return
    if status == "unsafe":
        _backup_ready = False
        await notify_creator(
            "⚠️ <b>Zaxira fayli buzilgan.</b> Bot BO'SH baza bilan ishga tushdi va yangi zaxira YUKLAMAYDI "
            "(eskisini buzmaslik uchun).\n\nZaxira chatidagi oldingi <code>examguard_backup.db</code> fayllaridan "
            "birini shu chatga forward qiling va unga reply qilib <code>/restore ha</code> deb yozing."
        )
        return

    _backup_ready = True
    rows = await _live_row_count()
    if status == "restored":
        _dirty = False
        await notify_creator(f"♻️ Baza Telegram zaxirasidan tiklandi: {rows} ta yozuv.")
    else:
        _dirty = rows > 0
        if _dirty and _backup_event is not None:
            _backup_event.set()
        if status == "no_backup" and rows == 0:
            await notify_creator(
                "ℹ️ Zaxira topilmadi, bot yangi (bo'sh) baza bilan boshladi. Bu birinchi ishga tushirish bo'lsa, "
                "e'tibor bermang.\n\nBo'lmasa: zaxira chatidagi oxirgi <code>examguard_backup.db</code> faylini shu "
                "chatga forward qilib, unga reply qilib <code>/restore ha</code> deb yozing."
            )


# ---------------------------------------------------------------- YUKLASH
async def _meta_get(key: str) -> Optional[str]:
    row = await q_one("SELECT value FROM meta WHERE key = ?", (key,))
    return row[0] if row else None


async def _meta_set(key: str, value: str):
    db = await get_db()
    async with _db_lock:
        await db.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        await db.commit()


async def _bump_seq() -> int:
    db = await get_db()
    async with _db_lock:
        await db.execute(
            "INSERT INTO meta (key, value) VALUES ('backup_seq', '1') "
            "ON CONFLICT(key) DO UPDATE SET value = CAST(value AS INTEGER) + 1"
        )
        await db.commit()
    return _to_int(await _meta_get("backup_seq"), 1)


async def _pin(message_id: int) -> bool:
    try:
        await bot.pin_chat_message(BACKUP_CHAT_ID, message_id, disable_notification=True)
        return True
    except Exception as e:
        logging.warning(f"Zaxira xabarini qadab bo'lmadi: {e}")
        return False


async def _verify_pinned(message_id: int) -> bool:
    """Yuklangan zaxira rostdan ham 'qadalgan xabar' sifatida topiladimi (restart'da shu topiladi)."""
    try:
        chat = await bot.get_chat(BACKUP_CHAT_ID)
        pinned = getattr(chat, "pinned_message", None)
        return pinned is not None and pinned.message_id == message_id
    except Exception as e:
        logging.warning(f"Qadalgan xabarni tekshirib bo'lmadi: {e}")
        return False


async def backup_db(force: bool = False) -> Dict[str, Any]:
    """Natija: {'status': 'ok'|'nothing'|'empty'|'disabled'|'not_ready', 'seq': int, 'verified': bool}"""
    global _dirty, _prev_msg_id, _prev_is_archive, _discovery_alert_sent
    if not BACKUP_ENABLED:
        return {"status": "disabled"}
    if not _backup_ready:
        return {"status": "not_ready"}

    async with _backup_lock:
        if not (_dirty or force):
            return {"status": "nothing"}
        _dirty = False
        try:
            if await _live_row_count() == 0:
                logging.info("Baza bo'sh: zaxira yuklanmadi (mavjud zaxirani buzmaslik uchun).")
                return {"status": "empty"}

            # Kunlik arxiv: oxirgi arxiv vaqti bazaning o'zida saqlanadi (restart/tiklashdan keyin ham yo'qolmaydi)
            now_ts = time.time()
            prev_archive_ts = await _meta_get("last_archive_ts")
            is_archive = (now_ts - float(prev_archive_ts or 0)) >= ARCHIVE_EVERY
            if is_archive:
                await _meta_set("last_archive_ts", str(now_ts))
            seq = await _bump_seq()
            snap = DB_NAME + ".snapshot"
            await asyncio.to_thread(_snapshot_db, DB_NAME, snap)
            if not await asyncio.to_thread(_validate_db, snap):
                raise RuntimeError("Zaxira nusxasi tekshiruvdan o'tmadi (integrity_check)")
            digest = await asyncio.to_thread(_file_sha256, snap)

            caption = f"💾 ExamGuard zaxira #{seq}\n🕒 {fmt_local(now_local())}\n#{digest[:8]}"
            if is_archive:
                caption += "\n📦 ARXIV"

            msg = await _retry(
                lambda: bot.send_document(
                    BACKUP_CHAT_ID,
                    FSInputFile(snap, filename=BACKUP_FILENAME),
                    caption=caption,
                    disable_notification=True,
                ),
                attempts=4,
                what="zaxirani yuklash",
            )
            pinned_ok = await _pin(msg.message_id)
            verified = pinned_ok and await _verify_pinned(msg.message_id)

            if verified:
                # Eski nusxa faqat YANGISI qadalgani tasdiqlangandan keyin tozalanadi
                if _prev_msg_id and _prev_msg_id != msg.message_id:
                    try:
                        await bot.unpin_chat_message(BACKUP_CHAT_ID, message_id=_prev_msg_id)
                    except Exception:
                        pass
                    if not _prev_is_archive:
                        try:
                            await bot.delete_message(BACKUP_CHAT_ID, _prev_msg_id)
                        except Exception:
                            pass
            elif not _discovery_alert_sent:
                _discovery_alert_sent = True
                await notify_creator(
                    "⚠️ <b>Zaxira yuborildi, lekin qadalgan xabar sifatida topilmadi.</b>\n"
                    "Bu holda bot qayta ishga tushganda zaxirani O'ZI topa olmaydi (shaxsiy chatda shunday bo'lishi mumkin).\n\n"
                    "Yechim: shaxsiy <b>kanal</b> yarating, botni shu kanalga admin qiling (xabar yozish, tahrirlash va "
                    "qadash huquqlari bilan). Bot sizga kanal ID'sini yuboradi, uni Render'da "
                    "<code>BACKUP_CHAT_ID</code> ga yozing. Batafsil: DEPLOY.md"
                )

            _prev_msg_id = msg.message_id
            _prev_is_archive = is_archive
            return {"status": "ok", "seq": seq, "verified": verified}
        except Exception:
            _dirty = True  # keyinroq qayta uriniladi
            raise


async def backup_loop():
    global _upload_failures
    while True:
        try:
            await asyncio.wait_for(_backup_event.wait(), timeout=BACKUP_INTERVAL)
        except asyncio.TimeoutError:
            pass
        _backup_event.clear()
        try:
            result = await backup_db()
            if result.get("status") == "ok":
                _upload_failures = 0
        except Exception as e:
            _upload_failures += 1
            logging.error(f"Zaxira nusxa yuklanmadi ({_upload_failures}-marta): {e}")
            if _upload_failures == 3:
                await notify_creator(
                    f"⚠️ Zaxira nusxa ketma-ket 3 marta yuklanmadi: <code>{esc(str(e))}</code>\n"
                    f"BACKUP_CHAT_ID to'g'ri va bot o'sha chatda yozish huquqiga egami?"
                )
        await asyncio.sleep(BACKUP_MIN_GAP)


# ---------------------------------------------------------------- QO'LDA BOSHQARUV (faqat yaratuvchi, shaxsiy chat)
def _is_creator_private(message: Message) -> bool:
    return bool(message.chat.type == "private" and message.from_user and message.from_user.id == CREATOR_ID)


@dp.message(Command("backup"))
async def cmd_backup(message: Message):
    """Zaxira nusxani hoziroq yuboradi va tiklash ishlashini tekshiradi."""
    if not _is_creator_private(message):
        return
    try:
        result = await backup_db(force=True)
    except Exception as e:
        await message.answer(f"⚠️ Zaxira nusxa yuborilmadi: <code>{esc(str(e))}</code>")
        return
    status = result.get("status")
    if status == "ok" and result.get("verified"):
        await message.answer(
            f"💾 Zaxira #{result['seq']} yuborildi va qadab qo'yildi. ✅ Tiklash tekshiruvi muvaffaqiyatli: "
            f"bot qayta ishga tushganda shu faylni o'zi topadi."
        )
    elif status == "ok":
        await message.answer(
            "⚠️ Zaxira yuborildi, lekin qadalgan xabar sifatida topilmadi. Bot qayta yoqilganda uni o'zi topa olmaydi. "
            "Shaxsiy kanal yarating (DEPLOY.md), BACKUP_CHAT_ID ga yozing."
        )
    elif status == "empty":
        await message.answer("ℹ️ Baza hozircha bo'sh, shuning uchun zaxira yuklanmadi.")
    elif status == "disabled":
        await message.answer("ℹ️ Zaxira o'chirilgan (BACKUP_ENABLED=0). VPS da yoqish: BACKUP_ENABLED=1")
    else:
        await message.answer("⚠️ Zaxira tizimi hozir yoqilmagan (tiklash hal bo'lmagan). /restore ha orqali tiklang.")


@dp.message(Command("restore"))
async def cmd_restore(message: Message, command: CommandObject):
    """Qo'lda tiklash: zaxira faylini botga yuboring (yoki forward qiling) va unga reply qilib /restore ha yozing."""
    global _backup_ready
    if not _is_creator_private(message):
        return
    doc = message.document or (message.reply_to_message.document if message.reply_to_message else None)
    if not doc:
        await message.answer(
            "ℹ️ Zaxira faylini (<code>examguard_backup.db</code>) shu chatga yuboring yoki forward qiling, "
            "so'ng unga reply qilib <code>/restore ha</code> deb yozing."
        )
        return
    if (command.args or "").strip().lower() != "ha":
        await message.answer(
            "⚠️ Bu amal HOZIRGI bazani tanlangan fayl bilan ALMASHTIRADI (eskisi .before_restore nomi bilan saqlanadi).\n"
            "Tasdiqlash uchun: <code>/restore ha</code>"
        )
        return

    tmp = DB_NAME + ".upload"
    try:
        os.makedirs(os.path.dirname(os.path.abspath(DB_NAME)), exist_ok=True)
        _safe_remove(tmp)
        await bot.download(doc.file_id, destination=tmp)
        if not await asyncio.to_thread(_validate_db, tmp):
            _safe_remove(tmp)
            await message.answer("❌ Bu fayl yaroqli ExamGuard bazasi emas yoki buzilgan. Hech narsa o'zgartirilmadi.")
            return
        rows = await asyncio.to_thread(_count_rows, tmp)
        await install_db_live(tmp)
    except Exception as e:
        await message.answer(f"❌ Tiklab bo'lmadi: <code>{esc(str(e))}</code>")
        return

    _backup_ready = BACKUP_ENABLED
    request_backup()
    await message.answer(f"✅ Baza tiklandi: {rows} ta yozuv. Guruhlar, vazifalar va javoblar qaytdi.")


async def install_db_live(tmp_path: str):
    """Bot ishlab turganda bazani almashtiradi: ulanish yopiladi, fayl almashadi, kesh qayta yuklanadi."""
    global _db_conn
    async with _db_lock:
        _db_gate.clear()
        try:
            if _db_conn is not None:
                await _db_conn.close()
                _db_conn = None
            await asyncio.to_thread(_install_db_file, tmp_path, DB_NAME, True)
        finally:
            _db_gate.set()
    _approved_groups_cache.clear()
    _seen.clear()
    _admin_cache.clear()
    _approval_asked.clear()
    await init_db()


# ============================================================================
# /members: bot qaysi a'zolarni taniydi
# ============================================================================
@dp.message(Command("members"))
async def cmd_members(message: Message):
    if not await group_guard(message, admin_only=True):
        return
    stats = await member_stats(message.chat.id)
    if not stats:
        await message.answer("⚠️ A'zolar statistikasini olib bo'lmadi. Bot guruhda administrator ekanini tekshiring.")
        return
    await message.answer(
        f"👥 Guruhda jami: <b>{stats['total']}</b> a'zo\n"
        f"✅ Bot taniydigan o'quvchilar: <b>{stats['known']}</b>\n"
        f"❓ Hali tanish emas: <b>{stats['unknown']}</b>\n\n"
        f"Telegram a'zolar ro'yxatini bermaydi: bot o'quvchini u guruhga xabar yozgach yoki yangi qo'shilganda taniydi. "
        f"Tanish bo'lmaganlar vazifa tugaganda tekshirilmaydi va ban qilinmaydi."
    )


# ============================================================================
# OBUNA (PULLIK TARIF) VA TO'LOV TIZIMI
# ============================================================================
# Oqim:
#  1. Guruhda /obuna -> "Obuna bo'lish" tugmasi
#  2. Tugma bosilsa: "💳 Karta raqami" va "👤 Admin bilan bog'lanish" tugmalari chiqadi
#  3. Karta raqami ko'rsatiladi. "🧾 Chekni yuborish" tugmasi botning shaxsiy chatiga olib boradi
#  4. O'quvchi/Ustoz tarifni tanlab, chek (rasm yoki PDF) yuboradi -> yaratuvchiga tugmali so'rov boradi
#  5. Yaratuvchi tasdiqlasa, guruh obunasi faollashadi
def fmt_price(n: int) -> str:
    return f"{n:,}".replace(",", " ")


def plans_text() -> str:
    return "\n".join(
        f"{p['icon']} <b>{p['title']}</b> — {fmt_price(p['price'])} so'm" for p in PLANS.values()
    )


def fmt_until(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%Y-%m-%d %H:%M")


def admin_button() -> InlineKeyboardButton:
    return InlineKeyboardButton(text="👤 Admin bilan bog'lanish", url=f"https://t.me/{ADMIN_USERNAME}")


async def subscription_status(chat_id: int) -> Tuple[bool, Optional[datetime], Optional[str]]:
    row = await q_one("SELECT sub_until, sub_plan FROM groups WHERE chat_id = ?", (chat_id,))
    until = parse_utc(row[0]) if row and row[0] else None
    active = bool(until and until > datetime.now(UTC))
    return active, until, (row[1] if row else None)


async def extend_subscription(chat_id: int, days: int, plan: str) -> Optional[datetime]:
    """Faol obuna bo'lsa uning ustiga qo'shadi, bo'lmasa hozirdan boshlaydi. Guruh bazada bo'lmasa None."""
    if await q_one("SELECT 1 FROM groups WHERE chat_id = ?", (chat_id,)) is None:
        return None
    active, until, _ = await subscription_status(chat_id)
    base = until if (active and until) else datetime.now(UTC)
    new_until = base + timedelta(days=days)
    await run(
        "UPDATE groups SET sub_until = ?, sub_plan = ? WHERE chat_id = ?",
        (new_until.strftime("%Y-%m-%d %H:%M:%S"), plan, chat_id),
    )
    return new_until


async def grant_trial_if_new(chat_id: int):
    """Yangi tasdiqlangan guruhga bir martalik sinov muddati (TRIAL_DAYS)."""
    if TRIAL_DAYS <= 0:
        return
    until = (datetime.now(UTC) + timedelta(days=TRIAL_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    await run("UPDATE groups SET sub_until = ?, sub_plan = 'trial' WHERE chat_id = ? AND sub_until IS NULL",
              (until, chat_id))


def subscribe_keyboard(chat_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="💎 Obuna bo'lish", callback_data=f"sub_open_{chat_id}"),
    ]])


async def require_subscription(message: Message) -> bool:
    """Yangi vazifa berish (/task, /extend) uchun faol obuna kerak. Berilgan vazifalar o'z muddatigacha ishlayveradi."""
    if not SUBSCRIPTION_REQUIRED:
        return True
    active, until, _ = await subscription_status(message.chat.id)
    if active:
        return True
    await message.reply(
        "💎 <b>Guruh obunasi faol emas.</b>\n"
        "Yangi vazifa berish uchun obuna bo'ling:\n\n" + plans_text(),
        reply_markup=subscribe_keyboard(message.chat.id),
    )
    return False


@dp.message(Command("obuna", "subscribe", "upgrade"))
async def cmd_obuna(message: Message):
    if not await group_guard(message):
        return
    active, until, plan = await subscription_status(message.chat.id)
    if active and until:
        left_days = max(0, (until - datetime.now(UTC)).days)
        status = f"✅ <b>Faol</b> — {fmt_until(until)} gacha (taxminan {left_days} kun qoldi)"
        if plan in ("trial", "grace"):
            status += "\n🎁 <i>Hozir sinov muddatidasiz.</i>"
    else:
        status = "❌ <b>Faol emas</b>"
    await message.reply(
        f"💎 <b>Guruh obunasi</b>\n\n{status}\n\n<b>Tariflar:</b>\n{plans_text()}",
        reply_markup=subscribe_keyboard(message.chat.id),
    )


async def _safe_edit(call: CallbackQuery, text: str, kb: InlineKeyboardMarkup):
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except Exception as e:
        logging.info(f"Xabarni tahrirlab bo'lmadi: {e}")


@dp.callback_query(F.data.startswith("sub_open_"))
async def sub_open(call: CallbackQuery):
    chat_id = int(call.data.split("_", 2)[2])
    await call.answer()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 Karta raqami", callback_data=f"sub_card_{chat_id}")],
        [admin_button()],
    ])
    await _safe_edit(call, f"💎 <b>Obuna tariflari</b>\n\n{plans_text()}\n\nTo'lov usulini tanlang:", kb)


@dp.callback_query(F.data.startswith("sub_card_"))
async def sub_card(call: CallbackQuery):
    chat_id = int(call.data.split("_", 2)[2])
    await call.answer()
    me = await bot.me()
    if PAYMENT_CARD:
        card_lines = f"💳 <b>Karta raqami:</b> <code>{esc(PAYMENT_CARD)}</code>\n"
        if PAYMENT_CARD_OWNER:
            card_lines += f"👤 <b>Karta egasi:</b> {esc(PAYMENT_CARD_OWNER)}\n"
    else:
        card_lines = "⚠️ Karta raqami hali sozlanmagan. Iltimos, admin bilan bog'laning.\n"
    text = (
        f"💳 <b>To'lov</b>\n\n{card_lines}\n{plans_text()}\n\n"
        f"To'lovni amalga oshirgach, chekni yuborish uchun <b>«🧾 Chekni yuborish»</b> tugmasini bosing "
        f"yoki admin bilan bog'laning. Admin tasdiqlagach, obuna faollashadi."
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧾 Chekni yuborish", url=f"https://t.me/{me.username}?start=pay_{chat_id}")],
        [admin_button()],
        [InlineKeyboardButton(text="◀️ Orqaga", callback_data=f"sub_open_{chat_id}")],
    ])
    await _safe_edit(call, text, kb)


async def begin_payment(message: Message, chat_arg: str):
    """Shaxsiy chat: /start pay_<chat_id> orqali kelgan foydalanuvchiga tarif tanlatadi."""
    try:
        chat_id = int(chat_arg)
    except ValueError:
        await message.answer("⚠️ Havola noto'g'ri. Guruhdagi /obuna tugmasidan qayta urinib ko'ring.")
        return
    if await q_one("SELECT 1 FROM groups WHERE chat_id = ?", (chat_id,)) is None:
        await message.answer("⚠️ Bu guruh bazada topilmadi. Guruhdagi /obuna tugmasidan qayta urinib ko'ring.")
        return
    try:
        title = (await bot.get_chat(chat_id)).title or str(chat_id)
    except Exception:
        title = str(chat_id)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{p['icon']} {p['title']} — {fmt_price(p['price'])} so'm",
                              callback_data=f"paypick_{chat_id}_{key}")]
        for key, p in PLANS.items()
    ])
    await message.answer(f"🧾 <b>Obuna to'lovi</b>\n📌 Guruh: <b>{esc(title)}</b>\n\nTarifni tanlang:", reply_markup=kb)


@dp.callback_query(F.data.startswith("paypick_"))
async def pay_pick(call: CallbackQuery):
    payload = call.data[len("paypick_"):]
    chat_part, _, plan_key = payload.rpartition("_")
    plan = PLANS.get(plan_key)
    try:
        chat_id = int(chat_part)
    except ValueError:
        chat_id = 0
    if not plan or not chat_id or await q_one("SELECT 1 FROM groups WHERE chat_id = ?", (chat_id,)) is None:
        await call.answer("Xatolik: tarif yoki guruh topilmadi.", show_alert=True)
        return

    user = call.from_user
    await run("DELETE FROM payments WHERE user_id = ? AND status = 'awaiting'", (user.id,))
    await run(
        "INSERT INTO payments (chat_id, user_id, user_name, plan, amount, status, created_at) "
        "VALUES (?, ?, ?, ?, ?, 'awaiting', ?)",
        (chat_id, user.id, raw_name(user), plan_key, plan["price"], utc_str()),
    )
    await call.answer()
    card = f"\n💳 Karta: <code>{esc(PAYMENT_CARD)}</code>" if PAYMENT_CARD else ""
    kb = InlineKeyboardMarkup(inline_keyboard=[[admin_button()]])
    await _safe_edit(
        call,
        f"✅ Tarif: <b>{plan['title']}</b> — {fmt_price(plan['price'])} so'm{card}\n\n"
        f"To'lovni amalga oshiring va <b>chekni (skrinshot yoki PDF)</b> shu chatga yuboring. "
        f"Admin tekshirib, obunani faollashtiradi.",
        kb,
    )


@dp.message(F.chat.type == "private", F.photo | F.document)
async def on_receipt(message: Message):
    user = message.from_user
    if not user or (message.caption or "").startswith("/"):
        return
    row = await q_one(
        "SELECT id, chat_id, plan, amount FROM payments WHERE user_id = ? AND status = 'awaiting' "
        "ORDER BY id DESC LIMIT 1",
        (user.id,),
    )
    if not row:
        return  # to'lov kutilmayapti: e'tiborsiz (masalan /restore uchun yuborilgan fayl)
    pid, chat_id, plan_key, amount = row

    if message.photo:
        file_id, ptype = message.photo[-1].file_id, "photo"
    else:
        doc = message.document
        if doc.file_name == BACKUP_FILENAME:
            return
        file_id, ptype = doc.file_id, "document"

    await run(
        "UPDATE payments SET status = 'pending', proof_file_id = ?, proof_type = ?, user_name = ? WHERE id = ?",
        (file_id, ptype, raw_name(user), pid),
    )
    try:
        title = (await bot.get_chat(chat_id)).title or str(chat_id)
    except Exception:
        title = str(chat_id)
    plan = PLANS.get(plan_key, {"title": plan_key})
    caption = (
        f"🧾 <b>Yangi to'lov cheki #{pid}</b>\n"
        f"👤 {esc(raw_name(user))} (ID: <code>{user.id}</code>"
        f"{', @' + esc(user.username) if user.username else ''})\n"
        f"📌 Guruh: <b>{esc(title)}</b> (<code>{chat_id}</code>)\n"
        f"💰 Tanlagan tarif: <b>{plan['title']}</b> — {fmt_price(amount or 0)} so'm\n\n"
        f"Chekni tekshiring va to'langan tarifni tasdiqlang:"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"✅ {p['title']} ({fmt_price(p['price'])})", callback_data=f"payok_{pid}_{key}")
         for key, p in PLANS.items()],
        [InlineKeyboardButton(text="❌ Rad etish", callback_data=f"payno_{pid}")],
    ])
    try:
        if ptype == "photo":
            await bot.send_photo(CREATOR_ID, file_id, caption=caption, reply_markup=kb)
        else:
            await bot.send_document(CREATOR_ID, file_id, caption=caption, reply_markup=kb)
    except Exception as e:
        logging.error(f"Chekni yaratuvchiga yuborib bo'lmadi: {e}")
        await message.answer(
            "⚠️ Chekni adminga yetkazib bo'lmadi. Iltimos, to'g'ridan-to'g'ri admin bilan bog'laning.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[admin_button()]]),
        )
        return
    await message.answer("✅ Chek qabul qilindi. Admin tekshirib, obunani tez orada faollashtiradi. Rahmat!")


@dp.callback_query(F.data.startswith("payok_"))
async def pay_approve(call: CallbackQuery):
    if call.from_user.id != CREATOR_ID:
        await call.answer("Bu tugma faqat admin uchun.", show_alert=True)
        return
    try:
        _, pid_text, plan_key = call.data.split("_")
        pid = int(pid_text)
        plan = PLANS[plan_key]
    except (ValueError, KeyError):
        await call.answer("Noto'g'ri ma'lumot.", show_alert=True)
        return

    row = await q_one("SELECT chat_id, user_id FROM payments WHERE id = ?", (pid,))
    if not row:
        await call.answer("To'lov topilmadi.", show_alert=True)
        return
    chat_id, user_id = row
    # Atomik: ikki marta bosilsa, obuna ikki marta qo'shilmaydi
    changed = await run(
        "UPDATE payments SET status = 'approved', approved_plan = ?, decided_at = ? WHERE id = ? AND status = 'pending'",
        (plan_key, utc_str(), pid),
    )
    if not changed:
        await call.answer("Bu to'lov allaqachon ko'rib chiqilgan.", show_alert=True)
        return

    new_until = await extend_subscription(chat_id, plan["days"], plan_key)
    await call.answer("Tasdiqlandi")
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    if new_until is None:
        await call.message.reply(f"⚠️ To'lov #{pid} tasdiqlandi, lekin guruh ({chat_id}) bazada topilmadi.")
        return
    until_text = fmt_until(new_until)
    await call.message.reply(f"✅ To'lov #{pid} tasdiqlandi: <b>{plan['title']}</b>. Obuna {until_text} gacha.")
    try:
        await bot.send_message(chat_id, f"💎 <b>Guruh obunasi faollashdi!</b>\n{plan['title']} — {until_text} gacha. Rahmat!")
    except Exception as e:
        logging.warning(f"Guruhga obuna xabari yuborilmadi: {e}")
    try:
        await bot.send_message(user_id, f"✅ To'lovingiz tasdiqlandi! Obuna {until_text} gacha faol. Rahmat!")
    except Exception:
        pass


@dp.callback_query(F.data.startswith("payno_"))
async def pay_reject(call: CallbackQuery):
    if call.from_user.id != CREATOR_ID:
        await call.answer("Bu tugma faqat admin uchun.", show_alert=True)
        return
    try:
        pid = int(call.data.split("_")[1])
    except (ValueError, IndexError):
        await call.answer("Noto'g'ri ma'lumot.", show_alert=True)
        return
    row = await q_one("SELECT user_id FROM payments WHERE id = ?", (pid,))
    changed = await run(
        "UPDATE payments SET status = 'rejected', decided_at = ? WHERE id = ? AND status = 'pending'",
        (utc_str(), pid),
    )
    if not row or not changed:
        await call.answer("Bu to'lov allaqachon ko'rib chiqilgan.", show_alert=True)
        return
    await call.answer("Rad etildi")
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await call.message.reply(f"❌ To'lov #{pid} rad etildi.")
    try:
        await bot.send_message(
            row[0],
            "❌ To'lovingiz tasdiqlanmadi. Iltimos, chekni tekshiring yoki admin bilan bog'laning.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[admin_button()]]),
        )
    except Exception:
        pass


def parse_grant_arg(arg: str) -> Optional[Tuple[int, str]]:
    arg = arg.strip().lower()
    if arg in PLANS:
        return PLANS[arg]["days"], arg
    m = re.fullmatch(r"(\d{1,4})d", arg)
    if m:
        return int(m.group(1)), arg
    return None


@dp.message(Command("grant"))
async def cmd_grant(message: Message, command: CommandObject):
    """Yaratuvchi uchun: guruhga qo'lda obuna berish. Guruhda: /grant 1m. Shaxsiy chatda: /grant 1m <chat_id>.
    Formatlar: 1m, 1y, 45d. Obunani to'xtatish: /grant 0d"""
    if not message.from_user or message.from_user.id != CREATOR_ID:
        return
    args = (command.args or "").split()
    usage = ("ℹ️ Foydalanish:\n<code>/grant 1m</code> (guruhda) yoki <code>/grant 1m &lt;chat_id&gt;</code>\n"
             "Formatlar: <code>1m</code>, <code>1y</code>, <code>45d</code>; to'xtatish: <code>0d</code>")
    parsed = parse_grant_arg(args[0]) if args else None
    if not parsed:
        await message.answer(usage)
        return
    if message.chat.type in GROUP_TYPES:
        chat_id = message.chat.id
    elif len(args) >= 2 and re.fullmatch(r"-?\d+", args[1]):
        chat_id = int(args[1])
    else:
        await message.answer(usage)
        return

    days, plan = parsed
    if days == 0:
        changed = await run(
            "UPDATE groups SET sub_until = ?, sub_plan = 'revoked' WHERE chat_id = ?", (utc_str(), chat_id)
        )
        await message.answer("🛑 Obuna to'xtatildi." if changed else "⚠️ Guruh bazada topilmadi.")
        return
    new_until = await extend_subscription(chat_id, days, plan)
    if new_until is None:
        await message.answer("⚠️ Guruh bazada topilmadi (bot u yerda tasdiqlangan bo'lishi kerak).")
        return
    await message.answer(f"✅ Obuna berildi: {fmt_until(new_until)} gacha.")
    if message.chat.type not in GROUP_TYPES:
        try:
            await bot.send_message(chat_id, f"💎 <b>Guruh obunasi faollashdi!</b> {fmt_until(new_until)} gacha.")
        except Exception:
            pass


@dp.message(Command("subs"))
async def cmd_subs(message: Message):
    """Yaratuvchi uchun: barcha guruhlar obunasi ro'yxati."""
    if message.chat.type != "private" or not message.from_user or message.from_user.id != CREATOR_ID:
        return
    rows = await q_all(
        "SELECT chat_id, COALESCE(teacher_name, '-'), is_approved, sub_until, COALESCE(sub_plan, '-') FROM groups "
        "ORDER BY is_approved DESC, sub_until DESC"
    )
    if not rows:
        await message.answer("📂 Hozircha guruhlar yo'q.")
        return
    now = datetime.now(UTC)
    lines = []
    for chat_id, teacher, approved, sub_until, plan in rows:
        until = parse_utc(sub_until) if sub_until else None
        if until and until > now:
            status = f"✅ {fmt_until(until)} ({esc(str(plan))})"
        else:
            status = "❌ faol emas"
        lines.append(f"• <code>{chat_id}</code> — {esc(str(teacher))}{'' if approved else ' ⏳tasdiqsiz'}\n  {status}")
    await send_chunks(message, "💎 <b>Guruhlar obunasi:</b>\n", lines)


# ============================================================================
# SOG'LIQNI TEKSHIRISH SERVERI (Render web service + UptimeRobot uchun)
# ============================================================================
async def start_health_server() -> Optional[web.AppRunner]:
    port = os.getenv("PORT")
    if not port:
        return None  # VPS da kerak emas

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

    health_runner = await start_health_server()  # Render portni tezroq ko'rishi uchun birinchi
    scheduler = backup_task = None
    try:
        try:
            restore_status = await restore_db_if_needed()
        except Exception as e:
            logging.critical(
                f"❌ Zaxira nusxani tekshirib bo'lmadi: {e}. Bot BO'SH baza bilan ishga TUSHMAYDI "
                f"(ma'lumotni yo'qotmaslik uchun). Qayta uriniladi."
            )
            await asyncio.sleep(10)
            raise SystemExit(1)
        logging.info(f"Baza holati: {restore_status}")

        await init_db()
        await arm_backups(restore_status)
        await set_bot_commands()

        scheduler = asyncio.create_task(check_deadlines())
        backup_task = asyncio.create_task(backup_loop())
        print("🛡 ExamGuard AI qat'iy intizom va anticheat rejimida ishga tushdi!")
        await dp.start_polling(
            bot,
            allowed_updates=["message", "callback_query", "my_chat_member", "chat_member"],
        )
    finally:
        for t in (scheduler, backup_task):
            if t:
                t.cancel()
        try:
            await asyncio.wait_for(backup_db(), timeout=20)  # o'chirilishdan oldin oxirgi nusxa (o'zgarish bo'lsa)
        except Exception as e:
            logging.error(f"Oxirgi zaxira nusxa yuborilmadi: {e}")
        if _db_conn:
            await _db_conn.close()
        if health_runner:
            await health_runner.cleanup()
        await bot.session.close()


def run_app():
    # Linux/macOS da uvloop o'rnatilgan bo'lsa tezroq ishlaydi (pip install uvloop), Windows da oddiy asyncio
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
