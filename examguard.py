import asyncio
import html
import logging
import os
import re
import sys
import time
from datetime import datetime, time as dtime, timedelta
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import aiosqlite
import pytz
from aiogram import BaseMiddleware, Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    BotCommand,
    CallbackQuery,
    ChatMemberUpdated,
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
# SOZLAMALAR (.env faylidan o'qiladi)
# ============================================================================
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    sys.exit("❌ XATOLIK: BOT_TOKEN topilmadi! .env faylga BOT_TOKEN=... deb yozing.")

CREATOR_ID = int(os.getenv("CREATOR_ID", "7439126820"))

DB_NAME = "examguard.db"
TZ = pytz.timezone("Asia/Tashkent")
UTC = pytz.utc
CHECK_INTERVAL = 2            # Skeyuler tekshiruv oralig'i (soniya)
BAN_DAYS = 3                  # Muddatida topshirmaganlar uchun chetlashtirish kunlari
WARN_MINUTES = 30             # Tugashiga necha daqiqa qolganda ogohlantirish
DURATION_TOLERANCE = 0        # 0 = aynan bir xil soniya; 1 = ±1 soniya farq
ALLOW_FORWARDED_VOICE = False
ADMIN_CACHE_TTL = 60          # Adminlik tekshiruvi keshi (soniya)
GROUP_TYPES = ("group", "supergroup")

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

TASK_RE = re.compile(r"^(?:(\d+)\s+)?(\d{1,2}):(\d{2})\s+(.+)$", re.S)
ANSWER_RE = re.compile(r"^/answer(?:@\w+)?(?:\s+(.*))?$", re.I | re.S)
TASK_CAPTION_RE = re.compile(r"^/task(?:@\w+)?(?:\s|$)", re.I)

esc = html.escape

# ============================================================================
# BAZA VA KESHLAR (Tezkor ishlash va poyga holatidan himoya)
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

    print("📁 Ma'lumotlar bazasi xavfsiz yangilandi va kesh yuklandi!")


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
        await message.answer("⚠️ Kechirasiz, bu buyruq faqat o'quv guruhida ishlaydi.")
        return False

    if not await is_group_approved(message.chat.id):
        await message.reply(
            "⚠️ Hurmatli a'zolar, bu guruh bot yaratuvchisi tomonidan hali tasdiqlanmagan. "
            "Tasdiq olinmaguncha tizim buyruqlari yopiq turadi."
        )
        return False

    if admin_only and not await is_user_admin(message):
        await message.reply(
            "⛔️ Kechirasiz, bu buyruq faqat guruh <b>Ustoziga</b> (administratorga) tegishli. "
            "Ustoz stuliga o'tirish uchun avval imtihondan a'lo bahoga o'tish kerak 😉"
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
            return True, "updated"

        await db.execute(
            "INSERT INTO answers (chat_id, task_id, user_id, full_name, question_num, answer_type, content, "
            "file_unique_id, duration, submitted_at) VALUES (?, ?, ?, ?, ?, 'voice', ?, ?, ?, ?)",
            (chat_id, task_id, user.id, raw_name(user), question_num,
             voice.file_id, voice.file_unique_id, voice.duration, utc_str()),
        )
        await db.commit()
    return True, "success"


async def process_answer(message: Message, args_text: str):
    user = message.from_user
    name = label(user)
    args = (args_text or "").split()

    if not args or not re.fullmatch(r"[0-9]{1,2}", args[0]):
        await message.reply(
            f"⚠️ Hurmatli <b>{name}</b>, qaysi savolga javob berayotganingizni ko'rsatmadingiz.\n"
            "<b>To'g'ri namuna:</b> O'zingizning ovozli xabaringizga reply qilib <code>/answer 1</code> yuboring."
        )
        return

    question_num = str(int(args[0]))
    task = await get_active_task(message.chat.id)
    if not task:
        await message.reply(f"⚠️ Hurmatli <b>{name}</b>, hozirda faol vazifa mavjud emas. Ustoz e'lon qilishini kuting.")
        return
    task_id, task_text, _, created_at, _ = task

    numbers = task_question_numbers(task_text)
    if numbers and question_num not in numbers:
        await message.reply(
            f"⚠️ Hurmatli <b>{name}</b>, ushbu vazifada <b>{question_num}-savol</b> mavjud emas. "
            f"Mavjud savollar raqami: <b>{', '.join(sorted(numbers, key=int))}</b>. Iltimos, diqqatli bo'ling."
        )
        return

    voice_msg = None
    if message.voice:
        voice_msg = message
    elif message.reply_to_message and message.reply_to_message.voice:
        voice_msg = message.reply_to_message

    if not voice_msg:
        await message.reply(
            f"⚠️ Hurmatli <b>{name}</b>, imtihon talabiga ko'ra matn emas, jonli ovozingiz baholanadi! 🎙\n"
            f"Avval ovozli xabar yuboring, so'ngra unga reply qilib <code>/answer {question_num}</code> deb yozing."
        )
        return

    voice = voice_msg.voice

    if voice_msg.from_user and voice_msg.from_user.id != user.id:
        await message.reply(f"⛔️ <b>{name}</b>, faqat o'zingiz yozgan ovozli xabarga reply qilishingiz mumkin. Boshqalarning javobidan foydalanish taqiqlanadi!")
        return

    if not ALLOW_FORWARDED_VOICE and is_forwarded(voice_msg):
        await message.reply(
            f"⛔️ <b>{name}</b>, forward (uzatilgan) ovozli xabarlar qabul qilinmaydi! Iltimos, o'zingiz yangidan ovoz yozing."
        )
        return

    created = parse_utc(created_at) if created_at else None
    voice_date = voice_msg.date
    if voice_date and voice_date.tzinfo is None:
        voice_date = UTC.localize(voice_date)
    if created and voice_date and voice_date < created - timedelta(seconds=5):
        await message.reply(
            f"⛔️ <b>{name}</b>, bu ovoz vazifa berilishidan oldin yozilgan ekan. Vazifadan keyin yangi javob yozing!"
        )
        return

    success, err = await save_answer(message.chat.id, task_id, user, question_num, voice)
    if success and err == "updated":
        await message.reply(
            f"🔄 Rahmat, <b>{name}</b>! <b>{question_num}-savol</b> uchun yuborgan yangi ovozingiz ({voice.duration} sek) avvalgisining o'rniga qabul qilindi."
        )
    elif success:
        await message.reply(
            f"✅ Barakalla, <b>{name}</b>! <b>{question_num}-savol</b> javobingiz muvaffaqiyatli qabul qilindi ({voice.duration} sek). Shu ruhda davom eting!"
        )
    elif err in ("duplicate_file", "duplicate_duration"):
        await message.reply(
            f"🛑 <b>{name}</b>, bu ovozli xabarni (yoki aynan shu soniyadagi xabarni) boshqa savolga ishlatgansiz! "
            f"Anticheat qoidasiga ko'ra har bir savolga alohida, har xil soniyadagi voice yuborishingiz shart."
        )
    elif err == "other_user_file":
        await message.reply(f"⛔️ <b>{name}</b>, bu ovoz boshqa o'quvchiga tegishli! Faqat o'z bilimingizga tayaning.")
    elif err == "no_task":
        await message.reply("⚠️ Afsuski, vazifa muddati yakunlangan yoki bekor qilingan. Javob qabul qilinmadi.")
    else:
        await message.reply("⚠️ Texnik xatolik yuz berdi, iltimos qaytadan urinib ko'ring.")


# ============================================================================
# BOT GURUHGA QO'SHILISHI / CHIQISHI (Yaratuvchi ruxsati)
# ============================================================================
def intro_text() -> str:
    return (
        "🛡 <b>ExamGuard AI guruhga muvaffaqiyatli ulandi va ishga tushdi!</b>\n\n"
        "🤖 <b>Asosiy vazifamiz:</b> CEFR Speaking va Writing imtihonlariga tayyorgarlik jarayonida temir intizom va adolatli nazoratni ta'minlash.\n\n"
        "👨‍🏫 <b>Ustozlar uchun:</b> <code>/task 21:00 1. What is your hobby?</code> (rasm bilan ham mumkin)\n"
        "👩‍🎓 <b>O'quvchilar uchun:</b> O'zingizning ovozli xabaringizga reply qilib <code>/answer &lt;raqam&gt;</code> yuboring.\n"
        "⚠️ <b>Anticheat:</b> Har bir savolga alohida va har xil soniyadagi ovoz yuborilishi shart.\n"
        f"⚠️ <i>Qoida barchaga barobar: belgilangan muddatda topshirmaganlar {BAN_DAYS} kunga guruhdan chetlashtiriladi.</i>\n\n"
        "To'liq qo'llanma: /help"
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
            "⚠️ <i>Xavfsizlik qoidasiga ko'ra, botni ishlatish uchun yaratuvchining rasmiy tasdig'i kutilmoqda.</i>",
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
            f"🔔 <b>Yangi guruhdan botni ulash so'rovi keldi!</b>\n\n"
            f"👤 <b>Qo'shgan shaxs:</b> {esc(adder_name)} (ID: <code>{adder_id}</code>, {esc(adder_username)})\n"
            f"📌 <b>Guruh nomi:</b> {esc(chat_title)}\n"
            f"🆔 <b>Chat ID:</b> <code>{chat_id}</code>\n\n"
            f"Ruxsat berasizmi?",
            reply_markup=keyboard,
        )
    except Exception as e:
        logging.error(f"Yaratuvchiga xabar yuborib bo'lmadi: {e}")
        try:
            await bot.send_message(
                chat_id,
                "⚠️ Yaratuvchiga so'rov yuborilmadi. Yaratuvchi avval botga shaxsiy chatda /start bosishi lozim.",
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
        await call.message.edit_text(f"⚠️ Guruh (ID: {chat_id}) topilmadi. Bot guruhdan chiqarilgan bo'lishi mumkin.")
        return

    _approved_groups_cache.add(chat_id)
    await call.message.edit_text(f"✅ Guruh (ID: {chat_id}) muvaffaqiyatli tasdiqlandi!")
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
            "❌ Yaratuvchi tomonidan ushbu guruhga ruxsat berilmadi. Bot guruhni tark etmoqda. Xayr!",
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
            "🛡 <b>ExamGuard AI</b> tizimiga xush kelibsiz!\n\n"
            "Meni o'quv guruhiga qo'shib administrator qilsangiz, vazifalar va intizomni qat'iy nazorat qilishni boshlayman. "
            "Muddatlar bilan hazillashmayman, lekin har doim adolatli va xushmuomalama bo'lib qolaman 🙂"
        )
        return

    if not await is_group_approved(message.chat.id):
        await message.reply("⚠️ Bu guruh hali tasdiqlanmagan.")
        return

    if await is_user_admin(message):
        await message.reply("🛡 <b>ExamGuard AI</b> guruhda to'liq nazorat rejimida ishlamoqda, Ustoz!")
    else:
        await message.reply(
            f"Hurmatli <b>{label(message.from_user)}</b>, /start bosish bilan vazifa bajarilib qolmaydi 😉 "
            "Yaxshisi /tasks buyrug'ini ochib, darsga kirishing."
        )


@dp.message(Command("help"))
async def cmd_help(message: Message):
    if not await group_guard(message, allow_private=True):
        return
    await message.answer(
        "🤖 <b>ExamGuard AI — Qo'llanma</b> 📖\n\n"
        "👨‍🏫 <b>Ustoz uchun (Adminlar):</b>\n"
        "• <code>/task &lt;soat:min&gt; &lt;matn&gt;</code> — bugungi muddatga vazifa berish.\n"
        "• <code>/task &lt;kun&gt; &lt;soat:min&gt; &lt;matn&gt;</code> — kelgusi kunlarga vazifa.\n"
        "• Rasmli vazifa: rasm yuborib, izohiga <code>/task 21:00 1. ...</code> yozing.\n"
        "• <code>/extend</code> — muddatga 1 soat qo'shish.\n"
        "• <code>/cancel_task</code> — joriy vazifani bekor qilish.\n"
        "• <code>/banned_list</code> — chetlashtirilganlar ro'yxati.\n\n"
        "👩‍🎓 <b>O'quvchilar uchun:</b>\n"
        "• O'zingizning ovozli xabaringizga reply qilib: <code>/answer &lt;raqam&gt;</code>.\n"
        "• <code>/tasks</code> — faol vazifa. <code>/submissions</code> — topshirganlar ro'yxati.\n\n"
        "⚠️ <b>Anticheat va intizom qoidalari:</b>\n"
        "• Har bir savolga alohida va har xil soniyadagi ovoz yuboring. Bir xil yoki takroriy soniyadagi javob o'tmaydi.\n"
        "• Forward qilingan, boshqa o'quvchiga tegishli yoki vazifadan oldin yozilgan ovoz qabul qilinmaydi.\n"
        "• Xato yuborgan bo'lsangiz, o'sha savolga qayta voice yuborsangiz, eskisi yangisiga almashadi.\n"
        f"• Muddatida topshirmaganlar ogohlantirishsiz {BAN_DAYS} kunga chetlashtiriladi."
    )


@dp.message(Command("rules"))
async def cmd_rules(message: Message):
    if not await group_guard(message, allow_private=True):
        return
    await message.answer(
        f"📜 <b>Intizom qoidalari:</b>\n"
        f"1. Vazifa belgilangan muddatda, faqat jonli ovozli xabar bilan topshiriladi.\n"
        f"2. Muddatida topshirmagan o'quvchi qoidaga ko'ra {BAN_DAYS} kunga guruhdan chetlashtiriladi.\n"
        f"3. Ovozni takrorlash, boshqadan ko'chirish yoki forward qilish qat'iyan taqiqlanadi.\n"
        f"Tartib hammaga bir xil. Intizom — muvaffaqiyat garovi!"
    )


# ============================================================================
# USTOZ BUYRUQLARI
# ============================================================================
TASK_FORMAT_HELP = (
    "⚠️ Ustoz, format biroz noto'g'ri kiritildi. To'g'ri namunalar:\n"
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
        await message.answer("⚠️ Ustoz, soat 0-23, daqiqa 0-59 oralig'ida bo'lishi kerak.")
        return
    if days > 30:
        await message.answer("⚠️ Kun soni 30 kundan oshmasligi kerak.")
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

    await send_task_card(
        chat_id,
        "📢 <b>YANGI CEFR VAZIFASI E'LON QILINDI!</b> 🎯",
        task_text,
        deadline_str,
        photo_id,
        "🎙 O'zingizning ovozli xabaringizga reply qilib <code>/answer &lt;raqam&gt;</code> yuboring.\n"
        f"⚠️ <i>Diqqat: muddatida topshirmaganlar {BAN_DAYS} kunga guruhdan chetlashtiriladi.</i>",
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
    await message.answer("ℹ️ <b>Joriy vazifa Ustoz tomonidan bekor qilindi.</b>")


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
        "Ustoz qo'shimcha <b>1 soat</b> vaqt berdilar. Ushbu imkoniyatdan oqilona foydalaning.\n"
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
        await message.answer("🛡 Hozirda chetlashtirilgan o'quvchilar yo'q. Guruh intizomi a'lo darajada!")
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
        "🎙 Javob berish: o'zingizning ovozli xabaringizga reply qilib <code>/answer &lt;raqam&gt;</code> yuboring.",
    )


@dp.message(Command("answer"))
async def cmd_answer(message: Message, command: CommandObject):
    if message.chat.type == "private":
        await message.answer("⚠️ Kechirasiz, javobingizni guruhga yuborishingiz shart.")
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
        await message.answer("📂 Hozircha hech kim vazifa topshirmagan. Barchadan faollik kutamiz!")
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
    await send_chunks(message, f"📋 <b>Topshirganlar ro'yxati — {len(users)} nafar o'quvchi:</b>\n", lines)


# ============================================================================
# SKEYULER: 30 daqiqalik ogohlantirish va muddat tugaganda ban
# ============================================================================
async def finish_task(task_id: int, chat_id: int):
    db = await get_db()
    async with _db_lock:
        cur = await db.execute("UPDATE tasks SET is_active = 0 WHERE id = ? AND is_active = 1", (task_id,))
        await db.commit()
        if cur.rowcount == 0:
            return

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
                    "✅ Vazifa topshirish vaqti yakunlandi. Barcha o'quvchilar o'z vaqtida topshirgan — intizom a'lo darajada. Rahmat!",
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

        try:
            await bot.send_message(
                chat_id,
                "⛔️ <b>Vazifa topshirish vaqti yakunlandi.</b>\n"
                f"Belgilangan muddatda vazifani bajarmagan o'quvchilar qoidaga muvofiq {BAN_DAYS} kunga guruhdan chetlashtirildilar. "
                f"Intizom — natija kaliti. {BAN_DAYS} kundan so'ng ko'rishguncha!",
            )
        except Exception as e:
            logging.warning(f"[{chat_id}] xabar yuborilmadi: {e}")

    if failed:
        try:
            await bot.send_message(
                chat_id,
                f"⚠️ {failed} nafar o'quvchini chetlashtira olmadim. Iltimos, menga adminlik sozlamalaridan "
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
            "⏰ <b>DIQQAT: OGOHLANTIRISH!</b>\n"
            f"Vazifa topshirish muddatiga atigi <b>{WARN_MINUTES} daqiqa</b> qoldi. Hali javob yubormaganlar — shoshiling, "
            f"keyin \"internet o'chib qoldi\" kabi bahonalar o'tmaydi 😉\n"
            f"Muddatida topshirmaganlar {BAN_DAYS} kunga chetlashtiriladilar.",
        )
    except Exception as e:
        logging.warning(f"[{chat_id}] ogohlantirish yuborilmadi: {e}")


async def check_deadlines():
    tick = 0
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
# ISHGA TUSHIRISH
# ============================================================================
async def main():
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    await init_db()
    await set_bot_commands()
    scheduler = asyncio.create_task(check_deadlines())
    print("🛡 ExamGuard AI qat'iy intizom, xushmuomala nazorat va anticheat rejimida ishga tushdi!")
    try:
        await dp.start_polling(
            bot,
            allowed_updates=["message", "callback_query", "my_chat_member", "chat_member"],
        )
    finally:
        scheduler.cancel()
        if _db_conn:
            await _db_conn.close()
        await bot.session.close()


from aiohttp import web

from aiohttp import web

async def handle_ping(request):
    return web.Response(text="OK", status=200)

async def web_server():
    app = web.Application()
    app.router.add_get("/", handle_ping)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

async def main_with_web():
    await web_server()
    await main()

def run_app():
    try:
        import uvloop  # type: ignore

        if sys.version_info >= (3, 11):
            with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
                runner.run(main_with_web())
            return
    except ImportError:
        pass
    asyncio.run(main_with_web())


if __name__ == "__main__":
    try:
        run_app()
    except KeyboardInterrupt:
        print("Bot to'xtatildi!")