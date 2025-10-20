import os
import asyncio
import logging
from typing import List, Optional, Tuple
from datetime import datetime, timezone, timedelta

import aiosqlite
from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.bot import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext

# ------------- CONFIG -------------
BOT_TOKEN = os.getenv("BOT_TOKEN") or "8211678683:AAFM8zcP3U2R6kTZmRZf2LyThhrbixD-2kk"
ADMIN_USER_ID = int(os.getenv("1820002044") or "1820002044")
DB_PATH = "keys.db"
LOCAL_TZ = timezone(timedelta(hours=+6))  # Asia/Almaty offset

# ------------- LOGGING -------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("keybot")

# ------------- DB -------------
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key_text TEXT NOT NULL UNIQUE
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS issued_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                key_text TEXT NOT NULL,
                issued_at TEXT NOT NULL
            )
        """)
        await db.commit()

async def add_keys(keys: List[str]) -> int:
    if not keys:
        return await count_keys()
    async with aiosqlite.connect(DB_PATH) as db:
        for k in keys:
            k = k.strip()
            if not k:
                continue
            await db.execute("INSERT OR IGNORE INTO keys (key_text) VALUES (?)", (k,))
        await db.commit()
        async with db.execute("SELECT COUNT(*) FROM keys") as cur:
            row = await cur.fetchone()
            return int(row[0]) if row else 0

async def pop_random_key() -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, key_text FROM keys ORDER BY RANDOM() LIMIT 1") as cur:
            row = await cur.fetchone()
            if not row:
                return None
            key_id, key_text = row
        await db.execute("DELETE FROM keys WHERE id = ?", (key_id,))
        await db.commit()
        return str(key_text)

async def count_keys() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM keys") as cur:
            row = await cur.fetchone()
            return int(row[0]) if row else 0

async def log_issued(user_id: int, key_text: str):
    ts = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO issued_log (user_id, key_text, issued_at) VALUES (?, ?, ?)", (user_id, key_text, ts))
        await db.commit()

async def get_last_issued(user_id: int) -> Optional[Tuple[str, datetime]]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT key_text, issued_at FROM issued_log WHERE user_id = ? ORDER BY id DESC LIMIT 1",
            (user_id,)
        ) as cur:
            row = await cur.fetchone()
            if not row:
                return None
            key_text, iso = row
            return (key_text, datetime.fromisoformat(iso).astimezone(LOCAL_TZ))

# ------------- UI -------------
def main_keyboard(is_admin: bool) -> InlineKeyboardMarkup:
    if is_admin:
        buttons = [
            [
                InlineKeyboardButton(text="🎟 Получить ключ на 1 год", callback_data="GET_YEAR_KEY"),
                InlineKeyboardButton(text="📦 Осталось ключей", callback_data="REMAINING"),
            ],
            [
                InlineKeyboardButton(text="🕒 Последний ключ", callback_data="LAST_ISSUED"),
                InlineKeyboardButton(text="➕ Добавить ключи", callback_data="ADD_KEYS"),
            ]
        ]
    else:
        buttons = [[InlineKeyboardButton(text="📦 Осталось ключей", callback_data="REMAINING")]]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ------------- FSM -------------
class AddKeysFlow(StatesGroup):
    waiting_for_keys = State()

# ------------- HELPERS -------------
def is_admin_user_id(user_id: int) -> bool:
    return ADMIN_USER_ID != 0 and user_id == ADMIN_USER_ID

def split_keys(text: str) -> List[str]:
    if not text:
        return []
    txt = text.strip()
    for sep in [",", ";"]:
        txt = txt.replace(sep, "\n")
    raw_parts = [p.strip() for p in txt.splitlines() if p.strip()]
    keys = []
    for line in raw_parts:
        for part in line.split():
            if part.strip():
                keys.append(part.strip())
    return list(dict.fromkeys(keys))

def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S %Z")

# ------------- ROUTER -------------
router = Router()

@router.message(F.text == "/start")
async def start(message: Message):
    is_admin = is_admin_user_id(message.from_user.id)
    await message.answer(
        "Приветствую в генератор ключей для сертификата на айфон!\n\n"
        "Используй кнопки ниже.",
        reply_markup=main_keyboard(is_admin)
    )

@router.callback_query(F.data == "GET_YEAR_KEY")
async def cb_get_year_key(callback: CallbackQuery):
    await callback.answer()
    if not is_admin_user_id(callback.from_user.id):
        await callback.message.answer("Эта функция доступна только владельцу.", reply_markup=main_keyboard(False))
        return
    key = await pop_random_key()
    if key is None:
        await callback.message.answer("Ключи закончились — база пуста.", reply_markup=main_keyboard(True))
        return
    await log_issued(callback.from_user.id, key)
    text = ("Юз бота @one_ibot\n"
            "Вы можете получить сертификат здесь, используя этот ключ:\n"
            f"<code>{key}</code>")
    await callback.message.answer(text, reply_markup=main_keyboard(True))

@router.callback_query(F.data == "REMAINING")
async def cb_remaining(callback: CallbackQuery):
    await callback.answer()
    cnt = await count_keys()
    is_admin = is_admin_user_id(callback.from_user.id)
    await callback.message.answer(f"Осталось ключей: <b>{cnt}</b>", reply_markup=main_keyboard(is_admin))

@router.callback_query(F.data == "LAST_ISSUED")
async def cb_last_issued(callback: CallbackQuery):
    await callback.answer()
    if not is_admin_user_id(callback.from_user.id):
        await callback.message.answer("Доступно только владельцу.", reply_markup=main_keyboard(False))
        return
    last = await get_last_issued(callback.from_user.id)
    if not last:
        await callback.message.answer("Ещё ни одного ключа не выдавалось.", reply_markup=main_keyboard(True))
        return
    key_text, dt = last
    await callback.message.answer(f"Последний ключ: <code>{key_text}</code>\nВыдан: <b>{fmt_dt(dt)}</b>",
                                  reply_markup=main_keyboard(True))

@router.callback_query(F.data == "ADD_KEYS")
async def cb_add_keys_begin(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if not is_admin_user_id(callback.from_user.id):
        await callback.message.answer("Добавлять ключи может только владелец.", reply_markup=main_keyboard(False))
        return
    await state.set_state(AddKeysFlow.waiting_for_keys)
    await callback.message.answer("Отправь список ключей (через запятую, точку с запятой или с новой строки).")

@router.message(AddKeysFlow.waiting_for_keys)
async def add_keys_collect(message: Message, state: FSMContext):
    if not is_admin_user_id(message.from_user.id):
        await state.clear()
        await message.answer("Недостаточно прав.", reply_markup=main_keyboard(False))
        return
    text = (message.text or "").strip()
    keys = split_keys(text)
    if not keys:
        await message.answer("Не нашёл валидных ключей. Отправь ещё раз или нажми любую кнопку для выхода.")
        return
    total = await add_keys(keys)
    await state.clear()
    await message.answer(f"Готово. Всего ключей в базе: <b>{total}</b>", reply_markup=main_keyboard(True))

# ------------- MAIN -------------
async def main():
    if BOT_TOKEN == "ВАШ_ТОКЕН_ЗДЕСЬ":
        logger.error("Поставь BOT_TOKEN в переменную окружения.")
        return
    await init_db()
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    logger.info("Bot started (aiogram v3, fixed adding)")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
