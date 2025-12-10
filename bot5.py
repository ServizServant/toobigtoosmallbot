import asyncio
import time
import hashlib
import sqlite3
import logging
import re
import requests
import os
from typing import List, Optional, Tuple

from dotenv import load_dotenv
load_dotenv()

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramRetryAfter

# =========================
# НАСТРОЙКИ из .env
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = list(map(int, os.getenv("ADMIN_IDS", "").split(",")))
SHEET_ID = os.getenv("SHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME")
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service_account.json")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", 10))
NOTIFY_DELAY = int(os.getenv("NOTIFY_DELAY", 2))

DB_ORDERS = os.getenv("DB_ORDERS", "orders.db")
DB_SUBS = os.getenv("DB_SUBS", "subs.db")

MAX_COLS = int(os.getenv("MAX_COLS", 25))
MAX_MESSAGE_LENGTH = int(os.getenv("MAX_MESSAGE_LENGTH", 4000))

# Логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =========================
# DATABASE ORDERS
# =========================
def init_db_orders():
    conn = sqlite3.connect(DB_ORDERS)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            row_index INTEGER PRIMARY KEY,
            hash TEXT NOT NULL,
            line TEXT NOT NULL,
            updated_at REAL DEFAULT (strftime('%s','now'))
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS pending (
            row_index INTEGER PRIMARY KEY,
            hash TEXT NOT NULL,
            line TEXT NOT NULL,
            ts REAL NOT NULL,
            is_new BOOLEAN DEFAULT 1
        )
    """)
    conn.commit()
    conn.close()

def upsert_order(row_index:int, hash_:str, line:str):
    conn = sqlite3.connect(DB_ORDERS)
    c = conn.cursor()
    c.execute("""
        INSERT INTO orders (row_index, hash, line, updated_at)
        VALUES (?, ?, ?, strftime('%s','now'))
        ON CONFLICT(row_index) DO UPDATE SET
            hash=excluded.hash,
            line=excluded.line,
            updated_at=excluded.updated_at
    """, (row_index, hash_, line))
    conn.commit()
    conn.close()

def upsert_pending(row_index:int, hash_:str, line:str, ts:float, is_new:bool=True):
    conn = sqlite3.connect(DB_ORDERS)
    c = conn.cursor()
    c.execute("""
        INSERT INTO pending (row_index, hash, line, ts, is_new)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(row_index) DO UPDATE SET
            hash=excluded.hash,
            line=excluded.line,
            ts=excluded.ts,
            is_new=excluded.is_new
    """, (row_index, hash_, line, ts, is_new))
    conn.commit()
    conn.close()

def pop_ready(now_ts:float, delay:float):
    conn = sqlite3.connect(DB_ORDERS)
    c = conn.cursor()
    c.execute("SELECT row_index, hash, line, is_new FROM pending WHERE ts <= ?", (now_ts - delay,))
    ready = c.fetchall()
    if ready:
        row_indices = [str(r[0]) for r in ready]
        c.execute(f"DELETE FROM pending WHERE row_index IN ({','.join(['?']*len(row_indices))})", row_indices)
    conn.commit()
    conn.close()
    return ready

def get_order(row_index:int) -> Optional[Tuple]:
    conn = sqlite3.connect(DB_ORDERS)
    c = conn.cursor()
    row = c.execute("SELECT row_index, hash, line FROM orders WHERE row_index=?", (row_index,)).fetchone()
    conn.close()
    return row

# =========================
# DATABASE SUBSCRIBERS
# =========================
def init_db_subs():
    conn = sqlite3.connect(DB_SUBS)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS subscribers (
            chat_id INTEGER PRIMARY KEY
        )
    """)
    conn.commit()
    conn.close()

def add_subscriber(chat_id:int):
    conn = sqlite3.connect(DB_SUBS)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)", (chat_id,))
    conn.commit()
    conn.close()

def remove_subscriber(chat_id:int):
    conn = sqlite3.connect(DB_SUBS)
    c = conn.cursor()
    c.execute("DELETE FROM subscribers WHERE chat_id=?", (chat_id,))
    conn.commit()
    conn.close()

def get_subscribers() -> List[int]:
    conn = sqlite3.connect(DB_SUBS)
    c = conn.cursor()
    result = [r[0] for r in c.execute("SELECT chat_id FROM subscribers").fetchall()]
    conn.close()
    return result

# =========================
# GOOGLE SHEETS
# =========================
def get_sheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
    client = gspread.authorize(creds)
    doc = client.open_by_key(SHEET_ID)

    # Если SHEET_NAME не указан → берем первый лист
    if not SHEET_NAME:
        return doc.get_worksheet(0)
    else:
        return doc.worksheet(SHEET_NAME)

# =========================
# UTILS
# =========================
def normalize_row(vals:List) -> List[str]:
    vals = vals or []
    vals = (vals + [""] * MAX_COLS)[:MAX_COLS]
    return [str(v).strip() if v is not None else "" for v in vals]

def is_empty_row(vals:List[str]) -> bool:
    return all(v == "" for v in vals)

def make_hash(vals:List[str]) -> str:
    joined = "␟".join(vals)
    return hashlib.md5(joined.encode("utf-8")).hexdigest()

def make_line(vals:List[str], max_length:int=50) -> str:
    parts = []
    for v in vals:
        if v:
            parts.append(v[:max_length] + "..." if len(v) > max_length else v)
    return " | ".join(parts)[:MAX_MESSAGE_LENGTH - 100]

def is_url(text:str) -> bool:
    return re.match(r'https?://\S+', text) is not None

def shorten_clck(long_url:str) -> str:
    try:
        r = requests.get("https://clck.ru/--", params={"url": long_url}, timeout=5)
        if r.status_code == 200:
            return r.text.strip()
        return f"Ошибка: {r.status_code}"
    except Exception as e:
        return f"Ошибка: {e}"

async def send_safe(bot:Bot, chat_id:int, text:str):
    try:
        await bot.send_message(chat_id, text)
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        await bot.send_message(chat_id, text)
    except:
        pass

# =========================
# POLL LOOP
# =========================
async def poll_loop(bot:Bot):
    first_run = True
    sheet = get_sheet()
    while True:
        rows = sheet.get_values("A1:Z", major_dimension="ROWS")
        now = time.time()
        for i, raw_row in enumerate(rows[1:], start=2):
            vals = normalize_row(raw_row)
            if is_empty_row(vals):
                continue
            line = make_line(vals)
            hash_ = make_hash(vals)
            prev = get_order(i)

            if prev is None:
                upsert_order(i, hash_, line)
                if not first_run:
                    upsert_pending(i, hash_, line, now, True)
            elif prev[1] != hash_:
                upsert_order(i, hash_, line)
                upsert_pending(i, hash_, line, now, False)

        first_run = False
        events = pop_ready(time.time(), NOTIFY_DELAY)
        for row_index, hash_, line, is_new in events:
            msg = (
                f"🆕 Новый заказ (строка {row_index}): {line}"
                if is_new else
                f"♻ Обновлён заказ (строка {row_index}): {line}"
            )
            for chat_id in get_subscribers():
                await send_safe(bot, chat_id, msg)

        await asyncio.sleep(POLL_INTERVAL)

# =========================
# BOT
# =========================
async def main():
    init_db_orders()
    init_db_subs()
    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()

    sub_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Подписаться на рассылку")],
            [KeyboardButton(text="Отписаться от рассылки")]
        ],
        resize_keyboard=True
    )

    @dp.message(Command("start"))
    async def cmd_start(msg:types.Message):
        await msg.answer("Привет! Я умею сокращать ссылки и рассылать заказы.", reply_markup=sub_kb)

    @dp.message()
    async def sub_buttons(msg:types.Message):
        text = msg.text
        if text == "Подписаться на рассылку":
            add_subscriber(msg.from_user.id)
            await msg.answer("✅ Вы подписаны!", reply_markup=sub_kb)
        elif text == "Отписаться от рассылки":
            remove_subscriber(msg.from_user.id)
            await msg.answer("❌ Вы отписались.", reply_markup=sub_kb)
        elif is_url(text):
            short = shorten_clck(text)
            if short.startswith("http"):
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton("Открыть короткую ссылку", url=short)]]
                )
                await msg.answer(f"🔗 Короткая ссылка: {short}", reply_markup=kb)
            else:
                await msg.answer(f"⚠ Не удалось сократить ссылку.\nОтвет: {short}")

    asyncio.create_task(poll_loop(bot))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
