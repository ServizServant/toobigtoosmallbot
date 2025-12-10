#!/usr/bin/env python3
import os
import io
import json
import time
import asyncio
import hashlib
import sqlite3
import logging
import re
import requests
from typing import List, Optional, Tuple

from dotenv import load_dotenv
load_dotenv()

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.exceptions import TelegramRetryAfter

# -----------------------
# Logging
# -----------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------
# Environment variables and defaults
# -----------------------
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required in environment variables")

# ADMIN_IDS: safe parsing (can be empty)
raw_admins = os.getenv("ADMIN_IDS", "")
if raw_admins:
    ADMIN_IDS = []
    for part in raw_admins.split(","):
        s = part.strip()
        if not s:
            continue
        try:
            ADMIN_IDS.append(int(s))
        except ValueError:
            logger.warning("Skipping invalid ADMIN_IDS item: %r", s)
else:
    ADMIN_IDS = []

SHEET_ID = os.getenv("SHEET_ID")  # spreadsheet ID (key) OR you can use SHEET_URL below
SHEET_URL = os.getenv("SHEET_URL")  # optional alternative: full URL of spreadsheet
if not (SHEET_ID or SHEET_URL):
    # We won't raise here — will fail later when trying to open sheet; but user likely should provide SHEET_ID or SHEET_URL
    logger.warning("Neither SHEET_ID nor SHEET_URL provided; Google Sheets access will fail until set.")

# If SHEET_NAME not provided — we'll automatically use first worksheet
SHEET_NAME = os.getenv("SHEET_NAME")  # if None or empty -> use first sheet

SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service_account.json")
# Optionally, user may provide full JSON content (string) in SERVICE_ACCOUNT_JSON env var (or base64)
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")  # raw JSON string
SERVICE_ACCOUNT_JSON_BASE64 = os.getenv("SERVICE_ACCOUNT_JSON_BASE64")  # optional base64-encoded JSON

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))
NOTIFY_DELAY = float(os.getenv("NOTIFY_DELAY", "2"))

DB_ORDERS = os.getenv("DB_ORDERS", "orders.db")
DB_SUBS = os.getenv("DB_SUBS", "subs.db")

MAX_COLS = int(os.getenv("MAX_COLS", "25"))
MAX_MESSAGE_LENGTH = int(os.getenv("MAX_MESSAGE_LENGTH", "4000"))

# -----------------------
# Utils for service account file resolution
# -----------------------
def find_service_json_search_paths():
    """
    Typical additional locations to check (bothost / platform quirks).
    You can extend this list if needed.
    """
    # project root first, then some common dirs
    paths = [
        os.path.join(os.getcwd(), SERVICE_ACCOUNT_FILE),
        os.path.join(os.path.dirname(__file__), SERVICE_ACCOUNT_FILE) if "__file__" in globals() else None,
        "/app/" + SERVICE_ACCOUNT_FILE,
        "/app/service_account.json",
        "/home/service_account.json",
        "/workspace/" + SERVICE_ACCOUNT_FILE,
        "/tmp/" + SERVICE_ACCOUNT_FILE,
    ]
    return [p for p in paths if p]

def find_service_json_auto():
    """
    Conservative filesystem search for service_account.json — limited depth to avoid expensive full root walk.
    We'll check common locations and then (as fallback) do a limited os.walk from cwd.
    """
    # check the candidate paths first
    for p in find_service_json_search_paths():
        try:
            if p and os.path.exists(p):
                return p
        except Exception:
            continue

    # limited search in project subtree (avoid walking entire root for speed; bothost usually places files near app)
    start = os.getcwd()
    max_depth = 3
    for root, dirs, files in os.walk(start):
        depth = root[len(start):].count(os.sep)
        if "service_account.json" in files:
            return os.path.join(root, "service_account.json")
        if depth >= max_depth:
            # don't dive deeper into this subtree
            dirs[:] = []
    return None

def prepare_service_account_file():
    """
    Determine how to get credentials:
    1) If SERVICE_ACCOUNT_JSON provided (raw JSON) -> write temp file and return path
    2) If SERVICE_ACCOUNT_JSON_BASE64 provided -> decode -> write -> return path
    3) If SERVICE_ACCOUNT_FILE path exists -> use it
    4) Try auto find -> return path
    Raises RuntimeError if not found.
    """
    # 1) raw JSON
    if SERVICE_ACCOUNT_JSON:
        try:
            info = json.loads(SERVICE_ACCOUNT_JSON)
            # write to temp file
            tmp = os.path.join("/tmp", "service_account_from_env.json")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(info, f, ensure_ascii=False)
            return tmp
        except Exception as e:
            raise RuntimeError(f"Invalid SERVICE_ACCOUNT_JSON content: {e}")

    # 2) base64 encoded
    if SERVICE_ACCOUNT_JSON_BASE64:
        try:
            import base64
            decoded = base64.b64decode(SERVICE_ACCOUNT_JSON_BASE64)
            tmp = os.path.join("/tmp", "service_account_from_env_b64.json")
            with open(tmp, "wb") as f:
                f.write(decoded)
            return tmp
        except Exception as e:
            raise RuntimeError(f"Invalid SERVICE_ACCOUNT_JSON_BASE64: {e}")

    # 3) path provided or default
    if SERVICE_ACCOUNT_FILE and os.path.exists(SERVICE_ACCOUNT_FILE):
        return SERVICE_ACCOUNT_FILE

    # 4) try more locations / auto search
    auto = find_service_json_auto()
    if auto:
        return auto

    # Not found
    raise RuntimeError("service_account.json not found (searched common locations). Please upload file or set SERVICE_ACCOUNT_JSON.")

# prepare global path (will raise if not found)
try:
    SERVICE_ACCOUNT_RESOLVED = prepare_service_account_file()
    logger.info("Using service account file: %s", SERVICE_ACCOUNT_RESOLVED)
except Exception as e:
    # keep None — will be handled when trying to access sheets
    SERVICE_ACCOUNT_RESOLVED = None
    logger.warning("Service account not resolved at startup: %s", e)

# -----------------------
# DATABASE ORDERS
# -----------------------
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

# -----------------------
# DATABASE SUBSCRIBERS
# -----------------------
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

# -----------------------
# GOOGLE SHEETS
# -----------------------
def get_sheet():
    """
    Returns a gspread worksheet object.
    Behavior:
    - If SERVICE_ACCOUNT_JSON is set -> use it.
    - Else use SERVICE_ACCOUNT_RESOLVED path discovered earlier.
    - Opens spreadsheet by SHEET_URL if present, else by SHEET_ID.
    - If SHEET_NAME not provided -> returns first worksheet.
    """
    if not (SHEET_ID or SHEET_URL):
        raise RuntimeError("SHEET_ID or SHEET_URL must be provided to access Google Sheets")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    # If raw JSON provided via env
    if SERVICE_ACCOUNT_JSON:
        try:
            info = json.loads(SERVICE_ACCOUNT_JSON)
            creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scope)  # type: ignore
        except Exception as e:
            raise RuntimeError(f"Invalid SERVICE_ACCOUNT_JSON: {e}")
    else:
        # SERVICE_ACCOUNT_RESOLVED may be None if not found at startup; try to resolve here again
        global SERVICE_ACCOUNT_RESOLVED
        if not SERVICE_ACCOUNT_RESOLVED:
            try:
                SERVICE_ACCOUNT_RESOLVED = prepare_service_account_file()
            except Exception as e:
                raise RuntimeError(f"service_account.json not found: {e}")
        creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_RESOLVED, scope)

    client = gspread.authorize(creds)

    # open spreadsheet
    if SHEET_URL:
        doc = client.open_by_url(SHEET_URL)
    else:
        doc = client.open_by_key(SHEET_ID)

    # choose sheet
    if not SHEET_NAME:
        # first worksheet
        ws = doc.get_worksheet(0)
    else:
        ws = doc.worksheet(SHEET_NAME)
    return ws

# -----------------------
# UTILS
# -----------------------
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
    """
    Shorten via clck.ru (as in your original).
    Returns short URL string on success or an error string otherwise.
    """
    try:
        r = requests.get("https://clck.ru/--", params={"url": long_url}, timeout=7)
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
    except Exception as e:
        logger.warning("Failed to send message to %s: %s", chat_id, e)

# -----------------------
# POLL LOOP: watches sheet, inserts into pending queue, notifies subscribers
# -----------------------
async def poll_loop(bot:Bot):
    first_run = True
    # We'll get sheet inside loop to be resilient to transient auth errors
    while True:
        try:
            sheet = get_sheet()
        except Exception as e:
            logger.error("Failed to open sheet: %s. Retrying in %s seconds...", e, POLL_INTERVAL)
            await asyncio.sleep(POLL_INTERVAL)
            continue

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

# -----------------------
# BOT: handlers and startup
# -----------------------
async def main():
    # init DBs
    init_db_orders()
    init_db_subs()

    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()

    # Reply keyboard
    sub_kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Подписаться на рассылку")],
            [KeyboardButton(text="Отписаться от рассылки")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )

    @dp.message(Command("start"))
    async def cmd_start(msg:types.Message):
        await msg.answer("Привет! Я умею сокращать ссылки и рассылать заказы.", reply_markup=sub_kb)

    @dp.message()
    async def sub_buttons(msg:types.Message):
        text = (msg.text or "").strip()
        if not text:
            return

        if text == "Подписаться на рассылку":
            add_subscriber(msg.from_user.id)
            await msg.answer("✅ Вы подписаны!", reply_markup=sub_kb)
            return

        if text == "Отписаться от рассылки":
            remove_subscriber(msg.from_user.id)
            await msg.answer("❌ Вы отписались.", reply_markup=sub_kb)
            return

        if is_url(text):
            short = shorten_clck(text)
            if short.startswith("http"):
                kb = InlineKeyboardMarkup(
                    inline_keyboard=[
                        [
                            InlineKeyboardButton(
                                text="Открыть короткую ссылку",
                                url=short
                            )
                        ]
                    ]
                )
                await msg.answer(f"🔗 Короткая ссылка: {short}", reply_markup=kb)
            else:
                await msg.answer(f"⚠ Не удалось сократить ссылку.\nОтвет: {short}")
            return

    # Start poll loop as background task
    asyncio.create_task(poll_loop(bot))

    # Start polling (aiogram starts long-polling loop)
    await dp.start_polling(bot)

# -----------------------
# Entrypoint
# -----------------------
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")
