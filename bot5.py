#!/usr/bin/env python3
import os
import time
import json
import asyncio
import hashlib
import sqlite3
import logging
import re
import requests
from typing import List, Optional, Tuple

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.exceptions import TelegramRetryAfter

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

BOT_TOKEN = os.getenv("BOT_TOKEN")
SHEET_ID = os.getenv("SHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME")
ADMIN_IDS = []
POLL_INTERVAL = 10
NOTIFY_DELAY = 2
MAX_COLS = 25

SERVICE_ACCOUNT_PATH = "/app/service_account.json"

# DB files
DB_ORDERS = "orders.db"
DB_SUBS = "subs.db"

# -------------------------------------------------------------------
# LOGGING
# -------------------------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("bot")

# -------------------------------------------------------------------
# GOOGLE SHEETS (НОРМАЛЬНАЯ КОРОТКАЯ ВЕРСИЯ)
# -------------------------------------------------------------------

def get_sheet():
    if not os.path.exists(SERVICE_ACCOUNT_PATH):
        raise RuntimeError(f"Файл сервисного аккаунта не найден: {SERVICE_ACCOUNT_PATH}")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        SERVICE_ACCOUNT_PATH, scope
    )
    client = gspread.authorize(creds)

    doc = client.open_by_key(SHEET_ID)

    if SHEET_NAME:
        return doc.worksheet(SHEET_NAME)

    return doc.get_worksheet(0)

# -------------------------------------------------------------------
# DATABASE — ORDERS
# -------------------------------------------------------------------

def init_db_orders():
    conn = sqlite3.connect(DB_ORDERS)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            row_index INTEGER PRIMARY KEY,
            hash TEXT NOT NULL,
            line TEXT NOT NULL,
            updated_at REAL
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS pending (
            row_index INTEGER PRIMARY KEY,
            hash TEXT NOT NULL,
            line TEXT NOT NULL,
            ts REAL NOT NULL,
            is_new INTEGER DEFAULT 1
        )
    """)
    conn.commit()
    conn.close()

def upsert_order(row_index, hash_, line):
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

def upsert_pending(row_index, hash_, line, ts, is_new):
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

def get_order(row_index):
    conn = sqlite3.connect(DB_ORDERS)
    c = conn.cursor()
    row = c.execute("SELECT row_index, hash, line FROM orders WHERE row_index=?", (row_index,)).fetchone()
    conn.close()
    return row

def pop_ready(now_ts, delay):
    conn = sqlite3.connect(DB_ORDERS)
    c = conn.cursor()
    c.execute("SELECT row_index, hash, line, is_new FROM pending WHERE ts <= ?", (now_ts - delay,))
    ready = c.fetchall()
    if ready:
        ids = [str(r[0]) for r in ready]
        c.execute(f"DELETE FROM pending WHERE row_index IN ({','.join(['?'] * len(ids))})", ids)
    conn.commit()
    conn.close()
    return ready

# -------------------------------------------------------------------
# DATABASE — SUBSCRIBERS
# -------------------------------------------------------------------

def init_db_subs():
    conn = sqlite3.connect(DB_SUBS)
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS subscribers (chat_id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()

def add_subscriber(chat_id):
    conn = sqlite3.connect(DB_SUBS)
    c = conn.cursor()
    c.execute("INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)", (chat_id,))
    conn.commit()
    conn.close()

def remove_subscriber(chat_id):
    conn = sqlite3.connect(DB_SUBS)
    c = conn.cursor()
    c.execute("DELETE FROM subscribers WHERE chat_id=?", (chat_id,))
    conn.commit()
    conn.close()

def get_subscribers():
    conn = sqlite3.connect(DB_SUBS)
    c = conn.cursor()
    arr = [r[0] for r in c.execute("SELECT chat_id FROM subscribers").fetchall()]
    conn.close()
    return arr

# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------

def normalize_row(vals):
    vals = vals or []
    vals = (vals + [""] * MAX_COLS)[:MAX_COLS]
    return [str(v).strip() for v in vals]

def make_hash(vals):
    return hashlib.md5("␟".join(vals).encode()).hexdigest()

def make_line(vals, max_len=50):
    parts = []
    for v in vals:
        if v:
            parts.append(v[:max_len] + "..." if len(v) > max_len else v)
    return " | ".join(parts)

def is_url(text):
    return re.match(r'https?://\S+', text) is not None

def shorten_clck(url):
    try:
        r = requests.get("https://clck.ru/--", params={"url": url}, timeout=7)
        if r.status_code == 200:
            return r.text.strip()
        return f"Ошибка: {r.status_code}"
    except Exception as e:
        return f"Ошибка: {e}"

async def send_safe(bot, chat_id, text):
    try:
        await bot.send_message(chat_id, text)
    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)
        await bot.send_message(chat_id, text)

# -------------------------------------------------------------------
# POLLING SHEET
# -------------------------------------------------------------------

async def poll_loop(bot):
    first_run = True

    while True:
        try:
            sheet = get_sheet()
        except Exception as e:
            logger.error("Ошибка доступа к таблице: %s", e)
            await asyncio.sleep(POLL_INTERVAL)
            continue

        rows = sheet.get_values("A1:Z", major_dimension="ROWS")
        now = time.time()

        for i, row in enumerate(rows[1:], start=2):
            vals = normalize_row(row)
            if all(v == "" for v in vals):
                continue

            line = make_line(vals)
            hash_ = make_hash(vals)
            prev = get_order(i)

            if prev is None:
                upsert_order(i, hash_, line)
                if not first_run:
                    upsert_pending(i, hash_, line, now, 1)
            elif prev[1] != hash_:
                upsert_order(i, hash_, line)
                upsert_pending(i, hash_, line, now, 0)

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

# -------------------------------------------------------------------
# BOT
# -------------------------------------------------------------------

async def main():
    init_db_orders()
    init_db_subs()

    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()

    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Подписаться на рассылку")],
            [KeyboardButton(text="Отписаться от рассылки")]
        ],
        resize_keyboard=True
    )

    @dp.message(Command("start"))
    async def cmd_start(msg):
        await msg.answer("Привет! Я сокращаю ссылки и присылаю новые заказы.", reply_markup=kb)

    @dp.message()
    async def handler(msg: types.Message):
        text = msg.text.strip()

        if text == "Подписаться на рассылку":
            add_subscriber(msg.chat.id)
            return await msg.answer("Вы подписались ✓")

        if text == "Отписаться от рассылки":
            remove_subscriber(msg.chat.id)
            return await msg.answer("Вы отписались ✗")

        if is_url(text):
            short = shorten_clck(text)
            if short.startswith("http"):
                kb_inline = InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="Открыть", url=short)]]
                )
                return await msg.answer(f"Короткая ссылка: {short}", reply_markup=kb_inline)
            else:
                return await msg.answer(f"Ошибка сокращения: {short}")

    asyncio.create_task(poll_loop(bot))
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
