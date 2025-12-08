import os
import re
import asyncio
import logging
import hashlib
import aiosqlite
from datetime import datetime, timedelta
from typing import Optional, List, Tuple, Dict, Any

import aiohttp
import gspread
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ParseMode
from oauth2client.service_account import ServiceAccountCredentials
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

# Конфигурация из переменных окружения
class Config:
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    SHEET_ID = os.getenv("SHEET_ID")
    SHEET_NAME = os.getenv("SHEET_NAME", "Лист1")
    POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "30"))
    NOTIFY_DELAY_SEC = int(os.getenv("NOTIFY_DELAY_SEC", "60"))
    DB_PATH = os.getenv("DB_PATH", "orders.db")
    SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE", "service_account.json")
    
    @classmethod
    def validate(cls):
        missing = []
        if not cls.TELEGRAM_BOT_TOKEN:
            missing.append("TELEGRAM_BOT_TOKEN")
        if not cls.SHEET_ID:
            missing.append("SHEET_ID")
        
        if missing:
            raise ValueError(f"Отсутствуют обязательные переменные окружения: {', '.join(missing)}")
        
        logger.info(f"Конфигурация загружена: poll_interval={cls.POLL_INTERVAL_SEC}s, notify_delay={cls.NOTIFY_DELAY_SEC}s")

# Инициализация Google Sheets
class GoogleSheetsClient:
    def __init__(self):
        self.worksheet = None
        self._last_error = None
        
    async def initialize(self):
        """Асинхронная инициализация Google Sheets"""
        try:
            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
                "https://www.googleapis.com/auth/drive"
            ]
            
            # Проверяем существование файла сервисного аккаунта
            if not os.path.exists(Config.SERVICE_ACCOUNT_FILE):
                raise FileNotFoundError(f"Файл сервисного аккаунта не найден: {Config.SERVICE_ACCOUNT_FILE}")
            
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                Config.SERVICE_ACCOUNT_FILE, scope
            )
            
            # Используем ThreadPoolExecutor для синхронных операций gspread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                client_future = executor.submit(gspread.authorize, creds)
                client = client_future.result()
                
                doc_future = executor.submit(client.open_by_key, Config.SHEET_ID)
                doc = doc_future.result()
                
                if Config.SHEET_NAME:
                    sheet_future = executor.submit(doc.worksheet, Config.SHEET_NAME)
                    self.worksheet = sheet_future.result()
                else:
                    self.worksheet = doc.sheet1
            
            logger.info(f"Google Sheets подключен: {Config.SHEET_NAME if Config.SHEET_NAME else 'первый лист'}")
            return True
            
        except Exception as e:
            self._last_error = str(e)
            logger.error(f"Ошибка подключения к Google Sheets: {e}")
            return False
    
    async def get_all_rows(self) -> Optional[List[List[str]]]:
        """Получение всех строк из таблицы"""
        if not self.worksheet:
            return None
            
        try:
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(self.worksheet.get_all_values)
                return future.result()
        except Exception as e:
            logger.error(f"Ошибка получения данных из таблицы: {e}")
            return None

# Работа с базой данных
class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        
    async def init_db(self):
        """Инициализация таблиц БД"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS subscribers (
                    chat_id INTEGER PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            await db.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    row_index INTEGER PRIMARY KEY,
                    hash TEXT NOT NULL,
                    line TEXT NOT NULL,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            await db.execute("""
                CREATE TABLE IF NOT EXISTS pending_notifications (
                    row_index INTEGER PRIMARY KEY,
                    hash TEXT NOT NULL,
                    line TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(row_index) REFERENCES orders(row_index)
                )
            """)
            
            # Индексы для ускорения запросов
            await db.execute("CREATE INDEX IF NOT EXISTS idx_pending_created ON pending_notifications(created_at)")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_orders_hash ON orders(hash)")
            
            await db.commit()
            logger.info("База данных инициализирована")
    
    async def add_subscriber(self, chat_id: int) -> bool:
        """Добавление подписчика"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    "INSERT OR IGNORE INTO subscribers (chat_id) VALUES (?)",
                    (chat_id,)
                )
                await db.commit()
                return True
        except Exception as e:
            logger.error(f"Ошибка добавления подписчика {chat_id}: {e}")
            return False
    
    async def remove_subscriber(self, chat_id: int) -> bool:
        """Удаление подписчика"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute(
                    "DELETE FROM subscribers WHERE chat_id = ?",
                    (chat_id,)
                )
                await db.commit()
                return True
        except Exception as e:
            logger.error(f"Ошибка удаления подписчика {chat_id}: {e}")
            return False
    
    async def get_subscribers(self) -> List[int]:
        """Получение всех подписчиков"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute("SELECT chat_id FROM subscribers")
                rows = await cursor.fetchall()
                return [row[0] for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения подписчиков: {e}")
            return []
    
    async def upsert_order(self, row_index: int, hash_value: str, line: str):
        """Обновление или добавление заказа"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("""
                    INSERT INTO orders (row_index, hash, line) 
                    VALUES (?, ?, ?)
                    ON CONFLICT(row_index) DO UPDATE SET 
                        hash = excluded.hash,
                        line = excluded.line,
                        updated_at = CURRENT_TIMESTAMP
                """, (row_index, hash_value, line))
                await db.commit()
        except Exception as e:
            logger.error(f"Ошибка обновления заказа {row_index}: {e}")
    
    async def get_order(self, row_index: int) -> Optional[Tuple]:
        """Получение заказа по номеру строки"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                cursor = await db.execute(
                    "SELECT row_index, hash, line FROM orders WHERE row_index = ?",
                    (row_index,)
                )
                return await cursor.fetchone()
        except Exception as e:
            logger.error(f"Ошибка получения заказа {row_index}: {e}")
            return None
    
    async def add_pending_notification(self, row_index: int, hash_value: str, line: str):
        """Добавление уведомления в очередь"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                await db.execute("""
                    INSERT OR REPLACE INTO pending_notifications (row_index, hash, line)
                    VALUES (?, ?, ?)
                """, (row_index, hash_value, line))
                await db.commit()
        except Exception as e:
            logger.error(f"Ошибка добавления уведомления {row_index}: {e}")
    
    async def get_ready_notifications(self, delay_seconds: int) -> List[Tuple]:
        """Получение готовых к отправке уведомлений"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                # Рассчитываем пороговое время
                threshold = datetime.now() - timedelta(seconds=delay_seconds)
                threshold_str = threshold.strftime('%Y-%m-%d %H:%M:%S')
                
                cursor = await db.execute("""
                    SELECT row_index, hash, line 
                    FROM pending_notifications 
                    WHERE created_at <= ?
                """, (threshold_str,))
                
                rows = await cursor.fetchall()
                
                # Удаляем обработанные уведомления
                await db.execute("DELETE FROM pending_notifications WHERE created_at <= ?", (threshold_str,))
                await db.commit()
                
                return rows
        except Exception as e:
            logger.error(f"Ошибка получения готовых уведомлений: {e}")
            return []

# Сервис сокращения ссылок
class UrlShortener:
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def initialize(self):
        """Инициализация HTTP-сессии"""
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
    
    async def close(self):
        """Закрытие HTTP-сессии"""
        if self.session:
            await self.session.close()
    
    @staticmethod
    def is_valid_url(text: str) -> bool:
        """Проверка, является ли текст URL"""
        pattern = re.compile(
            r'^https?://'  # http:// или https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # домен
            r'localhost|'  # localhost
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # IP
            r'(?::\d+)?'  # порт
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        return pattern.match(text) is not None
    
    async def shorten_url(self, long_url: str) -> str:
        """Сокращение ссылки через clck.ru"""
        if not self.is_valid_url(long_url):
            return "❌ Неверный URL. Убедитесь, что ссылка начинается с http:// или https://"
        
        try:
            async with self.session.get(
                "https://clck.ru/--",
                params={"url": long_url},
                headers={"User-Agent": "Mozilla/5.0"}
            ) as response:
                if response.status == 200:
                    short_url = (await response.text()).strip()
                    return short_url if short_url.startswith("http") else f"https://{short_url}"
                else:
                    return f"❌ Ошибка сервера: {response.status}"
        except asyncio.TimeoutError:
            return "❌ Таймаут при сокращении ссылки"
        except Exception as e:
            logger.error(f"Ошибка сокращения ссылки {long_url}: {e}")
            return f"❌ Ошибка: {str(e)}"

# Основной бот
class OrderNotificationBot:
    def __init__(self):
        self.bot: Optional[Bot] = None
        self.dp: Optional[Dispatcher] = None
        self.db: Optional[Database] = None
        self.sheets: Optional[GoogleSheetsClient] = None
        self.shortener: Optional[UrlShortener] = None
        self.scheduler: Optional[AsyncIOScheduler] = None
        
    async def initialize(self):
        """Инициализация всех компонентов"""
        logger.info("Инициализация бота...")
        
        # Валидация конфигурации
        Config.validate()
        
        # Инициализация компонентов
        self.bot = Bot(token=Config.TELEGRAM_BOT_TOKEN, parse_mode=ParseMode.HTML)
        self.dp = Dispatcher()
        
        self.db = Database(Config.DB_PATH)
        await self.db.init_db()
        
        self.sheets = GoogleSheetsClient()
        sheets_ok = await self.sheets.initialize()
        if not sheets_ok:
            raise ConnectionError("Не удалось подключиться к Google Sheets")
        
        self.shortener = UrlShortener()
        await self.shortener.initialize()
        
        self.scheduler = AsyncIOScheduler()
        
        # Регистрация обработчиков
        self._register_handlers()
        
        # Запуск планировщика
        await self._start_scheduler()
        
        logger.info("Бот инициализирован успешно")
    
    def _register_handlers(self):
        """Регистрация обработчиков команд"""
        
        @self.dp.message(Command("start"))
        async def start_command(message: types.Message):
            """Обработчик команды /start"""
            welcome_text = (
                "👋 <b>Добро пожаловать!</b>\n\n"
                "Я бот для:\n"
                "🔗 Сокращения ссылок через clck.ru\n"
                "📊 Отслеживания заказов из Google Таблиц\n\n"
                "📋 <b>Доступные команды:</b>\n"
                "/subscribe - Подписаться на уведомления\n"
                "/unsubscribe - Отписаться от уведомлений\n"
                "/status - Статус подписки\n"
                "/help - Помощь\n\n"
                "Просто отправьте мне ссылку для сокращения!"
            )
            
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Подписаться", callback_data="subscribe")],
                [InlineKeyboardButton(text="❌ Отписаться", callback_data="unsubscribe")],
                [InlineKeyboardButton(text="❓ Помощь", callback_data="help")]
            ])
            
            await message.answer(welcome_text, reply_markup=kb, parse_mode=ParseMode.HTML)
        
        @self.dp.message(Command("subscribe"))
        async def subscribe_command(message: types.Message):
            """Подписка на уведомления"""
            success = await self.db.add_subscriber(message.chat.id)
            if success:
                await message.answer(
                    "✅ <b>Вы успешно подписались на уведомления!</b>\n\n"
                    "Теперь вы будете получать сообщения о новых и обновленных заказах.",
                    parse_mode=ParseMode.HTML
                )
            else:
                await message.answer("❌ Произошла ошибка при подписке.")
        
        @self.dp.message(Command("unsubscribe"))
        async def unsubscribe_command(message: types.Message):
            """Отписка от уведомлений"""
            success = await self.db.remove_subscriber(message.chat.id)
            if success:
                await message.answer(
                    "❌ <b>Вы отписались от уведомлений.</b>\n\n"
                    "Чтобы снова получать уведомления, используйте /subscribe",
                    parse_mode=ParseMode.HTML
                )
            else:
                await message.answer("❌ Произошла ошибка при отписке.")
        
        @self.dp.message(Command("status"))
        async def status_command(message: types.Message):
            """Проверка статуса подписки"""
            subscribers = await self.db.get_subscribers()
            is_subscribed = message.chat.id in subscribers
            
            status_text = (
                f"📊 <b>Статус подписки:</b> {'✅ Подписан' if is_subscribed else '❌ Не подписан'}\n"
                f"👥 <b>Всего подписчиков:</b> {len(subscribers)}\n\n"
                f"🔄 <b>Интервал проверки:</b> {Config.POLL_INTERVAL_SEC} сек\n"
                f"⏳ <b>Задержка уведомлений:</b> {Config.NOTIFY_DELAY_SEC} сек"
            )
            
            await message.answer(status_text, parse_mode=ParseMode.HTML)
        
        @self.dp.message(Command("help"))
        async def help_command(message: types.Message):
            """Помощь"""
            help_text = (
                "🆘 <b>Помощь по боту</b>\n\n"
                "<b>Сокращение ссылок:</b>\n"
                "Просто отправьте мне любую ссылку, и я её сокращу через clck.ru\n\n"
                "<b>Отслеживание заказов:</b>\n"
                "Я автоматически проверяю Google Таблицу на наличие новых или измененных заказов.\n"
                "При обнаружении изменений отправляю уведомления всем подписчикам.\n\n"
                "<b>Команды:</b>\n"
                "/start - Начало работы\n"
                "/subscribe - Подписаться на уведомления\n"
                "/unsubscribe - Отписаться\n"
                "/status - Статус подписки\n"
                "/help - Эта справка\n\n"
                "<b>Поддержка:</b>\n"
                "При проблемах с ботом обратитесь к администратору."
            )
            
            await message.answer(help_text, parse_mode=ParseMode.HTML)
        
        @self.dp.callback_query(F.data == "subscribe")
        async def subscribe_callback(callback: types.CallbackQuery):
            """Обработка кнопки подписки"""
            success = await self.db.add_subscriber(callback.message.chat.id)
            if success:
                await callback.message.edit_text(
                    "✅ <b>Вы успешно подписались на уведомления!</b>",
                    parse_mode=ParseMode.HTML
                )
            else:
                await callback.message.edit_text("❌ Произошла ошибка при подписке.")
            await callback.answer()
        
        @self.dp.callback_query(F.data == "unsubscribe")
        async def unsubscribe_callback(callback: types.CallbackQuery):
            """Обработка кнопки отписки"""
            success = await self.db.remove_subscriber(callback.message.chat.id)
            if success:
                await callback.message.edit_text(
                    "❌ <b>Вы отписались от уведомлений.</b>",
                    parse_mode=ParseMode.HTML
                )
            else:
                await callback.message.edit_text("❌ Произошла ошибка при отписке.")
            await callback.answer()
        
        @self.dp.callback_query(F.data == "help")
        async def help_callback(callback: types.CallbackQuery):
            """Обработка кнопки помощи"""
            await help_command(callback.message)
            await callback.answer()
        
        @self.dp.message(F.text)
        async def text_message_handler(message: types.Message):
            """Обработка текстовых сообщений (сокращение ссылок)"""
            text = message.text.strip()
            
            # Игнорируем команды
            if text.startswith('/'):
                return
            
            if not self.shortener.is_valid_url(text):
                await message.answer(
                    "⚠️ <b>Это не похоже на ссылку.</b>\n\n"
                    "Отправьте мне ссылку в формате:\n"
                    "<code>https://example.com</code>\n"
                    "или\n"
                    "<code>http://example.com</code>",
                    parse_mode=ParseMode.HTML
                )
                return
            
            # Отправляем сообщение о начале обработки
            processing_msg = await message.answer("⏳ Сокращаю ссылку...")
            
            # Сокращаем ссылку
            short_url = await self.shortener.shorten_url(text)
            
            # Формируем ответ
            if short_url.startswith("http"):
                response_text = (
                    f"✅ <b>Ссылка сокращена!</b>\n\n"
                    f"🔗 <b>Исходная:</b>\n<code>{text}</code>\n\n"
                    f"🔗 <b>Сокращенная:</b>\n<code>{short_url}</code>"
                )
                
                kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="📋 Открыть короткую ссылку", url=short_url)],
                    [InlineKeyboardButton(text="📊 Скопировать", callback_data=f"copy:{short_url}")]
                ])
                
                await processing_msg.edit_text(response_text, reply_markup=kb, parse_mode=ParseMode.HTML)
            else:
                await processing_msg.edit_text(
                    f"❌ <b>Не удалось сократить ссылку</b>\n\n"
                    f"<b>Ошибка:</b> {short_url}",
                    parse_mode=ParseMode.HTML
                )
        
        @self.dp.callback_query(F.data.startswith("copy:"))
        async def copy_callback(callback: types.CallbackQuery):
            """Обработка кнопки копирования"""
            url = callback.data.split(":", 1)[1]
            await callback.answer(f"Ссылка скопирована: {url}", show_alert=True)
    
    async def _start_scheduler(self):
        """Запуск планировщика для проверки таблицы"""
        trigger = IntervalTrigger(seconds=Config.POLL_INTERVAL_SEC)
        self.scheduler.add_job(
            self.check_sheet_updates,
            trigger,
            id="sheet_check",
            replace_existing=True
        )
        self.scheduler.start()
        logger.info(f"Планировщик запущен с интервалом {Config.POLL_INTERVAL_SEC} сек")
    
    @staticmethod
    def normalize_row(values: List[str]) -> str:
        """Нормализация строки для сравнения"""
        filtered = [v.strip() for v in values if v and v.strip()]
        return " | ".join(filtered) if filtered else "Пустая строка"
    
    @staticmethod
    def calculate_hash(values: List[str]) -> str:
        """Вычисление хэша строки"""
        joined = "|".join([(v or "").strip() for v in values])
        return hashlib.sha256(joined.encode('utf-8')).hexdigest()
    
    async def silent_initialization(self):
        """Тихая инициализация - загрузка текущего состояния без уведомлений"""
        try:
            rows = await self.sheets.get_all_rows()
            if not rows:
                logger.warning("Таблица пуста или недоступна")
                return
            
            logger.info(f"Тихая инициализация: найдено {len(rows)} строк")
            
            # Начинаем со второй строки (первая - заголовки)
            for idx in range(1, len(rows)):
                row_values = rows[idx]
                if not any(row_values):
                    continue
                
                hash_value = self.calculate_hash(row_values)
                normalized_line = self.normalize_row(row_values)
                
                await self.db.upsert_order(idx + 1, hash_value, normalized_line)
            
            logger.info("Тихая инициализация завершена")
            
        except Exception as e:
            logger.error(f"Ошибка тихой инициализации: {e}")
    
    async def check_sheet_updates(self):
        """Проверка обновлений в таблице"""
        try:
            rows = await self.sheets.get_all_rows()
            if not rows:
                logger.warning("Не удалось получить данные из таблицы")
                return
            
            logger.debug(f"Проверка обновлений: {len(rows)} строк")
            
            for idx in range(1, len(rows)):
                row_index = idx + 1
                row_values = rows[idx]
                
                # Пропускаем пустые строки
                if not any(row_values):
                    continue
                
                # Вычисляем хэш и нормализуем строку
                hash_value = self.calculate_hash(row_values)
                normalized_line = self.normalize_row(row_values)
                
                # Проверяем существующий заказ
                existing_order = await self.db.get_order(row_index)
                
                if existing_order:
                    existing_hash = existing_order[1]
                    
                    if existing_hash != hash_value:
                        # Заказ изменился
                        logger.info(f"Обнаружено изменение в строке {row_index}")
                        await self.db.upsert_order(row_index, hash_value, normalized_line)
                        await self.db.add_pending_notification(row_index, hash_value, normalized_line)
                else:
                    # Новый заказ
                    logger.info(f"Обнаружен новый заказ в строке {row_index}")
                    await self.db.upsert_order(row_index, hash_value, normalized_line)
                    await self.db.add_pending_notification(row_index, hash_value, normalized_line)
            
            # Отправляем готовые уведомления
            await self.send_pending_notifications()
            
        except Exception as e:
            logger.error(f"Ошибка при проверке обновлений: {e}")
    
    async def send_pending_notifications(self):
        """Отправка готовых уведомлений"""
        try:
            ready_notifications = await self.db.get_ready_notifications(Config.NOTIFY_DELAY_SEC)
            
            if not ready_notifications:
                return
            
            subscribers = await self.db.get_subscribers()
            
            if not subscribers:
                logger.debug("Нет подписчиков для отправки уведомлений")
                return
            
            logger.info(f"Отправка {len(ready_notifications)} уведомлений для {len(subscribers)} подписчиков")
            
            for row_index, hash_value, line in ready_notifications:
                # Определяем, новое это или обновление
                existing_order = await self.db.get_order(row_index)
                is_update = existing_order is not None and existing_order[1] != hash_value
                
                emoji = "🔄" if is_update else "🆕"
                action = "обновлен" if is_update else "добавлен"
                
                message_text = (
                    f"{emoji} <b>Заказ {action}</b>\n\n"
                    f"📋 <b>Строка:</b> {row_index}\n"
                    f"📝 <b>Содержимое:</b>\n<code>{line}</code>\n\n"
                    f"🕐 <i>{datetime.now().strftime('%H:%M %d.%m.%Y')}</i>"
                )
                
                # Отправляем всем подписчикам
                for chat_id in subscribers:
                    try:
                        await self.bot.send_message(
                            chat_id,
                            message_text,
                            parse_mode=ParseMode.HTML,
                            disable_web_page_preview=True
                        )
                        logger.debug(f"Уведомление отправлено chat_id={chat_id}, строка={row_index}")
                    except Exception as e:
                        logger.error(f"Ошибка отправки уведомления chat_id={chat_id}: {e}")
                        
                        # Если пользователь заблокировал бота, удаляем его из подписчиков
                        if "bot was blocked" in str(e).lower() or "chat not found" in str(e).lower():
                            await self.db.remove_subscriber(chat_id)
                            logger.info(f"Удален недоступный подписчик: {chat_id}")
            
        except Exception as e:
            logger.error(f"Ошибка при отправке уведомлений: {e}")
    
    async def run(self):
        """Основной цикл работы бота"""
        try:
            # Тихая инициализация
            await self.silent_initialization()
            
            logger.info("Бот запущен. Ожидание сообщений...")
            await self.dp.start_polling(self.bot)
            
        except Exception as e:
            logger.error(f"Критическая ошибка в работе бота: {e}")
            raise
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Корректное завершение работы"""
        logger.info("Завершение работы бота...")
        
        if self.scheduler:
            self.scheduler.shutdown()
        
        if self.shortener:
            await self.shortener.close()
        
        if self.bot:
            await self.bot.session.close()
        
        logger.info("Бот завершил работу")

# Точка входа
async def main():
    """Основная функция"""
    bot = None
    try:
        bot = OrderNotificationBot()
        await bot.initialize()
        await bot.run()
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.critical(f"Необработанная ошибка: {e}")
    finally:
        if bot:
            await bot.shutdown()

if __name__ == "__main__":
    # Для bot-host.ru нужно использовать asyncio.run()
    asyncio.run(main())