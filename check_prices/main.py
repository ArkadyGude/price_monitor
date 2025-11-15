import os
from typing import List, Dict, Tuple, Any, Optional
import logging
from logging.handlers import RotatingFileHandler
import sys
from pathlib import Path
import time
import asyncio
from dataclasses import dataclass
import random
import json
import pandas as pd
from decouple import AutoConfig
import aiohttp
import async_timeout

# Импорт настроек
from settings import clients_list, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from settings import LOG_LEVEL, LOG_FILE, LOG_FORMAT, LOG_DATE_FORMAT
from settings import REQUEST_TIMEOUT, LIMIT

# Константы API
PATH_CLIENTS = "Clients"
PATH_PRICES = "reference_prices"
GET_PRICES = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"
CHANGE_PRICES = "https://discounts-prices-api.wildberries.ru/api/v2/upload/task"

# Лимиты API
RATE_LIMIT_WINDOW = 6
RATE_LIMIT_MAX_REQUESTS = 10
RATE_LIMIT_INTERVAL = 0.6
RATE_LIMIT_BURST = 5

# Настройки повторных попыток
MAX_RETRIES = 5
BASE_DELAY = 1.0


class RobustClientSession:
    """Надежная обертка вокруг aiohttp.ClientSession с повторными попытками"""

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self._connector = None

    async def __aenter__(self):
        await self._create_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
        if self._connector:
            await self._connector.close()

    async def _create_session(self):
        """Создать новую сессию"""
        if self.session:
            await self.session.close()

        self._connector = aiohttp.TCPConnector(
            limit=50,
            limit_per_host=10,
            keepalive_timeout=30,
            enable_cleanup_closed=True,
            force_close=False,
            use_dns_cache=True,
        )

        # Используем REQUEST_TIMEOUT из настроек
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)

        self.session = aiohttp.ClientSession(
            connector=self._connector,
            timeout=timeout,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
            },
            json_serialize=json.dumps,
        )

    async def request_with_retry(
        self, method: str, url: str, max_retries: int = MAX_RETRIES, **kwargs
    ) -> Optional[dict]:
        """Выполнить запрос с повторными попытками и парсингом JSON"""
        last_exception = None

        for attempt in range(max_retries):
            try:
                # Создаем новую сессию при необходимости
                if not self.session or self.session.closed:
                    await self._create_session()

                # Добавляем задержку между попытками
                if attempt > 0:
                    delay = BASE_DELAY * (2**attempt) + random.uniform(0.5, 1.5)
                    logging.info(
                        "Повторная попытка %d/%d через %.1fс",
                        attempt,
                        max_retries,
                        delay,
                    )
                    await asyncio.sleep(delay)

                # Используем REQUEST_TIMEOUT для async_timeout
                async with async_timeout.timeout(REQUEST_TIMEOUT):
                    async with self.session.request(method, url, **kwargs) as response:

                        # Проверяем статус ответа
                        if response.status == 429:
                            wait_time = (2**attempt) + random.uniform(1.0, 3.0)
                            logging.warning("Лимит запросов. Ждем %.1fс", wait_time)
                            await asyncio.sleep(wait_time)
                            continue

                        if response.status >= 500:
                            logging.warning("Серверная ошибка %d", response.status)
                            continue

                        # Читаем и парсим ответ
                        text = await response.text()

                        if not text:
                            logging.warning("Пустой ответ от сервера")
                            continue

                        try:
                            return json.loads(text)
                        except json.JSONDecodeError as e:
                            logging.error(
                                "Ошибка парсинга JSON: %s, ответ: %s", e, text[:200]
                            )
                            # Для некорректного JSON пробуем еще раз
                            continue

            except asyncio.TimeoutError:
                last_exception = "Таймаут запроса"
                logging.warning(
                    "%s (попытка %d/%d)", last_exception, attempt + 1, max_retries
                )

            except aiohttp.ClientConnectionError:
                last_exception = "Ошибка соединения"
                logging.warning(
                    "%s (попытка %d/%d)", last_exception, attempt + 1, max_retries
                )
                # При ошибке соединения пересоздаем сессию
                try:
                    await self._create_session()
                except Exception:
                    pass

            except aiohttp.ClientError:
                last_exception = "Ошибка клиента"
                logging.warning(
                    "%s (попытка %d/%d)", last_exception, attempt + 1, max_retries
                )

            except Exception as e:
                last_exception = f"Неожиданная ошибка: {e}"
                logging.error(
                    "%s (попытка %d/%d)", last_exception, attempt + 1, max_retries
                )

        logging.error(
            "Не удалось выполнить запрос после %d попыток: %s",
            max_retries,
            last_exception,
        )
        return None


@dataclass
class RateLimiter:
    """Класс для управления лимитами запросов"""

    account_id: str
    requests: List[float] = None
    lock: asyncio.Lock = None

    def __post_init__(self):
        if self.requests is None:
            self.requests = []
        if self.lock is None:
            self.lock = asyncio.Lock()

    async def acquire(self):
        """Дождаться возможности сделать запрос"""
        async with self.lock:
            now = time.time()

            # Удаляем старые запросы
            self.requests = [
                req_time
                for req_time in self.requests
                if now - req_time < RATE_LIMIT_WINDOW
            ]

            # Проверяем лимит всплеска
            if len(self.requests) >= RATE_LIMIT_BURST:
                wait_time = RATE_LIMIT_INTERVAL + random.uniform(0.1, 0.3)
                await asyncio.sleep(wait_time)
                now = time.time()
                self.requests = [
                    req_time
                    for req_time in self.requests
                    if now - req_time < RATE_LIMIT_WINDOW
                ]

            # Проверяем общий лимит
            while len(self.requests) >= RATE_LIMIT_MAX_REQUESTS:
                oldest_request = min(self.requests)
                wait_time = RATE_LIMIT_WINDOW - (now - oldest_request) + 0.1
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                now = time.time()
                self.requests = [
                    req_time
                    for req_time in self.requests
                    if now - req_time < RATE_LIMIT_WINDOW
                ]

            self.requests.append(time.time())


class RateLimitManager:
    """Менеджер лимитов"""

    def __init__(self):
        self.limiters: Dict[str, RateLimiter] = {}
        self.lock = asyncio.Lock()

    async def get_limiter(self, account_id: str) -> RateLimiter:
        async with self.lock:
            if account_id not in self.limiters:
                self.limiters[account_id] = RateLimiter(account_id)
            return self.limiters[account_id]

    async def acquire(self, account_id: str):
        limiter = await self.get_limiter(account_id)
        await limiter.acquire()


class AsyncTelegramHandler(logging.Handler):
    """Обработчик для Telegram"""

    def __init__(self, bot_token: str, chat_id: str):
        super().__init__()
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        self._session: Optional[RobustClientSession] = None

    def set_session(self, session: RobustClientSession):
        """Установить сессию для асинхронных запросов"""
        self._session = session

    async def async_emit(self, record, session: RobustClientSession):
        """Асинхронная отправка сообщения в Telegram"""
        try:
            log_entry = self.format(record)
            if record.levelno >= logging.WARNING:
                payload = {
                    "chat_id": self.chat_id,
                    "text": log_entry,
                    "parse_mode": "HTML",
                }
                await session.request_with_retry("POST", self.url, json=payload)
        except Exception:
            # Игнорируем ошибки при отправке в Telegram
            pass

    def emit(self, record):
        """Синхронная реализация абстрактного метода"""
        # Для совместимости с синхронным кодом логирования
        # В асинхронном контексте лучше использовать async_emit
        if self._session:
            # Создаем задачу для асинхронной отправки
            asyncio.create_task(self.async_emit(record, self._session))


def setup_logging(
    session: Optional[RobustClientSession] = None,
) -> AsyncTelegramHandler:
    """Настройка логирования"""
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, LOG_LEVEL))

    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    telegram_handler = None
    if (
        TELEGRAM_BOT_TOKEN
        and TELEGRAM_BOT_TOKEN != "your_bot_token_here"
        and TELEGRAM_CHAT_ID
    ):
        telegram_handler = AsyncTelegramHandler(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
        telegram_handler.setLevel(logging.WARNING)
        telegram_handler.setFormatter(formatter)
        if session:
            telegram_handler.set_session(session)
        logger.addHandler(telegram_handler)

    return telegram_handler


def get_clients_folders() -> List[Path]:
    """Получить список папок клиентов"""
    p = Path(PATH_CLIENTS)
    folders = []

    for client_name in clients_list:
        client_folder = p / client_name
        if client_folder.exists() and client_folder.is_dir():
            folders.append(client_folder)
        else:
            logging.warning("Папка клиента %s не найдена", client_name)

    return folders


def get_api_key(path: str) -> str:
    """Получить API ключ"""
    config = AutoConfig(search_path=path)
    return config("api_key")


def get_headers(api_key: str) -> Dict[str, str]:
    """Сформировать заголовки"""
    return {"Authorization": api_key}


def get_reference_prices_files(path: str) -> List[Path]:
    """Получить список файлов с ценами"""
    p = Path(path)
    return list(p.glob("*"))


def read_prices_data(files: List[Path]) -> List[Dict[str, Any]]:
    """Прочитать данные о ценах"""
    if not files:
        return []

    file = files[0]
    try:
        data_from_file = pd.read_excel(file)
        required_columns = ["nmID", "sizeID", "price", "discount"]

        missing_columns = set(required_columns) - set(data_from_file.columns)
        if missing_columns:
            logging.warning("В файле %s отсутствуют колонки: %s", file, missing_columns)
            return []

        data_from_file = data_from_file[required_columns]
        return data_from_file.to_dict("records")
    except Exception as e:
        logging.error("Ошибка чтения файла %s: %s", file, e)
        return []


def find_price_changes(
    api_response: dict, items_data: List[Dict[str, Any]]
) -> List[Tuple[int, int]]:
    """Найти изменения в ценах"""
    changes = []

    if not api_response.get("data") or not api_response["data"].get("listGoods"):
        return changes

    items_res = api_response["data"]["listGoods"]

    reference_dict = {
        (int(item["nmID"]), int(item["sizeID"])): int(item["discount"])
        for item in items_data
    }

    for item_res in items_res:
        nm_id = int(item_res["nmID"])
        current_discount = int(item_res["discount"])

        for item_size in item_res.get("sizes", []):
            size_id = int(item_size["sizeID"])
            key = (nm_id, size_id)

            if key in reference_dict and reference_dict[key] != current_discount:
                changes.append((nm_id, size_id))

    return changes


async def _get_client_data(
    folder: Path, client_name: str
) -> Optional[Tuple[List[Dict[str, Any]], str]]:
    """Получить данные клиента (файлы и API ключ)"""
    references_prices_path = folder / PATH_PRICES

    if not references_prices_path.exists():
        logging.warning("Папка с ценами не найдена для клиента %s", client_name)
        return None

    files = get_reference_prices_files(str(references_prices_path))
    if not files:
        logging.warning("Файлы с ценами не найдены для клиента %s", client_name)
        return None

    items_data = read_prices_data(files)
    if not items_data:
        logging.warning(
            "Не удалось прочитать данные о ценах для клиента %s", client_name
        )
        return None

    api_key = get_api_key(str(folder))
    return items_data, api_key


async def _fetch_current_prices(
    client_name: str,
    session: RobustClientSession,
    rate_manager: RateLimitManager,
    headers: Dict[str, str],
) -> Optional[dict]:
    """Получить текущие цены через API"""
    await rate_manager.acquire(client_name)

    params = {"limit": LIMIT}
    response_data = await session.request_with_retry(
        "GET", GET_PRICES, headers=headers, params=params
    )

    if not response_data:
        logging.error("%s - Не удалось получить данные API", client_name)

    return response_data


async def _send_price_changes(
    client_name: str,
    session: RobustClientSession,
    rate_manager: RateLimitManager,
    headers: Dict[str, str],
    items_data: List[Dict[str, Any]],
) -> bool:
    """Отправить изменения цен"""
    await rate_manager.acquire(client_name)

    change_data = {"data": items_data}
    change_response = await session.request_with_retry(
        "POST", CHANGE_PRICES, headers=headers, json=change_data
    )

    if change_response:
        logging.info("%s - Цены успешно отправлены", client_name)
        return True
    else:
        logging.error("%s - Не удалось отправить цены", client_name)
        return False


async def _send_telegram_notification(
    client_name: str,
    message_type: str,  # "changed" или "restored"
    changes_count: int,
    telegram_handler: Optional[AsyncTelegramHandler],
    session: RobustClientSession,
) -> None:
    """Отправить уведомление в Telegram (только через явный вызов)"""
    if telegram_handler and hasattr(telegram_handler, "async_emit"):
        if message_type == "changed":
            msg_text = f"🔄 {client_name} - Цены изменились (попали в акцию)"
        else:  # restored
            msg_text = f"✅ {client_name} - Цены восстановлены в исходное состояние"

        await telegram_handler.async_emit(
            logging.LogRecord(
                name="price_monitor",
                level=logging.WARNING,
                pathname=__file__,
                lineno=0,
                msg="%s\nИзменений: %s шт.",
                args=(msg_text, changes_count),
                exc_info=None,
            ),
            session,
        )


async def process_client(
    folder: Path,
    session: RobustClientSession,
    rate_manager: RateLimitManager,
    telegram_handler: Optional[AsyncTelegramHandler] = None,
) -> None:
    """Обработать данные для одного клиента"""
    client_name = folder.name

    try:
        # Получение данных клиента
        client_data = await _get_client_data(folder, client_name)
        if not client_data:
            return

        items_data, api_key = client_data
        headers = get_headers(api_key)

        # Получение текущих цен
        response_data = await _fetch_current_prices(
            client_name, session, rate_manager, headers
        )
        if not response_data:
            return

        logging.info("%s - Данные получены успешно", client_name)

        # Поиск изменений
        changes = find_price_changes(response_data, items_data)

        if not changes:
            logging.info("%s - Все цены актуальны", client_name)
            return

        # Первое уведомление: товары попали в акцию
        logging.info(
            "%s - Обнаружены изменения цен: %s шт.", client_name, len(changes)
        )  # Изменено на INFO чтобы не дублировалось в Telegram

        # Первое сообщение в Telegram
        await _send_telegram_notification(
            client_name, "changed", len(changes), telegram_handler, session
        )

        # Отправка исправленных цен
        success = await _send_price_changes(
            client_name, session, rate_manager, headers, items_data
        )

        # Второе уведомление: цены успешно восстановлены
        if success:
            logging.info(
                "%s - Цены успешно восстановлены: %s шт.", client_name, len(changes)
            )  # Изменено на INFO

            # Второе сообщение в Telegram
            await _send_telegram_notification(
                client_name, "restored", len(changes), telegram_handler, session
            )
        else:
            logging.error(
                "%s - Не удалось восстановить цены. Ошибка при отправке", client_name
            )

    except Exception as e:
        logging.error("%s - Неожиданная ошибка: %s", client_name, e)


async def process_clients_concurrently():
    """Обработать всех клиентов"""
    rate_manager = RateLimitManager()

    async with RobustClientSession() as session:
        telegram_handler = setup_logging(session)

        logging.info("Запуск обработки клиентов...")
        logging.info("Клиенты: %s", clients_list)

        folders = get_clients_folders()
        if not folders:
            logging.error("Не найдены папки клиентов: %s", clients_list)
            return

        logging.info("Найдено клиентов: %s из %s", len(folders), len(clients_list))

        # Ограничиваем одновременные задачи
        semaphore = asyncio.Semaphore(5)

        async def bounded_process(folder):
            async with semaphore:
                return await process_client(
                    folder, session, rate_manager, telegram_handler
                )

        tasks = [bounded_process(folder) for folder in folders]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Логируем ошибки
        for result in results:
            if isinstance(result, Exception):
                logging.error("Ошибка в задаче: %s", result)

        logging.info("Обработка завершена")


async def main():
    """Основная функция"""
    await process_clients_concurrently()


def sync_main():
    """Синхронный запуск"""
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    asyncio.run(main())


if __name__ == "__main__":
    sync_main()
