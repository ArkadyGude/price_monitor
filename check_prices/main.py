import os
from typing import List, Dict, Tuple, Any
import logging
from logging.handlers import RotatingFileHandler
import sys
from pathlib import Path
import requests
import pandas as pd
from decouple import AutoConfig

# Импорт настроек
from settings import clients_list, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
from settings import LOG_LEVEL, LOG_FILE, LOG_FORMAT, LOG_DATE_FORMAT
from settings import REQUEST_TIMEOUT, LIMIT

# Константы API
PATH_CLIENTS = "Clients"
PATH_PRICES = "reference_prices"
GET_PRICES = "https://discounts-prices-api.wildberries.ru/api/v2/list/goods/filter"
CHANGE_PRICES = "https://discounts-prices-api.wildberries.ru/api/v2/upload/task"


class TelegramHandler(logging.Handler):
    """Кастомный обработчик для отправки логов в Telegram"""

    def __init__(self, bot_token: str, chat_id: str):
        super().__init__()
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    def emit(self, record):
        try:
            log_entry = self.format(record)

            # Отправляем только важные сообщения в Telegram
            if record.levelno >= logging.WARNING:
                payload = {
                    "chat_id": self.chat_id,
                    "text": log_entry,
                    "parse_mode": "HTML",
                }
                requests.post(self.url, json=payload, timeout=10)
        except Exception:
            # Игнорируем ошибки при отправке в Telegram, чтобы не прерывать основной процесс
            pass


def setup_logging():
    """Настройка логирования с выводом в файл и Telegram"""
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, LOG_LEVEL))

    # Форматтер
    formatter = logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    # Обработчик для файла с ротацией
    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"  # 10 MB
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Обработчик для консоли
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Обработчик для Telegram (если настроены токен и chat_id)
    if (
        TELEGRAM_BOT_TOKEN
        and TELEGRAM_BOT_TOKEN != "your_bot_token_here"
        and TELEGRAM_CHAT_ID
    ):
        telegram_handler = TelegramHandler(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
        telegram_handler.setLevel(logging.WARNING)  # Только WARNING и выше
        telegram_handler.setFormatter(formatter)
        logger.addHandler(telegram_handler)

    return logger


def get_clients_folders() -> List[Path]:
    """Получить список папок клиентов из settings.py"""
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
    """Получить API ключ из конфигурации."""
    config = AutoConfig(search_path=path)
    return config("api_key")


def get_headers(api_key: str) -> Dict[str, str]:
    """Сформировать заголовки запроса."""
    return {"Authorization": api_key}


def get_reference_prices_files(path: str) -> List[Path]:
    """Получить список файлов с референсными ценами."""
    p = Path(path)
    return list(p.glob("*"))


def read_prices_data(files: List[Path]) -> List[Dict[str, Any]]:
    """Прочитать данные о ценах из файлов."""
    if not files:
        return []

    # Берем первый файл (оригинальная логика)
    file = files[0]
    try:
        data_from_file = pd.read_excel(file)
        required_columns = ["nmID", "sizeID", "price", "discount"]

        # Проверяем наличие необходимых колонок
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
    """Найти изменения в ценах между API и референсными данными."""
    changes = []

    if not api_response.get("data") or not api_response["data"].get("listGoods"):
        return changes

    items_res = api_response["data"]["listGoods"]

    # Создаем словарь для быстрого поиска по nmID и sizeID
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


def process_client(folder: Path) -> None:
    """Обработать данные для одного клиента."""
    client_name = folder.name
    references_prices_path = folder / PATH_PRICES

    if not references_prices_path.exists():
        logging.warning("Папка с ценами не найдена для клиента %s", client_name)
        return

    try:
        # Получение данных
        files = get_reference_prices_files(str(references_prices_path))
        if not files:
            logging.warning("Файлы с ценами не найдены для клиента %s", client_name)
            return

        items_data = read_prices_data(files)
        if not items_data:
            logging.warning(
                "Не удалось прочитать данные о ценах для клиента %s", client_name
            )
            return

        # Получение API ключа и заголовков
        api_key = get_api_key(str(folder))
        headers = get_headers(api_key)

        # Запрос текущих цен
        params = {"limit": LIMIT}
        response = requests.get(
            GET_PRICES, headers=headers, params=params, timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()

        logging.info("%s - Статус запроса: %s", client_name, response.status_code)

        # Поиск изменений
        changes = find_price_changes(response.json(), items_data)

        if not changes:
            logging.info("%s - Все цены актуальны", client_name)
            return

        # Обработка изменений
        logging.warning(
            "%s - Товары попали в акцию. Количество изменений: %s шт. Артикулы: %s",
            client_name,
            len(changes),
            changes,
        )

        # Отправка исправленных цен
        change_data = {"data": items_data}
        change_response = requests.post(
            CHANGE_PRICES, headers=headers, json=change_data, timeout=REQUEST_TIMEOUT
        )
        change_response.raise_for_status()

        logging.info(
            "%s - Цены исправлены. Статус: %s", client_name, change_response.status_code
        )

    except requests.exceptions.RequestException as e:
        logging.error("%s - Ошибка сетевого запроса: %s", client_name, e)
    except Exception as e:
        logging.error("%s - Неожиданная ошибка: %s", client_name, e)


def main():
    """Основная функция."""
    # Настройка логирования
    setup_logging()

    logging.info("Запуск обработки клиентов...")
    logging.info("Клиенты для обработки: %s", clients_list)

    folders = get_clients_folders()
    if not folders:
        logging.error("Не найдены папки клиентов из списка: %s", clients_list)
        return

    logging.info(
        "Найдено клиентов для обработки: %s из %s", len(folders), len(clients_list)
    )

    for folder in folders:
        process_client(folder)

    logging.info("Обработка клиентов завершена")


if __name__ == "__main__":
    main()
