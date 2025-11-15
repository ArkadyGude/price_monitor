import os
from dotenv import load_dotenv

# Список клиентов для обработки
clients_list = ["idel-max", "adamov"]

# Настройки Telegram
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = "628079333"

# Настройки логирования
LOG_LEVEL = "INFO"
LOG_FILE = "price_monitor.log"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Настройки API
REQUEST_TIMEOUT = 30
LIMIT = 1000
