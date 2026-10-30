"""Настройки Superset. Загружается автоматически из /app/pythonpath; секреты - из окружения (.env)."""
import os

from sqlalchemy.engine import URL

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Метаданные (дашборды, чарты, пользователи) - в отдельной базе существующего Postgres.
# URL.create экранирует спецсимволы в пароле, в отличие от сборки строки вручную.
SQLALCHEMY_DATABASE_URI = URL.create(
    "postgresql+psycopg2",
    username="superset",
    password=os.environ["SUPERSET_DB_PASSWORD"],
    host=os.getenv("POSTGRES_HOST", "postgres"),
    port=5432,
    database="superset",
).render_as_string(hide_password=False)

# Интерфейс на русском, английский остаётся в переключателе
BABEL_DEFAULT_LOCALE = "ru"
LANGUAGES = {
    "ru": {"flag": "ru", "name": "Русский"},
    "en": {"flag": "us", "name": "English"},
}

# Кэш без Redis: результаты запросов хранятся в метабазе. Для локального дашборда этого хватает,
# а Redis - лишний контейнер и лишняя память.
DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "SupersetMetastoreCache",
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_DEFAULT_TIMEOUT": 300,
}

# Стенд локальный, HTTPS нет: иначе Talisman перенаправляет на https и ломает вход
TALISMAN_ENABLED = False
