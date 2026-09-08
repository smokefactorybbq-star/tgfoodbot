import os
import sys
import csv
import io
import json
import time
import hmac
import html
import base64
import hashlib
import logging
import asyncio
import threading
import re

from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlencode, urlsplit, urlunsplit, parse_qsl
from zoneinfo import ZoneInfo

import aiohttp
import asyncpg

from aiogram import Bot, Dispatcher, types, F
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiogram.enums import ContentType
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
)
from aiogram.filters import Command
from aiogram.types import BufferedInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder


# ============================================================================
# ЛОГИРОВАНИЕ
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


# ============================================================================
# НАСТРОЙКИ — ТВОИ ДАННЫЕ НЕ ИЗМЕНЕНЫ
# ============================================================================

try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass


API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

if not API_TOKEN:
    logger.critical("ERROR: TELEGRAM_BOT_TOKEN не установлен")
    sys.exit(1)


DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    logger.critical("ERROR: DATABASE_URL не установлен")
    sys.exit(1)


ADMIN_CHAT_ID = int(
    os.getenv(
        "ADMIN_CHAT_ID",
        "7309681026",
    )
)

RESTART_MINUTES = int(
    os.getenv(
        "RESTART_MINUTES",
        "420",
    )
)

PORT = int(
    os.getenv(
        "PORT",
        "8080",
    )
)


MANAGER_URL = os.getenv(
    "MANAGER_URL",
    "https://t.me/SmokefactoryBBQ",
)


WEBAPP_URL = os.getenv(
    "WEBAPP_URL",
    "https://mini-app-production-67f2.up.railway.app",
).rstrip("/")


MENU_BTN_TEXT = "📋 Открыть меню"

ASK_BTN_TEXT = "💬 Задать вопрос менеджеру"


LOYALTY_SETTLE_URL = f"{WEBAPP_URL}/api/loyalty/settle"

PRINT_URL = os.getenv(
    "PRINT_URL",
    "https://pseudosocially-tiddly-alysia.ngrok-free.dev/order",
)


# New integrations are optional and do not replace the old Mini App / polling logic.
WEBSITE_ORDER_SECRET = os.getenv("WEBSITE_ORDER_SECRET", "").strip()

DEFAULT_SCREEN_SERVICE_URL = "https://screegrab-production.up.railway.app"
SCREEN_SERVICE_URL = os.getenv(
    "SCREEN_SERVICE_URL",
    DEFAULT_SCREEN_SERVICE_URL,
).rstrip("/")
SCREEN_SERVICE_SECRET = os.getenv("SCREEN_SERVICE_SECRET", "").strip()

GOOGLE_MAPS_API_KEY = (
    os.getenv("GOOGLE_ROUTES_API_KEY")
    or os.getenv("GOOGLE_MAPS_API_KEY")
    or os.getenv("NEXT_PUBLIC_GOOGLE_MAPS_API_KEY")
    or ""
).strip()

CAFE_LATITUDE = float(os.getenv("CAFE_LATITUDE", "7.910335"))
CAFE_LONGITUDE = float(os.getenv("CAFE_LONGITUDE", "98.368771"))

PREP_BUFFER_MINUTES = 10
DELIVERY_BUFFER_MINUTES = 10


# ============================================================================
# ИНИЦИАЛИЗАЦИЯ
# ============================================================================

bot = Bot(
    token=API_TOKEN
)

dp = Dispatcher()


KEYBOARD_SHOWN_USERS: set[int] = set()


# Менеджер пишет клиенту.
waiting_reply: dict[
    int,
    dict[str, int],
] = {}


# Менеджер готовит рекламную рассылку.
waiting_broadcast: set[int] = set()


# Подготовленное сообщение для рассылки.
pending_broadcasts: dict[
    int,
    dict,
] = {}


# Состояние команды /bonus.
waiting_bonus: dict[
    int,
    dict,
] = {}


broadcast_lock = asyncio.Lock()

broadcast_running = False

BROADCAST_DELAY = 0.06


db_pool: asyncpg.Pool | None = None

# Event loop is saved for the small HTTP endpoint used by smokefactorybbq.com.
MAIN_LOOP: asyncio.AbstractEventLoop | None = None

# PromptPay receipt reminders are deliberately kept in memory. A redeploy may
# cancel a reminder, but it never cancels or loses the order itself.
pending_receipt_tasks: dict[str, asyncio.Task] = {}
pending_promptpay_by_user: dict[int, str] = {}
receipt_received_orders: set[str] = set()


TIMEZONE = ZoneInfo(
    "Asia/Bangkok"
)

MENU_PRICE_MAP: dict[str, int] = {
    "Борщ": 180,
    "Солянка": 180,
    "Гороховый суп": 180,
    "Грибной суп": 180,
    "Окрошка": 180,
    "Куриный суп": 150,
    "Котлеты куриные": 150,
    "Вареники с картошкой и беконом": 170,
    "Пельмени": 190,
    "Котлеты из домашнего фарша": 180,
    "Перец фаршированный": 200,
    "Бефстроганов": 220,
    "Лепешка с сыром": 100,
    "Лепешка с картошкой": 100,
    "Лепешка с рваной свининой": 150,
    "Котлета по-киевски": 230,
    "Зраза": 200,
    "Драники": 200,
    "Ленивые голубцы Том ям": 250,
    "Картошка фри": 120,
    "Картошка дольками": 120,
    "Мини чебуреки": 170,
    "Салат Цезарь с копченой курицей": 180,
    "Овощной салат": 120,
    "Салат Обжорка": 180,
    "Салат Крабовый": 170,
    "Салат баклажаны в кляре": 160,
    "Салат Деревенский": 180,
    "Салат Столичный": 160,
    "Ребра BBQ": 350,
    "Кебаб свинина-говядина": 250,
    "Кебаб из курицы": 200,
    "Шашлык из курицы": 200,
    "Шашлык из курицы 2.0": 250,
    "Шашлык из куриного крыла": 240,
    "Шашлык из свинины": 250,
    "Ребро варено-копченое": 300,
    "Лепешка с мясом (Standart)": 100,
    "Лепешка с мясом (XXL)": 180,
    "Лепешка с сыром (Standart)": 100,
    "Лепешка с сыром (XXL)": 200,
    "Лепешка с картошкой (Standart)": 100,
    "Лепешка с картошкой (XXL)": 200,
    "Лепешка с рваной свининой (Standart)": 140,
    "Лепешка с рваной свининой (XXL)": 250
}
MAX_BONUS_REDEEM_PERCENT = 20


# ============================================================================
# БАЗА ДАННЫХ
# ============================================================================

async def init_database() -> None:
    global db_pool

    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=5,
        command_timeout=30,
    )

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                telegram_id BIGINT PRIMARY KEY,
                username TEXT,
                telegram_first_name TEXT,
                telegram_last_name TEXT,
                profile_name TEXT,
                phone TEXT,
                address TEXT,
                photo_url TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_bot_activity_at TIMESTAMPTZ,
                last_site_visit_at TIMESTAMPTZ
            );


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS username TEXT;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS telegram_first_name TEXT;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS telegram_last_name TEXT;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS profile_name TEXT;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS phone TEXT;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS address TEXT;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS photo_url TEXT;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS created_at
                TIMESTAMPTZ NOT NULL DEFAULT NOW();


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS updated_at
                TIMESTAMPTZ NOT NULL DEFAULT NOW();


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS last_bot_activity_at
                TIMESTAMPTZ;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS last_site_visit_at
                TIMESTAMPTZ;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS is_active
                BOOLEAN NOT NULL DEFAULT TRUE;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS marketing_allowed
                BOOLEAN NOT NULL DEFAULT TRUE;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS blocked_at
                TIMESTAMPTZ;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS last_send_error
                TEXT;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS last_successful_send_at
                TIMESTAMPTZ;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS last_broadcast_at
                TIMESTAMPTZ;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS last_keyboard_sent_at
                TIMESTAMPTZ;


            /*
             * Историческая сумма покупок,
             * заданная менеджером через /bonus.
             */
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS manual_spend
                BIGINT NOT NULL DEFAULT 0;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS bonus_updated_at
                TIMESTAMPTZ;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS bonus_updated_by
                BIGINT;


            CREATE INDEX IF NOT EXISTS idx_users_active
            ON users(is_active);


            CREATE INDEX IF NOT EXISTS idx_users_marketing
            ON users(marketing_allowed);


            CREATE INDEX IF NOT EXISTS idx_users_last_activity
            ON users(last_bot_activity_at);


            CREATE TABLE IF NOT EXISTS visits (
                id BIGSERIAL PRIMARY KEY,

                telegram_id BIGINT
                    REFERENCES users(telegram_id)
                    ON DELETE SET NULL,

                visited_at TIMESTAMPTZ
                    NOT NULL DEFAULT NOW(),

                session_key TEXT,
                user_agent TEXT
            );


            CREATE INDEX IF NOT EXISTS idx_visits_visited_at
            ON visits(visited_at);


            CREATE INDEX IF NOT EXISTS idx_visits_telegram_id
            ON visits(telegram_id);


            CREATE TABLE IF NOT EXISTS orders (
                id BIGSERIAL PRIMARY KEY,

                telegram_id BIGINT NOT NULL
                    REFERENCES users(telegram_id)
                    ON DELETE RESTRICT,

                source TEXT
                    NOT NULL DEFAULT 'mini_app',

                customer_name TEXT,
                phone TEXT,
                address TEXT,
                address_plain TEXT,
                payment_method TEXT,

                delivery_fee INTEGER
                    NOT NULL DEFAULT 0,

                items_total INTEGER
                    NOT NULL DEFAULT 0,

                discount_percent INTEGER
                    NOT NULL DEFAULT 0,

                discount_amount INTEGER
                    NOT NULL DEFAULT 0,

                total INTEGER
                    NOT NULL DEFAULT 0,

                order_when TEXT,
                order_date DATE,
                order_time TEXT,
                comment TEXT,

                status TEXT
                    NOT NULL DEFAULT 'created',

                created_at TIMESTAMPTZ
                    NOT NULL DEFAULT NOW()
            );


            /*
             * Номер заказа назначает только бот.
             * Первый новый номер: SM-472.
             */
            ALTER TABLE orders
            ADD COLUMN IF NOT EXISTS order_number TEXT;

            ALTER TABLE orders
            ADD COLUMN IF NOT EXISTS bonus_used INTEGER NOT NULL DEFAULT 0;

            ALTER TABLE orders
            ADD COLUMN IF NOT EXISTS cashback_percent INTEGER NOT NULL DEFAULT 0;

            ALTER TABLE orders
            ADD COLUMN IF NOT EXISTS cashback_earned INTEGER NOT NULL DEFAULT 0;

            ALTER TABLE orders
            ADD COLUMN IF NOT EXISTS loyalty_request_id TEXT;

            ALTER TABLE orders
            ADD COLUMN IF NOT EXISTS fulfillment_type TEXT NOT NULL DEFAULT 'DELIVERY';

            ALTER TABLE orders
            ADD COLUMN IF NOT EXISTS customer_latitude DOUBLE PRECISION;

            ALTER TABLE orders
            ADD COLUMN IF NOT EXISTS customer_longitude DOUBLE PRECISION;

            ALTER TABLE orders
            ADD COLUMN IF NOT EXISTS receipt_received_at TIMESTAMPTZ;

            ALTER TABLE orders
            ADD COLUMN IF NOT EXISTS tracking_url TEXT;


            CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_order_number
            ON orders(order_number)
            WHERE order_number IS NOT NULL;


            /*
             * Последовательность хранится в PostgreSQL,
             * поэтому не сбрасывается при Redeploy Railway.
             *
             * Если последовательность создаётся впервые,
             * учитываем уже записанные номера SM-*.
             */
            DO $order_number_sequence$
            DECLARE
                max_existing_number BIGINT;
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_class
                    WHERE relkind = 'S'
                      AND relname = 'sm_order_number_seq'
                )
                THEN
                    CREATE SEQUENCE sm_order_number_seq
                    START WITH 472
                    INCREMENT BY 1
                    MINVALUE 1
                    NO MAXVALUE
                    CACHE 1;

                    SELECT COALESCE(
                        MAX(
                            CASE
                                WHEN order_number ~ '^SM-[0-9]+$'
                                THEN SUBSTRING(order_number FROM 4)::BIGINT
                                ELSE NULL
                            END
                        ),
                        471
                    )
                    INTO max_existing_number
                    FROM orders;

                    PERFORM setval(
                        'sm_order_number_seq',
                        GREATEST(max_existing_number, 471),
                        TRUE
                    );
                END IF;
            END
            $order_number_sequence$;



            CREATE INDEX IF NOT EXISTS idx_orders_created_at
            ON orders(created_at);


            CREATE INDEX IF NOT EXISTS idx_orders_telegram_id
            ON orders(telegram_id);


            CREATE TABLE IF NOT EXISTS order_items (
                id BIGSERIAL PRIMARY KEY,

                order_id BIGINT NOT NULL
                    REFERENCES orders(id)
                    ON DELETE CASCADE,

                item_name TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                unit_price INTEGER NOT NULL,
                image_url TEXT
            );


            CREATE INDEX IF NOT EXISTS idx_order_items_order_id
            ON order_items(order_id);


            /*
             * История массовых рассылок.
             */
            CREATE TABLE IF NOT EXISTS broadcast_logs (
                id BIGSERIAL PRIMARY KEY,
                broadcast_type TEXT,
                created_by BIGINT,
                source_chat_id BIGINT,
                source_message_id BIGINT,
                total_targets INTEGER NOT NULL DEFAULT 0,
                delivered INTEGER NOT NULL DEFAULT 0,
                blocked INTEGER NOT NULL DEFAULT 0,
                failed INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'created',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                completed_at TIMESTAMPTZ
            );


            ALTER TABLE broadcast_logs
            ADD COLUMN IF NOT EXISTS broadcast_type TEXT;


            ALTER TABLE broadcast_logs
            ADD COLUMN IF NOT EXISTS created_by BIGINT;


            ALTER TABLE broadcast_logs
            ADD COLUMN IF NOT EXISTS source_chat_id BIGINT;


            ALTER TABLE broadcast_logs
            ADD COLUMN IF NOT EXISTS source_message_id BIGINT;


            ALTER TABLE broadcast_logs
            ADD COLUMN IF NOT EXISTS total_targets
                INTEGER NOT NULL DEFAULT 0;


            ALTER TABLE broadcast_logs
            ADD COLUMN IF NOT EXISTS delivered
                INTEGER NOT NULL DEFAULT 0;


            ALTER TABLE broadcast_logs
            ADD COLUMN IF NOT EXISTS blocked
                INTEGER NOT NULL DEFAULT 0;


            ALTER TABLE broadcast_logs
            ADD COLUMN IF NOT EXISTS failed
                INTEGER NOT NULL DEFAULT 0;


            ALTER TABLE broadcast_logs
            ADD COLUMN IF NOT EXISTS status
                TEXT NOT NULL DEFAULT 'created';


            ALTER TABLE broadcast_logs
            ADD COLUMN IF NOT EXISTS created_at
                TIMESTAMPTZ NOT NULL DEFAULT NOW();


            ALTER TABLE broadcast_logs
            ADD COLUMN IF NOT EXISTS completed_at
                TIMESTAMPTZ;


            /*
             * Исправление старой таблицы,
             * где колонка называлась kind.
             */
            DO $broadcast_migration$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = 'broadcast_logs'
                      AND column_name = 'kind'
                )
                THEN
                    EXECUTE '
                        UPDATE broadcast_logs
                        SET broadcast_type =
                            COALESCE(broadcast_type, kind)
                    ';

                    EXECUTE '
                        ALTER TABLE broadcast_logs
                        ALTER COLUMN kind DROP NOT NULL
                    ';
                END IF;
            END
            $broadcast_migration$;


            UPDATE broadcast_logs
            SET broadcast_type = 'unknown'
            WHERE broadcast_type IS NULL;


            ALTER TABLE broadcast_logs
            ALTER COLUMN broadcast_type SET DEFAULT 'unknown';


            ALTER TABLE broadcast_logs
            ALTER COLUMN broadcast_type SET NOT NULL;


            CREATE INDEX IF NOT EXISTS idx_broadcast_logs_created_at
            ON broadcast_logs(created_at DESC);


            /*
             * История ручных начислений лояльности.
             */
            CREATE TABLE IF NOT EXISTS loyalty_adjustments (
                id BIGSERIAL PRIMARY KEY,
                request_id TEXT UNIQUE,
                telegram_id BIGINT NOT NULL,
                previous_amount BIGINT NOT NULL DEFAULT 0,
                new_amount BIGINT NOT NULL DEFAULT 0,
                created_by BIGINT,
                source TEXT NOT NULL DEFAULT 'manager_bonus',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );


            CREATE INDEX IF NOT EXISTS idx_loyalty_adjustments_user
            ON loyalty_adjustments(telegram_id);


            CREATE INDEX IF NOT EXISTS idx_loyalty_adjustments_created
            ON loyalty_adjustments(created_at DESC);
            """
        )

        await conn.execute(
            """
            UPDATE broadcast_logs
            SET
                status = 'interrupted',
                completed_at = NOW()
            WHERE status = 'running'
            """
        )

    logger.info(
        "База данных подключена, таблицы готовы"
    )


async def upsert_user(
    user: types.User | None,
) -> asyncpg.Record | None:
    if not db_pool or not user:
        return None

    if user.is_bot:
        return None

    try:
        row = await db_pool.fetchrow(
            """
            INSERT INTO users (
                telegram_id,
                username,
                telegram_first_name,
                telegram_last_name,
                created_at,
                updated_at,
                last_bot_activity_at,
                is_active,
                blocked_at,
                last_send_error
            )
            VALUES (
                $1,
                $2,
                $3,
                $4,
                NOW(),
                NOW(),
                NOW(),
                TRUE,
                NULL,
                NULL
            )

            ON CONFLICT (telegram_id)
            DO UPDATE SET
                username = EXCLUDED.username,
                telegram_first_name = EXCLUDED.telegram_first_name,
                telegram_last_name = EXCLUDED.telegram_last_name,
                updated_at = NOW(),
                last_bot_activity_at = NOW(),
                is_active = TRUE,
                blocked_at = NULL,
                last_send_error = NULL

            RETURNING
                telegram_id,
                username,
                telegram_first_name,
                telegram_last_name,
                created_at,
                last_bot_activity_at,
                manual_spend
            """,
            user.id,
            user.username,
            user.first_name,
            user.last_name,
        )

        logger.info(
            "USER SAVED: id=%s username=@%s name=%s %s",
            row["telegram_id"],
            row["username"] or "-",
            row["telegram_first_name"] or "",
            row["telegram_last_name"] or "",
        )

        return row

    except Exception:
        logger.exception(
            "USER SAVE ERROR: id=%s",
            getattr(
                user,
                "id",
                "unknown",
            ),
        )

        return None


async def set_marketing_allowed(
    telegram_id: int,
    allowed: bool,
) -> None:
    if not db_pool:
        return

    await db_pool.execute(
        """
        UPDATE users
        SET
            marketing_allowed = $2,
            updated_at = NOW(),
            is_active = TRUE,
            blocked_at = NULL
        WHERE telegram_id = $1
        """,
        telegram_id,
        allowed,
    )


async def mark_send_success(
    telegram_id: int,
    send_type: str,
) -> None:
    if not db_pool:
        return

    if send_type == "broadcast":
        await db_pool.execute(
            """
            UPDATE users
            SET
                is_active = TRUE,
                blocked_at = NULL,
                last_send_error = NULL,
                last_successful_send_at = NOW(),
                last_broadcast_at = NOW()
            WHERE telegram_id = $1
            """,
            telegram_id,
        )

    elif send_type == "keyboard":
        await db_pool.execute(
            """
            UPDATE users
            SET
                is_active = TRUE,
                blocked_at = NULL,
                last_send_error = NULL,
                last_successful_send_at = NOW(),
                last_keyboard_sent_at = NOW()
            WHERE telegram_id = $1
            """,
            telegram_id,
        )

    else:
        await db_pool.execute(
            """
            UPDATE users
            SET
                is_active = TRUE,
                blocked_at = NULL,
                last_send_error = NULL,
                last_successful_send_at = NOW()
            WHERE telegram_id = $1
            """,
            telegram_id,
        )


async def mark_send_error(
    telegram_id: int,
    error_text: str,
    deactivate: bool,
) -> None:
    if not db_pool:
        return

    await db_pool.execute(
        """
        UPDATE users
        SET
            is_active = CASE
                WHEN $3 THEN FALSE
                ELSE is_active
            END,

            blocked_at = CASE
                WHEN $3 THEN NOW()
                ELSE blocked_at
            END,

            last_send_error = LEFT($2, 1000),
            updated_at = NOW()

        WHERE telegram_id = $1
        """,
        telegram_id,
        error_text,
        deactivate,
    )


# ============================================================================
# АВТОМАТИЧЕСКОЕ СОХРАНЕНИЕ ПОЛЬЗОВАТЕЛЯ
# ============================================================================

class UserTrackingMiddleware(
    BaseMiddleware
):
    async def __call__(
        self,
        handler,
        event,
        data,
    ):
        user = data.get(
            "event_from_user"
        )

        if user:
            await upsert_user(
                user
            )

        return await handler(
            event,
            data,
        )


dp.message.outer_middleware(
    UserTrackingMiddleware()
)

dp.callback_query.outer_middleware(
    UserTrackingMiddleware()
)


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

def safe_int(
    value,
    default: int = 0,
) -> int:
    try:
        return int(value)

    except Exception:
        return default


def safe_str(
    value,
    default: str = "",
) -> str:
    try:
        if value is None:
            return default

        return str(value)

    except Exception:
        return default


def is_admin(
    telegram_id: int,
) -> bool:
    return (
        telegram_id
        == ADMIN_CHAT_ID
    )


def is_blocking_error(
    error: Exception,
) -> bool:
    if isinstance(
        error,
        TelegramForbiddenError,
    ):
        return True

    error_text = str(
        error
    ).lower()

    blocking_phrases = (
        "bot was blocked by the user",
        "bot was blocked",
        "chat not found",
        "user is deactivated",
        "bot was kicked",
        "forbidden",
    )

    return any(
        phrase in error_text
        for phrase in blocking_phrases
    )


def parse_money_amount(
    value: str,
) -> int | None:
    cleaned = (
        str(value or "")
        .replace("฿", "")
        .replace("₽", "")
        .replace(" ", "")
        .replace(",", "")
        .strip()
    )

    if not cleaned.isdigit():
        return None

    amount = int(
        cleaned
    )

    if (
        amount < 0
        or amount > 10_000_000
    ):
        return None

    return amount


def discount_by_spend(
    total_spend: int,
) -> int:
    if total_spend >= 20_000:
        return 20

    if total_spend >= 15_000:
        return 15

    if total_spend >= 10_000:
        return 10

    if total_spend >= 5_000:
        return 5

    return 0


# ============================================================================
# НОВЫЙ АВТОМАТИЧЕСКИЙ ПОТОК ЗАКАЗА
# Добавлен поверх старой логики: старые handlers, polling, бонусы и Mini App
# не заменяются.
# ============================================================================

def normalize_payment_name(value: object) -> str:
    raw = safe_str(value, "").strip().lower().replace(" ", "").replace("-", "")
    if raw in {"promptpay", "promtpay", "thaibank", "thai", "true", "truemoney"}:
        return "PromptPay"
    if raw in {"cash", "наличные", "кэш"}:
        return "Cash"
    return safe_str(value, "PromptPay") or "PromptPay"


def normalize_fulfillment(value: object) -> str:
    return "PICKUP" if safe_str(value, "").upper() == "PICKUP" else "DELIVERY"


def dish_prep_minutes(name: str) -> int:
    n = safe_str(name, "").lower().replace("ё", "е")

    if any(x in n for x in (
        "борщ", "солянка", "гороховый суп", "грибной суп", "окрошка", "куриный суп",
    )):
        return 10

    if any(x in n for x in (
        "салат", "цезарь", "обжор", "столич", "деревен", "баклаж",
    )):
        return 15

    if "пельмен" in n or "вареник" in n:
        return 20

    if "лепеш" in n:
        return 16

    if "киев" in n:
        return 22

    if "чебур" in n:
        return 20

    if "фри" in n or "дольк" in n:
        return 16

    if "зраз" in n or "драник" in n:
        return 24

    if "ребр" in n:
        return 12

    # По согласованным временам: курица — 28, остальные шашлыки/кебабы — 30.
    if "шашлык из курицы" in n or "кебаб из курицы" in n:
        return 28

    if "шашлык" in n or "кебаб" in n or "крыл" in n:
        return 30

    if any(x in n for x in ("котлет", "перец", "беф", "голуб")):
        return 13

    return 13


def calculate_prep_minutes(order_items: list[dict]) -> int:
    longest = 5
    for item in order_items or []:
        longest = max(longest, dish_prep_minutes(safe_str(item.get("name"), "")))
    return longest + PREP_BUFFER_MINUTES


def payload_coordinates(data: dict) -> tuple[float | None, float | None]:
    location = data.get("location") if isinstance(data.get("location"), dict) else {}
    raw_lat = data.get("latitude", data.get("customer_latitude", location.get("lat")))
    raw_lng = data.get("longitude", data.get("customer_longitude", location.get("lng")))
    try:
        lat = float(raw_lat)
        lng = float(raw_lng)
        if -90 <= lat <= 90 and -180 <= lng <= 180:
            return lat, lng
    except (TypeError, ValueError):
        pass
    return None, None


async def geocode_delivery_address(address: str) -> tuple[float | None, float | None]:
    if not GOOGLE_MAPS_API_KEY or not address.strip():
        return None, None

    try:
        timeout = aiohttp.ClientTimeout(total=6)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(
                "https://maps.googleapis.com/maps/api/geocode/json",
                params={"address": address, "key": GOOGLE_MAPS_API_KEY},
            ) as response:
                if response.status != 200:
                    return None, None
                body = await response.json(content_type=None)
                results = body.get("results") or []
                if not results:
                    return None, None
                location = results[0].get("geometry", {}).get("location", {})
                lat = float(location.get("lat"))
                lng = float(location.get("lng"))
                return lat, lng
    except Exception as exc:
        logger.warning("Google geocoding failed: %s", exc)
        return None, None


async def google_two_wheeler_minutes(
    destination_lat: float,
    destination_lng: float,
) -> int | None:
    if not GOOGLE_MAPS_API_KEY:
        return None

    payload = {
        "origin": {
            "location": {
                "latLng": {
                    "latitude": CAFE_LATITUDE,
                    "longitude": CAFE_LONGITUDE,
                }
            }
        },
        "destination": {
            "location": {
                "latLng": {
                    "latitude": destination_lat,
                    "longitude": destination_lng,
                }
            }
        },
        "travelMode": "TWO_WHEELER",
        "routingPreference": "TRAFFIC_AWARE",
        "computeAlternativeRoutes": False,
        "languageCode": "ru",
        "units": "METRIC",
    }

    try:
        timeout = aiohttp.ClientTimeout(total=7)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                "https://routes.googleapis.com/directions/v2:computeRoutes",
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "X-Goog-Api-Key": GOOGLE_MAPS_API_KEY,
                    "X-Goog-FieldMask": "routes.duration",
                },
            ) as response:
                if response.status != 200:
                    logger.warning("Google Routes HTTP %s: %s", response.status, (await response.text())[:300])
                    return None
                body = await response.json(content_type=None)
                routes = body.get("routes") or []
                if not routes:
                    return None
                duration = safe_str(routes[0].get("duration"), "")
                match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)s", duration)
                if not match:
                    return None
                seconds = float(match.group(1))
                return max(1, int((seconds + 59) // 60))
    except Exception as exc:
        logger.warning("Google Routes failed: %s", exc)
        return None


async def calculate_order_eta(
    data: dict,
    order_items: list[dict],
) -> dict:
    prep_minutes = calculate_prep_minutes(order_items)
    fulfillment = normalize_fulfillment(
        data.get("fulfillmentType", data.get("fulfillment_type"))
    )

    result = {
        "prep_minutes": prep_minutes,
        "travel_minutes": None,
        "delivery_minutes": None,
        "eta_minutes": prep_minutes,
        "fulfillment": fulfillment,
    }

    if fulfillment == "PICKUP":
        return result

    lat, lng = payload_coordinates(data)
    if lat is None or lng is None:
        address_plain = safe_str(
            data.get("address_plain") or data.get("address"),
            "",
        )
        lat, lng = await geocode_delivery_address(address_plain)

    if lat is not None and lng is not None:
        travel = await google_two_wheeler_minutes(lat, lng)
        if travel is not None:
            result["travel_minutes"] = travel
            result["delivery_minutes"] = travel + DELIVERY_BUFFER_MINUTES
            result["eta_minutes"] = prep_minutes + travel + DELIVERY_BUFFER_MINUTES

    return result


def eta_client_line(eta: dict) -> str:
    if eta.get("fulfillment") == "PICKUP":
        return (
            f"Расчётное время приготовления: {eta['prep_minutes']} мин., "
            "но мы постараемся как можно быстрее."
        )

    if eta.get("travel_minutes") is not None:
        return (
            f"Расчётное время доставки: {eta['eta_minutes']} мин., "
            "но мы постараемся как можно быстрее. "
            "Как курьер будет выезжать, я вам сообщу."
        )

    return (
        f"Расчётное время приготовления: {eta['prep_minutes']} мин. "
        "Точное время дороги не удалось получить автоматически, "
        "но мы постараемся как можно быстрее. Как курьер будет выезжать, я вам сообщу."
    )


async def send_order_to_screen(
    order_number: str,
    prep_minutes: int,
    order_items: list[dict],
    cutlery: bool | None = None,
) -> bool:
    screen_bases: list[str] = []
    for candidate in (SCREEN_SERVICE_URL, DEFAULT_SCREEN_SERVICE_URL):
        base = safe_str(candidate, "").rstrip("/")
        if base and base not in screen_bases:
            screen_bases.append(base)
    if not screen_bases:
        return False

    headers = {"Content-Type": "application/json"}
    if SCREEN_SERVICE_SECRET:
        headers["X-Screen-Secret"] = SCREEN_SERVICE_SECRET

    body = {
        "orderNo": order_number,
        "prepMinutes": prep_minutes,
        "items": [
            {
                "name": safe_str(item.get("name"), ""),
                "qty": max(1, safe_int(item.get("qty"), 1)),
            }
            for item in order_items
        ],
        "cutlery": cutlery,
    }

    # Try the configured URL first and the known working Screencook URL second.
    # This makes old/stale Railway variables non-fatal.
    for base in screen_bases:
        url = f"{base}/api/external-order"
        for attempt in range(2):
            try:
                timeout = aiohttp.ClientTimeout(total=4)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(url, json=body, headers=headers) as response:
                        text = await response.text()
                        if 200 <= response.status < 300:
                            logger.info("SCREEN ORDER OK: %s -> %s", order_number, base)
                            return True
                        logger.warning("SCREEN ORDER HTTP %s (%s): %s", response.status, base, text[:300])
            except Exception as exc:
                logger.warning("SCREEN ORDER %s attempt %s failed: %s", base, attempt + 1, exc)
            if attempt == 0:
                await asyncio.sleep(0.8)
    return False


async def latest_promptpay_order_for_user(telegram_id: int) -> str:
    memory_order = pending_promptpay_by_user.get(telegram_id)
    if memory_order:
        return memory_order
    if not db_pool:
        return ""
    try:
        row = await db_pool.fetchrow(
            """
            SELECT order_number
            FROM orders
            WHERE telegram_id=$1
              AND LOWER(COALESCE(payment_method,'')) IN ('promptpay','promtpay','thaibank','thai bank')
              AND created_at > NOW() - INTERVAL '6 hours'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            telegram_id,
        )
        return safe_str(row["order_number"], "") if row else ""
    except Exception as exc:
        logger.warning("Receipt order lookup failed: %s", exc)
        return ""


async def mark_receipt_received(order_number: str) -> None:
    if not order_number:
        return
    receipt_received_orders.add(order_number)
    task = pending_receipt_tasks.pop(order_number, None)
    if task and not task.done():
        task.cancel()
    if db_pool:
        try:
            await db_pool.execute(
                "UPDATE orders SET receipt_received_at=NOW() WHERE order_number=$1",
                order_number,
            )
        except Exception as exc:
            logger.warning("Could not mark receipt in DB: %s", exc)


async def receipt_reminder_worker(telegram_id: int, order_number: str) -> None:
    try:
        await asyncio.sleep(300)
        if order_number in receipt_received_orders:
            return
        if db_pool:
            try:
                received = await db_pool.fetchval(
                    "SELECT receipt_received_at IS NOT NULL FROM orders WHERE order_number=$1",
                    order_number,
                )
                if received:
                    return
            except Exception:
                pass
        await bot.send_message(
            telegram_id,
            "Не забудьте отправить мне чек об оплате пожалуйста.",
        )
    except asyncio.CancelledError:
        return
    except Exception as exc:
        logger.warning("Receipt reminder failed: %s", exc)
    finally:
        pending_receipt_tasks.pop(order_number, None)


def schedule_receipt_reminder(telegram_id: int, order_number: str, payment: str) -> None:
    if normalize_payment_name(payment) != "PromptPay" or not order_number:
        return
    pending_promptpay_by_user[telegram_id] = order_number
    previous = pending_receipt_tasks.pop(order_number, None)
    if previous and not previous.done():
        previous.cancel()
    pending_receipt_tasks[order_number] = asyncio.create_task(
        receipt_reminder_worker(telegram_id, order_number)
    )


async def process_website_order(payload: dict) -> dict:
    """Processes an order already saved by smokefactorybbq.com.

    This does not create a second DB order or change the old Mini App handler.
    """
    order_number = safe_str(
        payload.get("order_number") or payload.get("orderNumber"),
        "",
    ).strip().upper()
    telegram_id = safe_int(payload.get("telegram_id"), 0)
    source_items = payload.get("items") if isinstance(payload.get("items"), list) else []
    order_items = []
    for item in source_items:
        if not isinstance(item, dict):
            continue
        name = safe_str(item.get("name"), "").strip()
        qty = max(1, safe_int(item.get("qty"), 1))
        if name:
            order_items.append({
                "name": name,
                "qty": qty,
                "price": max(0, safe_int(item.get("price"), 0)),
                "img": safe_str(item.get("img"), ""),
            })

    if not order_number or not order_items:
        raise ValueError("order_number and items are required")

    eta = await calculate_order_eta(payload, order_items)
    screen_ok = await send_order_to_screen(
        order_number,
        int(eta["prep_minutes"]),
        order_items,
        payload.get("cutlery") if isinstance(payload.get("cutlery"), bool) else None,
    )

    customer_name = safe_str(payload.get("name"), "Клиент")
    phone = safe_str(payload.get("phone"), "")
    address_plain = safe_str(payload.get("address_plain") or payload.get("address"), "")
    payment = normalize_payment_name(payload.get("payment") or payload.get("payMethod"))
    total = max(0, safe_int(payload.get("total"), 0))
    fulfillment = normalize_fulfillment(payload.get("fulfillment_type") or payload.get("fulfillmentType"))

    if telegram_id:
        text = (
            f"📦 Ваш заказ {order_number} принят и уже готовится!\n\n"
            f"{eta_client_line(eta)}\n\n"
            f"💰 Итого: {total} ฿\n"
            f"Оплата: {payment}"
        )
        await bot.send_message(telegram_id, text)
        schedule_receipt_reminder(telegram_id, order_number, payment)

    admin_lines = [
        f"✅ <b>Новый заказ {html.escape(order_number)}</b>",
        f"Клиент: {html.escape(customer_name)}",
        f"Телефон: {html.escape(phone)}",
        f"Получение: {'Самовывоз' if fulfillment == 'PICKUP' else 'Доставка'}",
    ]
    if address_plain:
        admin_lines.append(f"Адрес: {html.escape(address_plain)}")
    admin_lines.append(f"Приготовление: {eta['prep_minutes']} мин")
    if eta.get("travel_minutes") is not None:
        admin_lines.append(f"Google мото: {eta['travel_minutes']} мин + {DELIVERY_BUFFER_MINUTES} мин")
    admin_lines.append("\n🍽 <b>Состав:</b>")
    for item in order_items:
        admin_lines.append(f"• {html.escape(item['name'])} ×{item['qty']}")
    admin_lines.append(f"\n💰 <b>Итого: {total} ฿</b>")

    try:
        await bot.send_message(ADMIN_CHAT_ID, "\n".join(admin_lines), parse_mode="HTML")
    except Exception:
        logger.exception("Website order manager message failed")

    print_payload = dict(payload)
    print_payload.update({
        "order_number": order_number,
        "orderNumber": order_number,
        "orderNo": order_number,
        "payment": payment,
        "items": order_items,
        "name": customer_name,
        "phone": phone,
        "address": safe_str(payload.get("address"), address_plain),
        "address_plain": address_plain,
        "fulfillment_type": fulfillment,
        "prep_minutes": int(eta["prep_minutes"]),
        "travel_minutes": eta.get("travel_minutes"),
        "eta_minutes": eta.get("eta_minutes"),
    })

    print_ok = False
    try:
        await send_payload_to_receipt_program(print_payload, timeout_seconds=8)
        print_ok = True
    except Exception as exc:
        logger.warning("Website order print failed: %s", exc)

    return {
        "ok": True,
        "orderNumber": order_number,
        "screenDelivered": screen_ok,
        "printDelivered": print_ok,
        "prepMinutes": eta["prep_minutes"],
        "travelMinutes": eta.get("travel_minutes"),
        "etaMinutes": eta.get("eta_minutes"),
    }


# ============================================================================
# HEALTHCHECK И ПЛАНОВЫЙ ПЕРЕЗАПУСК
# ============================================================================

def run_fake_server(
    port: int = PORT,
) -> None:
    class Handler(
        BaseHTTPRequestHandler
    ):
        def _json(self, status: int, payload: dict) -> None:
            raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self) -> None:
            self._json(
                200,
                {
                    "ok": True,
                    "service": "tgfoodbot",
                    "screenServiceUrl": SCREEN_SERVICE_URL,
                    "googleRoutesConfigured": bool(GOOGLE_MAPS_API_KEY),
                },
            )

        def do_POST(self) -> None:
            if self.path.rstrip("/") != "/website-order":
                self._json(404, {"ok": False, "error": "Not found"})
                return

            supplied_secret = self.headers.get("X-Website-Order-Secret", "")
            if WEBSITE_ORDER_SECRET and not hmac.compare_digest(
                supplied_secret, WEBSITE_ORDER_SECRET
            ):
                self._json(401, {"ok": False, "error": "Unauthorized"})
                return

            try:
                content_length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                content_length = 0

            if content_length <= 0 or content_length > 1_000_000:
                self._json(400, {"ok": False, "error": "Invalid body"})
                return

            try:
                raw = self.rfile.read(content_length)
                payload = json.loads(raw.decode("utf-8"))
                if not isinstance(payload, dict):
                    raise ValueError("JSON object expected")
            except Exception as exc:
                self._json(400, {"ok": False, "error": f"Invalid JSON: {exc}"})
                return

            if MAIN_LOOP is None:
                self._json(503, {"ok": False, "error": "Bot loop is not ready"})
                return

            future = asyncio.run_coroutine_threadsafe(
                process_website_order(payload),
                MAIN_LOOP,
            )
            try:
                result = future.result(timeout=24)
                self._json(200, result)
            except Exception as exc:
                future.cancel()
                logger.exception("/website-order failed")
                self._json(500, {"ok": False, "error": str(exc)[:500]})

        def log_message(
            self,
            format: str,
            *args,
        ) -> None:
            return

    server = HTTPServer(
        (
            "",
            port,
        ),
        Handler,
    )

    threading.Thread(
        target=server.serve_forever,
        daemon=True,
    ).start()


def schedule_restart() -> None:
    if RESTART_MINUTES <= 0:
        logger.info(
            "Плановый перезапуск отключён"
        )
        return

    def _restart() -> None:
        global broadcast_running

        if broadcast_running:
            logger.warning(
                "Перезапуск отложен: выполняется рассылка"
            )

            retry_timer = threading.Timer(
                600,
                _restart,
            )

            retry_timer.daemon = True
            retry_timer.start()

            return

        os.execv(
            sys.executable,
            [
                sys.executable,
                *sys.argv,
            ],
        )

    timer = threading.Timer(
        RESTART_MINUTES * 60,
        _restart,
    )

    timer.daemon = True
    timer.start()


# ============================================================================
# ПОДПИСАННАЯ ССЫЛКА MINI APP
# ============================================================================

def build_signed_webapp_url(
    user: types.User,
) -> str:
    payload = {
        "i": user.id,
        "n": user.username or "",
        "f": user.first_name or "",
        "l": user.last_name or "",
        "t": int(time.time()),
    }

    payload_json = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode(
        "utf-8"
    )

    token = (
        base64
        .urlsafe_b64encode(
            payload_json
        )
        .decode("ascii")
        .rstrip("=")
    )

    signature = hmac.new(
        API_TOKEN.encode(
            "utf-8"
        ),
        token.encode(
            "ascii"
        ),
        hashlib.sha256,
    ).hexdigest()

    parts = urlsplit(
        WEBAPP_URL
    )

    query = dict(
        parse_qsl(
            parts.query,
            keep_blank_values=True,
        )
    )

    query.update(
        {
            "u": token,
            "s": signature,
        }
    )

    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path or "/",
            urlencode(query),
            parts.fragment,
        )
    )


def start_keyboard(
    user: types.User,
) -> types.ReplyKeyboardMarkup:
    web_app_btn = types.KeyboardButton(
        text=MENU_BTN_TEXT,
        web_app=types.WebAppInfo(
            url=build_signed_webapp_url(
                user
            )
        ),
    )

    ask_btn = types.KeyboardButton(
        text=ASK_BTN_TEXT
    )

    return types.ReplyKeyboardMarkup(
        keyboard=[
            [
                web_app_btn
            ],
            [
                ask_btn
            ],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def updated_keyboard(
    user: types.User,
) -> types.ReplyKeyboardMarkup:
    return start_keyboard(
        user
    )


async def send_main_keyboard(
    message: types.Message,
    text: str,
    force: bool = False,
) -> bool:
    uid = message.from_user.id

    if (
        uid in KEYBOARD_SHOWN_USERS
        and not force
    ):
        return False

    await message.answer(
        text,
        reply_markup=start_keyboard(
            message.from_user
        ),
    )

    KEYBOARD_SHOWN_USERS.add(
        uid
    )

    return True


def make_user_from_database(
    row: asyncpg.Record,
) -> types.User:
    return types.User(
        id=int(
            row["telegram_id"]
        ),
        is_bot=False,
        first_name=(
            row[
                "telegram_first_name"
            ]
            or "Пользователь"
        ),
        last_name=row[
            "telegram_last_name"
        ],
        username=row[
            "username"
        ],
    )


# ============================================================================
# КНОПКИ
# ============================================================================

def build_admin_kb_full(
    client_id: int,
    order_id: int,
) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()

    kb.button(
        text="👤 Открыть профиль клиента",
        url=f"tg://user?id={client_id}",
    )

    kb.button(
        text="✍️ Написать клиенту",
        callback_data=(
            f"write_client:{client_id}"
        ),
    )

    kb.button(
        text="🧾 Отправить чек",
        callback_data=(
            f"resend_receipt:{order_id}"
        ),
    )

    kb.adjust(1)

    return kb.as_markup()


def build_admin_kb_safe(
    client_id: int,
    order_id: int,
) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()

    kb.button(
        text="✍️ Написать клиенту",
        callback_data=(
            f"write_client:{client_id}"
        ),
    )

    kb.button(
        text="🧾 Отправить чек",
        callback_data=(
            f"resend_receipt:{order_id}"
        ),
    )

    kb.adjust(1)

    return kb.as_markup()

def build_unsubscribe_keyboard(
) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()

    kb.button(
        text="🔕 Не получать рекламу",
        callback_data="unsubscribe_ads",
    )

    return kb.as_markup()


def build_broadcast_confirm_keyboard(
) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()

    kb.button(
        text="✅ Начать рассылку",
        callback_data="broadcast_confirm",
    )

    kb.button(
        text="❌ Отменить",
        callback_data="broadcast_cancel",
    )

    kb.adjust(1)

    return kb.as_markup()


def build_keyboard_update_confirm(
) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()

    kb.button(
        text="✅ Отправить клавиатуру всем",
        callback_data="keyboard_update_confirm",
    )

    kb.button(
        text="❌ Отменить",
        callback_data="keyboard_update_cancel",
    )

    kb.adjust(1)

    return kb.as_markup()


# ============================================================================
# ОТПРАВКА ЗАКАЗА МЕНЕДЖЕРУ
# ============================================================================

async def send_order_to_admin(
    admin_text_html: str,
    client_id: int,
    order_id: int,
) -> None:
    try:
        await bot.send_message(
            ADMIN_CHAT_ID,
            admin_text_html,
            parse_mode="HTML",
            reply_markup=(
                build_admin_kb_full(
                    client_id,
                    order_id,
                )
            ),
        )

        logger.info(
            "ADMIN: заказ отправлен с полной клавиатурой"
        )

    except Exception as exc:
        error_text = str(
            exc
        )

        logger.error(
            "ADMIN send failed: %s",
            error_text,
        )

        if (
            "BUTTON_USER_PRIVACY_RESTRICTED"
            in error_text
        ):
            await bot.send_message(
                ADMIN_CHAT_ID,
                admin_text_html,
                parse_mode="HTML",
                reply_markup=(
                    build_admin_kb_safe(
                        client_id,
                        order_id,
                    )
                ),
            )

            return

        raise


# ============================================================================
# ЗАЩИЩЁННАЯ СИСТЕМА ЛОЯЛЬНОСТИ
# ============================================================================

def make_loyalty_signature_payload(
    telegram_id: int,
    order_ref: str,
    items_total: int,
    delivery: int,
    requested_bonus: int,
    timestamp: int,
) -> str:
    return "|".join(
        [
            str(telegram_id),
            str(order_ref),
            str(items_total),
            str(delivery),
            str(requested_bonus),
            str(timestamp),
        ]
    )


async def settle_loyalty_order(
    telegram_id: int,
    order_ref: str,
    items_total: int,
    delivery: int,
    requested_bonus: int,
) -> dict:
    timestamp = int(time.time())
    body = {
        "telegramId": str(telegram_id),
        "orderRef": str(order_ref),
        "itemsTotal": int(items_total),
        "delivery": int(delivery),
        "requestedBonus": int(requested_bonus),
    }

    signature_payload = make_loyalty_signature_payload(
        telegram_id,
        order_ref,
        items_total,
        delivery,
        requested_bonus,
        timestamp,
    )

    signature = hmac.new(
        API_TOKEN.encode("utf-8"),
        signature_payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    timeout = aiohttp.ClientTimeout(total=20)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(
            LOYALTY_SETTLE_URL,
            json=body,
            headers={
                "X-Loyalty-Timestamp": str(timestamp),
                "X-Loyalty-Signature": signature,
            },
        ) as response:
            raw = await response.text()

            try:
                result = json.loads(raw)
            except Exception:
                result = {"ok": False, "error": raw[:500]}

            if response.status != 200 or not result.get("ok"):
                raise RuntimeError(
                    result.get("error")
                    or f"Loyalty HTTP {response.status}"
                )

            return result


async def update_saved_order_loyalty(
    order_id: int,
    bonus_used: int,
    cashback_percent: int,
    cashback_earned: int,
    final_total: int,
    loyalty_request_id: str,
) -> None:
    """
    Обновляет бонусы и итог заказа.

    Номер SM-* является только номером заказа.
    Нумерацию кассовых чеков ведёт printer_gui.py.
    """
    if not db_pool:
        raise RuntimeError("База данных не подключена")

    result = await db_pool.execute(
        """
        UPDATE orders
        SET
            discount_percent = 0,
            discount_amount = $2,
            bonus_used = $2,
            cashback_percent = $3,
            cashback_earned = $4,
            total = $5,
            loyalty_request_id = $6
        WHERE id = $1
        """,
        order_id,
        bonus_used,
        cashback_percent,
        cashback_earned,
        final_total,
        loyalty_request_id,
    )

    if result != "UPDATE 1":
        raise RuntimeError(
            "Заказ не найден при финализации"
        )


async def cancel_saved_order(order_id: int) -> None:
    if db_pool:
        await db_pool.execute(
            "UPDATE orders SET status='cancelled' WHERE id=$1",
            order_id,
        )


# ============================================================================
# СОХРАНЕНИЕ ЗАКАЗА В БАЗУ
# ============================================================================

async def save_order_to_database(
    user: types.User,
    data: dict,
    order_items: list[dict],
) -> tuple[int, str]:
    """
    Сохраняет заказ и атомарно получает следующий номер SM-*.

    SM-* — это внутренний номер заказа, а не номер кассового чека.
    Пропуски в номерах заказов допустимы. Нумерацию чеков ведёт
    локальная программа printer_gui.py.
    """

    if not db_pool:
        raise RuntimeError(
            "База данных не подключена"
        )

    await upsert_user(
        user
    )

    items_total = sum(
        max(
            0,
            safe_int(
                item.get("qty")
            ),
        )
        *
        max(
            0,
            safe_int(
                item.get("price")
            ),
        )
        for item in order_items
    )

    delivery = max(
        0,
        safe_int(
            data.get(
                "delivery",
                0,
            )
        ),
    )

    discount_percent = max(
        0,
        min(
            100,
            safe_int(
                data.get(
                    "discountPercent",
                    data.get(
                        "discount_percent",
                        0,
                    ),
                )
            ),
        ),
    )

    discount_amount = max(
        0,
        safe_int(
            data.get(
                "discountAmount",
                data.get(
                    "discount_amount",
                    data.get(
                        "discount",
                        0,
                    ),
                ),
            )
        ),
    )

    total = max(
        0,
        safe_int(
            data.get(
                "total",
                items_total
                + delivery
                - discount_amount,
            )
        ),
    )

    order_date = None

    raw_order_date = data.get(
        "orderDate"
    )

    if raw_order_date:
        try:
            order_date = datetime.strptime(
                str(raw_order_date),
                "%Y-%m-%d",
            ).date()

        except Exception:
            order_date = None

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            sequence_number = await conn.fetchval(
                """
                SELECT nextval(
                    'sm_order_number_seq'
                )
                """
            )

            order_number = (
                f"SM-{int(sequence_number)}"
            )

            order_id = await conn.fetchval(
                """
                INSERT INTO orders (
                    order_number,
                    telegram_id,
                    customer_name,
                    phone,
                    address,
                    address_plain,
                    payment_method,
                    delivery_fee,
                    items_total,
                    discount_percent,
                    discount_amount,
                    total,
                    order_when,
                    order_date,
                    order_time,
                    comment,
                    fulfillment_type,
                    customer_latitude,
                    customer_longitude
                )
                VALUES (
                    $1,$2,$3,$4,$5,$6,
                    $7,$8,$9,$10,$11,
                    $12,$13,$14,$15,$16,
                    $17,$18,$19
                )
                RETURNING id
                """,
                order_number,
                user.id,
                safe_str(
                    data.get("name")
                    or user.full_name
                ),
                safe_str(
                    data.get("phone")
                ),
                safe_str(
                    data.get("address")
                ),
                safe_str(
                    data.get(
                        "address_plain"
                    )
                ),
                safe_str(
                    data.get(
                        "payMethod"
                    )
                ),
                delivery,
                items_total,
                discount_percent,
                discount_amount,
                total,
                safe_str(
                    data.get(
                        "orderWhen"
                    )
                ),
                order_date,
                safe_str(
                    data.get(
                        "orderTime"
                    )
                ),
                safe_str(
                    data.get("comment")
                    or data.get(
                        "comments"
                    )
                    or data.get(
                        "note"
                    )
                ),
                normalize_fulfillment(
                    data.get("fulfillmentType", data.get("fulfillment_type"))
                ),
                payload_coordinates(data)[0],
                payload_coordinates(data)[1],
            )

            if order_items:
                await conn.executemany(
                    """
                    INSERT INTO order_items (
                        order_id,
                        item_name,
                        quantity,
                        unit_price,
                        image_url
                    )
                    VALUES (
                        $1,$2,$3,$4,$5
                    )
                    """,
                    [
                        (
                            order_id,
                            item["name"],
                            item["qty"],
                            item["price"],
                            item.get("img"),
                        )
                        for item
                        in order_items
                    ],
                )

    return (
        int(order_id),
        order_number,
    )


# ============================================================================
# ОТПРАВКА / ПОВТОРНАЯ ОТПРАВКА ЧЕКА
# ============================================================================

async def build_print_payload_from_database(
    order_id: int,
) -> dict:
    """
    Восстанавливает полный payload заказа из PostgreSQL.
    Используется кнопкой «Отправить чек», поэтому повторная отправка
    работает даже после перезапуска Railway.
    """
    if not db_pool:
        raise RuntimeError(
            "База данных не подключена"
        )

    async with db_pool.acquire() as conn:
        order_row = await conn.fetchrow(
            """
            SELECT
                id,
                order_number,
                customer_name,
                phone,
                address,
                payment_method,
                delivery_fee,
                items_total,
                discount_percent,
                discount_amount,
                bonus_used,
                cashback_percent,
                cashback_earned,
                total,
                order_when,
                order_date,
                order_time,
                comment,
                fulfillment_type,
                customer_latitude,
                customer_longitude,
                created_at
            FROM orders
            WHERE id = $1
            """,
            order_id,
        )

        if not order_row:
            raise LookupError(
                "Заказ не найден в базе"
            )

        item_rows = await conn.fetch(
            """
            SELECT
                item_name,
                quantity,
                unit_price,
                image_url
            FROM order_items
            WHERE order_id = $1
            ORDER BY id
            """,
            order_id,
        )

    order_number = safe_str(
        order_row["order_number"]
    )

    comment = safe_str(
        order_row["comment"]
    )

    items = [
        {
            "name": safe_str(
                row["item_name"]
            ),
            "qty": max(
                1,
                safe_int(
                    row["quantity"],
                    1,
                ),
            ),
            "price": max(
                0,
                safe_int(
                    row["unit_price"],
                    0,
                ),
            ),
            "img": safe_str(
                row["image_url"]
            ),
        }
        for row in item_rows
    ]

    created_at = order_row[
        "created_at"
    ]

    if created_at:
        try:
            created_at = created_at.astimezone(
                TIMEZONE
            ).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        except Exception:
            created_at = safe_str(
                created_at
            )
    else:
        created_at = datetime.now(
            TIMEZONE
        ).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    order_time = safe_str(
        order_row["order_time"]
    )

    return {
        "order_number": order_number,
        "orderNumber": order_number,
        "order_no": order_number,
        "orderNo": order_number,

        "name": safe_str(
            order_row["customer_name"]
        ),
        "phone": safe_str(
            order_row["phone"]
        ),
        "address": safe_str(
            order_row["address"]
        ),
        "delivery": max(
            0,
            safe_int(
                order_row["delivery_fee"],
                0,
            ),
        ),
        "payment": safe_str(
            order_row["payment_method"]
        ),
        "fulfillment_type": safe_str(
            order_row["fulfillment_type"],
            "DELIVERY",
        ),
        "customer_latitude": order_row["customer_latitude"],
        "customer_longitude": order_row["customer_longitude"],
        "items": items,

        "items_total": max(
            0,
            safe_int(
                order_row["items_total"],
                0,
            ),
        ),
        "itemsTotal": max(
            0,
            safe_int(
                order_row["items_total"],
                0,
            ),
        ),
        "subtotal": max(
            0,
            safe_int(
                order_row["items_total"],
                0,
            ),
        ),

        "discount_percent": max(
            0,
            safe_int(
                order_row["discount_percent"],
                0,
            ),
        ),
        "discountPercent": max(
            0,
            safe_int(
                order_row["discount_percent"],
                0,
            ),
        ),
        "discount_amount": max(
            0,
            safe_int(
                order_row["discount_amount"],
                0,
            ),
        ),
        "discountAmount": max(
            0,
            safe_int(
                order_row["discount_amount"],
                0,
            ),
        ),
        "discount": max(
            0,
            safe_int(
                order_row["bonus_used"] or order_row["discount_amount"],
                0,
            ),
        ),
        "bonus_used": max(
            0,
            safe_int(
                order_row["bonus_used"] or order_row["discount_amount"],
                0,
            ),
        ),
        "used_bonuses": max(
            0,
            safe_int(
                order_row["bonus_used"] or order_row["discount_amount"],
                0,
            ),
        ),
        "cashback_percent": max(
            0,
            safe_int(order_row["cashback_percent"], 0),
        ),
        "cashback_earned": max(
            0,
            safe_int(order_row["cashback_earned"], 0),
        ),

        "total": max(
            0,
            safe_int(
                order_row["total"],
                0,
            ),
        ),
        "date": created_at,
        "order_time": order_time,
        "order_when": safe_str(
            order_row["order_when"]
        ),

        "comment": comment,
        "comments": comment,
        "comment_text": comment,
        "note": comment,
        "notes": comment,
    }


async def send_payload_to_receipt_program(
    print_payload: dict,
    timeout_seconds: int = 12,
) -> tuple[int, str]:
    """
    Отправляет заказ в чековую программу.
    При временной ошибке ngrok (502/503/504) или сети делает один повтор.
    Повтор безопасен, потому что printer_gui.py не создаёт второй JSON
    для уже принятого order_number.
    """
    timeout = aiohttp.ClientTimeout(
        total=timeout_seconds
    )

    last_error: Exception | None = None

    async with aiohttp.ClientSession(
        timeout=timeout
    ) as session:
        for attempt in range(2):
            try:
                async with session.post(
                    PRINT_URL,
                    json=print_payload,
                ) as response:
                    response_text = await response.text()

                    if 200 <= response.status < 300:
                        return (
                            response.status,
                            response_text,
                        )

                    error = RuntimeError(
                        (
                            f"Чековая программа вернула HTTP "
                            f"{response.status}: "
                            f"{response_text[:500]}"
                        )
                    )

                    if response.status in (502, 503, 504) and attempt == 0:
                        last_error = error
                        logger.warning(
                            "PRINT HTTP %s. Повторная отправка через 1 секунду.",
                            response.status,
                        )
                        await asyncio.sleep(1)
                        continue

                    raise error

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = exc

                if attempt == 0:
                    logger.warning(
                        "PRINT network error: %s. Повторная отправка через 1 секунду.",
                        safe_str(exc),
                    )
                    await asyncio.sleep(1)
                    continue

                raise

    if last_error is not None:
        raise last_error

    raise RuntimeError("Не удалось отправить заказ в чековую программу")


# ============================================================================
# СТАТИСТИКА
# ============================================================================

async def build_daily_report() -> str:
    if not db_pool:
        raise RuntimeError(
            "База данных не подключена"
        )

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            WITH day_bounds AS (
                SELECT
                    (
                        date_trunc(
                            'day',
                            NOW()
                            AT TIME ZONE
                            'Asia/Bangkok'
                        )
                        AT TIME ZONE
                        'Asia/Bangkok'
                    ) AS start_utc,

                    (
                        (
                            date_trunc(
                                'day',
                                NOW()
                                AT TIME ZONE
                                'Asia/Bangkok'
                            )
                            + INTERVAL '1 day'
                        )
                        AT TIME ZONE
                        'Asia/Bangkok'
                    ) AS end_utc
            )

            SELECT
                (
                    SELECT COUNT(*)
                    FROM users
                ) AS total_users,

                (
                    SELECT COUNT(*)
                    FROM users
                    WHERE is_active = TRUE
                ) AS active_users,

                (
                    SELECT COUNT(*)
                    FROM users
                    WHERE
                        is_active = TRUE
                        AND marketing_allowed = TRUE
                ) AS marketing_users,

                (
                    SELECT COUNT(*)
                    FROM users
                    WHERE is_active = FALSE
                ) AS blocked_users,

                (
                    SELECT COUNT(*)
                    FROM users u,
                         day_bounds d
                    WHERE
                        u.last_bot_activity_at
                            >= d.start_utc
                        AND
                        u.last_bot_activity_at
                            < d.end_utc
                ) AS active_today,

                (
                    SELECT COUNT(*)
                    FROM visits v,
                         day_bounds d
                    WHERE
                        v.visited_at
                            >= d.start_utc
                        AND
                        v.visited_at
                            < d.end_utc
                ) AS visits,

                (
                    SELECT COUNT(
                        DISTINCT telegram_id
                    )
                    FROM visits v,
                         day_bounds d
                    WHERE
                        v.visited_at
                            >= d.start_utc
                        AND
                        v.visited_at
                            < d.end_utc
                        AND
                        telegram_id
                            IS NOT NULL
                ) AS unique_visitors,

                (
                    SELECT COUNT(*)
                    FROM users u,
                         day_bounds d
                    WHERE
                        u.created_at
                            >= d.start_utc
                        AND
                        u.created_at
                            < d.end_utc
                ) AS new_users,

                (
                    SELECT COUNT(*)
                    FROM orders o,
                         day_bounds d
                    WHERE
                        o.created_at
                            >= d.start_utc
                        AND
                        o.created_at
                            < d.end_utc
                ) AS orders_count,

                (
                    SELECT COUNT(
                        DISTINCT telegram_id
                    )
                    FROM orders o,
                         day_bounds d
                    WHERE
                        o.created_at
                            >= d.start_utc
                        AND
                        o.created_at
                            < d.end_utc
                ) AS buyers,

                (
                    SELECT
                        COALESCE(
                            SUM(total),
                            0
                        )
                    FROM orders o,
                         day_bounds d
                    WHERE
                        o.created_at
                            >= d.start_utc
                        AND
                        o.created_at
                            < d.end_utc
                ) AS revenue,

                (
                    SELECT
                        COALESCE(
                            AVG(total),
                            0
                        )
                    FROM orders o,
                         day_bounds d
                    WHERE
                        o.created_at
                            >= d.start_utc
                        AND
                        o.created_at
                            < d.end_utc
                ) AS avg_check
            """
        )

    visits = int(
        row["visits"]
        or 0
    )

    unique_visitors = int(
        row["unique_visitors"]
        or 0
    )

    orders_count = int(
        row["orders_count"]
        or 0
    )

    conversion = (
        orders_count
        / unique_visitors
        * 100
        if unique_visitors
        else 0
    )

    today = datetime.now(
        TIMEZONE
    ).strftime(
        "%d.%m.%Y"
    )

    return (
        f"📊 Статистика за {today}\n\n"

        f"👥 Всего ID в базе: "
        f"{int(row['total_users'] or 0)}\n"

        f"✅ Активных пользователей: "
        f"{int(row['active_users'] or 0)}\n"

        f"📣 Доступно для рекламы: "
        f"{int(row['marketing_users'] or 0)}\n"

        f"🚫 Заблокировали/недоступны: "
        f"{int(row['blocked_users'] or 0)}\n"

        f"💬 Пользователей бота сегодня: "
        f"{int(row['active_today'] or 0)}\n"

        f"🆕 Новых пользователей: "
        f"{int(row['new_users'] or 0)}\n\n"

        f"Открытий сайта: {visits}\n"

        f"Уникальных посетителей: "
        f"{unique_visitors}\n\n"

        f"Заказов: {orders_count}\n"

        f"Покупателей: "
        f"{int(row['buyers'] or 0)}\n"

        f"Конверсия: "
        f"{conversion:.1f}%\n\n"

        f"Выручка: "
        f"{int(row['revenue'] or 0)} ฿\n"

        f"Средний чек: "
        f"{round(float(row['avg_check'] or 0))} ฿"
    )


# ============================================================================
# РАССЫЛКИ
# ============================================================================

async def get_broadcast_targets(
    broadcast_type: str,
) -> list[asyncpg.Record]:
    if not db_pool:
        return []

    if broadcast_type == "advertising":
        return await db_pool.fetch(
            """
            SELECT
                telegram_id,
                username,
                telegram_first_name,
                telegram_last_name
            FROM users
            WHERE
                is_active = TRUE
                AND marketing_allowed = TRUE
                AND telegram_id <> $1
            ORDER BY telegram_id
            """,
            ADMIN_CHAT_ID,
        )

    return await db_pool.fetch(
        """
        SELECT
            telegram_id,
            username,
            telegram_first_name,
            telegram_last_name
        FROM users
        WHERE
            is_active = TRUE
            AND telegram_id <> $1
        ORDER BY telegram_id
        """,
        ADMIN_CHAT_ID,
    )


async def get_broadcast_target_count(
    broadcast_type: str,
) -> int:
    targets = await get_broadcast_targets(
        broadcast_type
    )

    return len(
        targets
    )


async def create_broadcast_log(
    broadcast_type: str,
    source_chat_id: int | None,
    source_message_id: int | None,
    total_targets: int,
) -> int:
    if not db_pool:
        return 0

    log_id = await db_pool.fetchval(
        """
        INSERT INTO broadcast_logs (
            broadcast_type,
            created_by,
            source_chat_id,
            source_message_id,
            total_targets,
            status
        )
        VALUES (
            $1,$2,$3,$4,$5,
            'running'
        )
        RETURNING id
        """,
        broadcast_type,
        ADMIN_CHAT_ID,
        source_chat_id,
        source_message_id,
        total_targets,
    )

    return int(
        log_id
    )


async def finish_broadcast_log(
    log_id: int,
    delivered: int,
    blocked: int,
    failed: int,
    status: str,
) -> None:
    if (
        not db_pool
        or not log_id
    ):
        return

    await db_pool.execute(
        """
        UPDATE broadcast_logs
        SET
            delivered = $2,
            blocked = $3,
            failed = $4,
            status = $5,
            completed_at = NOW()
        WHERE id = $1
        """,
        log_id,
        delivered,
        blocked,
        failed,
        status,
    )


async def send_advertising_message(
    telegram_id: int,
    source_chat_id: int,
    source_message_id: int,
) -> str:
    try:
        await bot.copy_message(
            chat_id=telegram_id,
            from_chat_id=source_chat_id,
            message_id=source_message_id,
            reply_markup=(
                build_unsubscribe_keyboard()
            ),
        )

        await mark_send_success(
            telegram_id,
            "broadcast",
        )

        return "delivered"

    except TelegramRetryAfter as exc:
        await asyncio.sleep(
            float(
                exc.retry_after
            )
            + 1
        )

        return await send_advertising_message(
            telegram_id,
            source_chat_id,
            source_message_id,
        )

    except Exception as exc:
        blocked = is_blocking_error(
            exc
        )

        await mark_send_error(
            telegram_id,
            str(exc),
            blocked,
        )

        if blocked:
            return "blocked"

        logger.warning(
            "BROADCAST SEND ERROR: user=%s error=%s",
            telegram_id,
            exc,
        )

        return "failed"


async def send_new_keyboard(
    user_row: asyncpg.Record,
) -> str:
    telegram_id = int(
        user_row[
            "telegram_id"
        ]
    )

    try:
        telegram_user = (
            make_user_from_database(
                user_row
            )
        )

        await bot.send_message(
            telegram_id,
            (
                "🔄 Меню Smoke Factory BBQ обновлено.\n"
                "Используйте новую кнопку ниже 👇"
            ),
            reply_markup=start_keyboard(
                telegram_user
            ),
        )

        await mark_send_success(
            telegram_id,
            "keyboard",
        )

        return "delivered"

    except TelegramRetryAfter as exc:
        await asyncio.sleep(
            float(
                exc.retry_after
            )
            + 1
        )

        return await send_new_keyboard(
            user_row
        )

    except Exception as exc:
        blocked = is_blocking_error(
            exc
        )

        await mark_send_error(
            telegram_id,
            str(exc),
            blocked,
        )

        if blocked:
            return "blocked"

        logger.warning(
            "KEYBOARD SEND ERROR: user=%s error=%s",
            telegram_id,
            exc,
        )

        return "failed"


async def run_broadcast(
    broadcast_type: str,
    source_chat_id: int | None = None,
    source_message_id: int | None = None,
) -> None:
    global broadcast_running

    if broadcast_lock.locked():
        await bot.send_message(
            ADMIN_CHAT_ID,
            "⚠️ Другая рассылка уже выполняется.",
        )

        return

    async with broadcast_lock:
        broadcast_running = True

        delivered = 0
        blocked = 0
        failed = 0
        log_id = 0

        try:
            targets = await get_broadcast_targets(
                broadcast_type
            )

            total = len(
                targets
            )

            log_id = await create_broadcast_log(
                broadcast_type,
                source_chat_id,
                source_message_id,
                total,
            )

            progress_message = await bot.send_message(
                ADMIN_CHAT_ID,
                (
                    "🚀 Рассылка запущена\n\n"
                    f"Получателей: {total}\n"
                    "Обработано: 0"
                ),
            )

            for index, user_row in enumerate(
                targets,
                start=1,
            ):
                telegram_id = int(
                    user_row[
                        "telegram_id"
                    ]
                )

                if broadcast_type == "advertising":
                    if (
                        source_chat_id is None
                        or source_message_id is None
                    ):
                        raise RuntimeError(
                            "Не найдено сообщение для рассылки"
                        )

                    result = await send_advertising_message(
                        telegram_id,
                        source_chat_id,
                        source_message_id,
                    )

                else:
                    result = await send_new_keyboard(
                        user_row
                    )

                if result == "delivered":
                    delivered += 1

                elif result == "blocked":
                    blocked += 1

                else:
                    failed += 1

                if (
                    index % 25 == 0
                    or index == total
                ):
                    try:
                        await bot.edit_message_text(
                            chat_id=ADMIN_CHAT_ID,
                            message_id=(
                                progress_message
                                .message_id
                            ),
                            text=(
                                "🚀 Рассылка выполняется\n\n"
                                f"Получателей: {total}\n"
                                f"Обработано: {index}\n"
                                f"Доставлено: {delivered}\n"
                                f"Недоступны: {blocked}\n"
                                f"Другие ошибки: {failed}"
                            ),
                        )

                    except TelegramBadRequest:
                        pass

                await asyncio.sleep(
                    BROADCAST_DELAY
                )

            await finish_broadcast_log(
                log_id,
                delivered,
                blocked,
                failed,
                "completed",
            )

            result_text = (
                "✅ Рассылка завершена\n\n"
                f"Всего получателей: {total}\n"
                f"Доставлено: {delivered}\n"
                f"Недоступны: {blocked}\n"
                f"Другие ошибки: {failed}"
            )

            try:
                await bot.edit_message_text(
                    chat_id=ADMIN_CHAT_ID,
                    message_id=(
                        progress_message
                        .message_id
                    ),
                    text=result_text,
                )

            except TelegramBadRequest:
                await bot.send_message(
                    ADMIN_CHAT_ID,
                    result_text,
                )

        except Exception as exc:
            logger.exception(
                "MASS BROADCAST ERROR"
            )

            await finish_broadcast_log(
                log_id,
                delivered,
                blocked,
                failed,
                "failed",
            )

            await bot.send_message(
                ADMIN_CHAT_ID,
                (
                    "⚠️ Рассылка остановлена.\n\n"
                    f"Ошибка: {exc}"
                ),
            )

        finally:
            broadcast_running = False


# ============================================================================
# КОМАНДА /bonus
# ============================================================================

def make_bonus_request_payload(
    telegram_id: int,
    amount: int,
    manager_id: int,
    timestamp: int,
    request_id: str,
) -> str:
    return json.dumps(
        {
            "amount": amount,
            "managerId": manager_id,
            "requestId": request_id,
            "telegramId": str(
                telegram_id
            ),
            "timestamp": timestamp,
        },
        separators=(",", ":"),
        sort_keys=True,
    )


async def update_bonus_in_mini_app(
    telegram_id: int,
    amount: int,
    manager_id: int,
    request_id: str,
) -> dict:
    timestamp = int(
        time.time()
    )

    payload_string = make_bonus_request_payload(
        telegram_id,
        amount,
        manager_id,
        timestamp,
        request_id,
    )

    signature = hmac.new(
        API_TOKEN.encode(
            "utf-8"
        ),
        payload_string.encode(
            "utf-8"
        ),
        hashlib.sha256,
    ).hexdigest()

    request_body = {
        "telegramId": str(
            telegram_id
        ),
        "amount": amount,
        "managerId": manager_id,
        "timestamp": timestamp,
        "requestId": request_id,
    }

    url = (
        f"{WEBAPP_URL}/api/admin/bonus"
    )

    timeout = aiohttp.ClientTimeout(
        total=15
    )

    async with aiohttp.ClientSession(
        timeout=timeout
    ) as session:
        async with session.post(
            url,
            json=request_body,
            headers={
                "X-Bonus-Signature":
                    signature,
            },
        ) as response:
            response_text = (
                await response.text()
            )

            try:
                response_data = json.loads(
                    response_text
                )

            except Exception:
                response_data = {
                    "ok": False,
                    "error": response_text,
                }

            if (
                response.status != 200
                or not response_data.get(
                    "ok"
                )
            ):
                raise RuntimeError(
                    response_data.get(
                        "error"
                    )
                    or (
                        "Mini App вернул ошибку "
                        f"HTTP {response.status}"
                    )
                )

            return response_data


async def save_bonus_in_bot_database(
    telegram_id: int,
    amount: int,
    manager_id: int,
    request_id: str,
) -> int:
    if not db_pool:
        raise RuntimeError(
            "База данных бота не подключена"
        )

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            previous_amount = await conn.fetchval(
                """
                SELECT manual_spend
                FROM users
                WHERE telegram_id = $1
                """,
                telegram_id,
            )

            previous_amount = int(
                previous_amount
                or 0
            )

            await conn.execute(
                """
                INSERT INTO users (
                    telegram_id,
                    manual_spend,
                    bonus_updated_at,
                    bonus_updated_by,
                    created_at,
                    updated_at
                )
                VALUES (
                    $1,
                    $2,
                    NOW(),
                    $3,
                    NOW(),
                    NOW()
                )

                ON CONFLICT (telegram_id)
                DO UPDATE SET
                    manual_spend =
                        EXCLUDED.manual_spend,

                    bonus_updated_at =
                        NOW(),

                    bonus_updated_by =
                        EXCLUDED.bonus_updated_by,

                    updated_at =
                        NOW()
                """,
                telegram_id,
                amount,
                manager_id,
            )

            await conn.execute(
                """
                INSERT INTO loyalty_adjustments (
                    request_id,
                    telegram_id,
                    previous_amount,
                    new_amount,
                    created_by,
                    source
                )
                VALUES (
                    $1,$2,$3,$4,$5,
                    'manager_bonus'
                )

                ON CONFLICT (request_id)
                DO NOTHING
                """,
                request_id,
                telegram_id,
                previous_amount,
                amount,
                manager_id,
            )

            return previous_amount


async def apply_manager_bonus(
    telegram_id: int,
    amount: int,
    manager_id: int,
) -> dict:
    request_id = (
        f"bonus:{manager_id}:"
        f"{telegram_id}:"
        f"{amount}:"
        f"{time.time_ns()}"
    )

    mini_app_result = (
        await update_bonus_in_mini_app(
            telegram_id,
            amount,
            manager_id,
            request_id,
        )
    )

    previous_local = (
        await save_bonus_in_bot_database(
            telegram_id,
            amount,
            manager_id,
            request_id,
        )
    )

    mini_app_result[
        "previousLocalAmount"
    ] = previous_local

    return mini_app_result


async def notify_user_about_bonus(
    telegram_id: int,
    total_spend: int,
    discount_percent: int,
) -> str:
    try:
        await bot.send_message(
            telegram_id,
            (
                "🎁 Ваша накопленная сумма "
                "в программе лояльности обновлена.\n\n"

                f"Накопленная сумма: "
                f"{total_spend:,} ฿\n"

                f"Текущая скидка: "
                f"{discount_percent}%\n\n"

                "Информация уже доступна "
                "в личном кабинете."
            ).replace(
                ",",
                " ",
            ),
        )

        await mark_send_success(
            telegram_id,
            "direct",
        )

        return "sent"

    except Exception as exc:
        blocked = is_blocking_error(
            exc
        )

        await mark_send_error(
            telegram_id,
            str(exc),
            blocked,
        )

        return (
            "blocked"
            if blocked
            else "failed"
        )


async def process_bonus_amount(
    message: types.Message,
    telegram_id: int,
    amount: int,
) -> None:
    await message.answer(
        (
            "⏳ Сохраняю сумму "
            "в личный кабинет...\n\n"

            f"Telegram ID: {telegram_id}\n"

            f"Ручная сумма: "
            f"{amount:,} ฿"
        ).replace(
            ",",
            " ",
        )
    )

    try:
        result = await apply_manager_bonus(
            telegram_id,
            amount,
            message.from_user.id,
        )

        manual_spend = int(
            result.get(
                "manualSpend",
                amount,
            )
            or amount
        )

        order_spend = int(
            result.get(
                "orderSpend",
                0,
            )
            or 0
        )

        total_spend = int(
            result.get(
                "totalSpend",
                manual_spend
                + order_spend,
            )
            or 0
        )

        discount_percent = int(
            result.get(
                "discountPercent",
                discount_by_spend(
                    total_spend
                ),
            )
            or 0
        )

        notification_status = (
            await notify_user_about_bonus(
                telegram_id,
                total_spend,
                discount_percent,
            )
        )

        notification_text = {
            "sent":
                "Пользователь уведомлён.",

            "blocked":
                (
                    "Пользователь заблокировал бота, "
                    "но сумма в ЛК сохранена."
                ),

            "failed":
                (
                    "Сумма сохранена, но уведомление "
                    "отправить не удалось."
                ),
        }.get(
            notification_status,
            "Статус уведомления неизвестен.",
        )

        await message.answer(
            (
                "✅ Сумма сохранена "
                "в личном кабинете\n\n"

                f"Telegram ID: "
                f"{telegram_id}\n"

                f"Фактические заказы: "
                f"{order_spend:,} ฿\n"

                f"Ручная сумма: "
                f"{manual_spend:,} ฿\n"

                f"Общая накопленная сумма: "
                f"{total_spend:,} ฿\n"

                f"Уровень скидки: "
                f"{discount_percent}%\n\n"

                f"{notification_text}"
            ).replace(
                ",",
                " ",
            )
        )

        waiting_bonus.pop(
            message.from_user.id,
            None,
        )

    except Exception as exc:
        logger.exception(
            "BONUS SAVE ERROR"
        )

        await message.answer(
            (
                "⚠️ Не удалось сохранить сумму.\n\n"

                f"Ошибка: {exc}\n\n"

                "Можно повторно отправить сумму "
                "или выполнить /cancel."
            )
        )


# ============================================================================
# ПОЛЬЗОВАТЕЛЬСКИЕ КОМАНДЫ
# ============================================================================

@dp.message(
    Command("start")
)
async def cmd_start(
    message: types.Message,
) -> None:
    await upsert_user(
        message.from_user
    )

    await send_main_keyboard(
        message,
        (
            "Нажмите кнопку ниже, "
            "чтобы открыть меню.\n"

            "Если есть вопросы — нажмите "
            "«💬 Задать вопрос менеджеру».\n\n"

            "Отключить рекламу: /stop"
        ),
        force=True,
    )


@dp.message(
    Command("stop")
)
async def cmd_stop_ads(
    message: types.Message,
) -> None:
    await set_marketing_allowed(
        message.from_user.id,
        False,
    )

    await message.answer(
        (
            "🔕 Рекламные сообщения отключены.\n\n"

            "Сообщения о заказах и обновления меню "
            "продолжат приходить.\n\n"

            "Включить рекламу снова: /ads_on"
        ),
        reply_markup=start_keyboard(
            message.from_user
        ),
    )


@dp.message(
    Command("ads_on")
)
async def cmd_ads_on(
    message: types.Message,
) -> None:
    await set_marketing_allowed(
        message.from_user.id,
        True,
    )

    await message.answer(
        "🔔 Рекламные сообщения включены.",
        reply_markup=start_keyboard(
            message.from_user
        ),
    )


@dp.callback_query(
    F.data == "unsubscribe_ads"
)
async def callback_unsubscribe_ads(
    call: types.CallbackQuery,
) -> None:
    await set_marketing_allowed(
        call.from_user.id,
        False,
    )

    await call.answer(
        "Рекламные сообщения отключены",
        show_alert=True,
    )

    try:
        await call.message.edit_reply_markup(
            reply_markup=None
        )

    except TelegramBadRequest:
        pass


@dp.message(
    F.text == MENU_BTN_TEXT
)
async def refresh_menu_keyboard(
    message: types.Message,
) -> None:
    await message.answer(
        (
            "✅ Кнопка меню обновлена. "
            "Нажмите её ещё раз."
        ),
        reply_markup=updated_keyboard(
            message.from_user
        ),
    )

    KEYBOARD_SHOWN_USERS.add(
        message.from_user.id
    )


@dp.message(
    F.text == ASK_BTN_TEXT
)
async def open_manager_chat(
    message: types.Message,
) -> None:
    kb = InlineKeyboardBuilder()

    kb.button(
        text="👉 Открыть чат менеджера",
        url=MANAGER_URL,
    )

    kb.button(
        text="⬅️ Назад в меню",
        callback_data="back_to_menu",
    )

    kb.adjust(1)

    await message.answer(
        "Открой чат менеджера по кнопке ниже 👇",
        reply_markup=kb.as_markup(),
    )


@dp.callback_query(
    F.data == "back_to_menu"
)
async def back_to_menu(
    call: types.CallbackQuery,
) -> None:
    await call.message.answer(
        "Ок. Возвращаю кнопки меню 👇",
        reply_markup=start_keyboard(
            call.from_user
        ),
    )

    KEYBOARD_SHOWN_USERS.add(
        call.from_user.id
    )

    await call.answer()


# ============================================================================
# АДМИНИСТРАТИВНЫЕ КОМАНДЫ
# ============================================================================

@dp.message(
    Command("adminhelp")
)
async def cmd_admin_help(
    message: types.Message,
) -> None:
    if not is_admin(
        message.from_user.id
    ):
        return

    await message.answer(
        (
            "🛠 Команды администратора\n\n"

            "/nu4etam — общая статистика\n"

            "/users — пользователи бота\n"

            "/checkuser ID — проверить ID\n"

            "/export_users — выгрузить CSV\n"

            "/broadcast — создать рекламу\n"

            "/broadcast_history — история рассылок\n"

            "/update_keyboard — обновить клавиатуру всем\n"

            "/bonus — установить ручную накопленную сумму\n"

            "/cancel — отменить текущее действие"
        )
    )


@dp.message(
    Command("bonus")
)
async def cmd_bonus(
    message: types.Message,
) -> None:
    if not is_admin(
        message.from_user.id
    ):
        return

    parts = (
        message.text
        or ""
    ).split()

    if len(parts) >= 3:
        try:
            telegram_id = int(
                parts[1]
            )

        except ValueError:
            await message.answer(
                "Telegram ID должен состоять из цифр."
            )
            return

        amount = parse_money_amount(
            "".join(
                parts[2:]
            )
        )

        if amount is None:
            await message.answer(
                "Сумма указана неправильно."
            )
            return

        await process_bonus_amount(
            message,
            telegram_id,
            amount,
        )

        return

    waiting_bonus[
        message.from_user.id
    ] = {
        "stage": "telegram_id"
    }

    await message.answer(
        (
            "🎁 Ручная сумма лояльности\n\n"

            "Пришли Telegram ID и сумму.\n\n"

            "Можно одним сообщением:\n"

            "123456789 5000\n\n"

            "Или сначала ID, "
            "а следующим сообщением сумму.\n\n"

            "Сумма заменит прежнюю ручную сумму. "
            "Фактические заказы не изменятся.\n\n"

            "Для сброса укажи 0.\n"

            "Отмена: /cancel"
        )
    )


@dp.message(
    Command("nu4etam")
)
async def cmd_daily_report(
    message: types.Message,
) -> None:
    if not is_admin(
        message.from_user.id
    ):
        return

    try:
        await message.answer(
            await build_daily_report()
        )

    except Exception:
        logger.exception(
            "Ошибка формирования отчёта"
        )

        await message.answer(
            "⚠️ Не удалось сформировать отчёт."
        )


@dp.message(
    Command("users")
)
async def cmd_users(
    message: types.Message,
) -> None:
    if not is_admin(
        message.from_user.id
    ):
        return

    if not db_pool:
        await message.answer(
            "База данных не подключена."
        )
        return

    stats = await db_pool.fetchrow(
        """
        SELECT
            COUNT(*) AS total,

            COUNT(*) FILTER (
                WHERE is_active = TRUE
            ) AS active,

            COUNT(*) FILTER (
                WHERE
                    is_active = TRUE
                    AND marketing_allowed = TRUE
            ) AS advertising,

            COUNT(*) FILTER (
                WHERE marketing_allowed = FALSE
            ) AS unsubscribed,

            COUNT(*) FILTER (
                WHERE is_active = FALSE
            ) AS blocked

        FROM users
        """
    )

    recent_users = await db_pool.fetch(
        """
        SELECT
            telegram_id,
            username,
            telegram_first_name,
            telegram_last_name,
            is_active,
            marketing_allowed,
            manual_spend,
            last_bot_activity_at

        FROM users

        ORDER BY
            last_bot_activity_at
            DESC NULLS LAST

        LIMIT 15
        """
    )

    lines = [
        "👥 Пользователи бота",
        "",
        f"Всего ID: {int(stats['total'] or 0)}",
        f"Активных: {int(stats['active'] or 0)}",
        f"Для рекламы: {int(stats['advertising'] or 0)}",
        (
            "Отказались от рекламы: "
            f"{int(stats['unsubscribed'] or 0)}"
        ),
        f"Недоступны: {int(stats['blocked'] or 0)}",
        "",
        "Последние 15:",
    ]

    for user in recent_users:
        full_name = " ".join(
            part
            for part in (
                user[
                    "telegram_first_name"
                ],
                user[
                    "telegram_last_name"
                ],
            )
            if part
        ).strip()

        username = (
            f"@{user['username']}"
            if user["username"]
            else "без username"
        )

        active_icon = (
            "✅"
            if user["is_active"]
            else "🚫"
        )

        ads_icon = (
            "📣"
            if user[
                "marketing_allowed"
            ]
            else "🔕"
        )

        manual_spend = int(
            user[
                "manual_spend"
            ]
            or 0
        )

        lines.append(
            (
                f"{active_icon}{ads_icon} "
                f"{user['telegram_id']} — "
                f"{full_name or 'Без имени'} — "
                f"{username} — "
                f"ручная сумма {manual_spend} ฿"
            )
        )

    await message.answer(
        "\n".join(
            lines
        )[:4096]
    )


@dp.message(
    Command("checkuser")
)
async def cmd_check_user(
    message: types.Message,
) -> None:
    if not is_admin(
        message.from_user.id
    ):
        return

    if not db_pool:
        return

    parts = (
        message.text
        or ""
    ).split(
        maxsplit=1
    )

    if len(parts) != 2:
        await message.answer(
            "Использование:\n/checkuser 123456789"
        )
        return

    try:
        telegram_id = int(
            parts[1].strip()
        )

    except ValueError:
        await message.answer(
            "Telegram ID должен состоять из цифр."
        )
        return

    user = await db_pool.fetchrow(
        """
        SELECT *
        FROM users
        WHERE telegram_id = $1
        """,
        telegram_id,
    )

    if not user:
        await message.answer(
            f"❌ Пользователь {telegram_id} не найден."
        )
        return

    full_name = " ".join(
        part
        for part in (
            user[
                "telegram_first_name"
            ],
            user[
                "telegram_last_name"
            ],
        )
        if part
    ).strip()

    await message.answer(
        (
            "✅ Пользователь найден\n\n"

            f"Telegram ID: "
            f"{user['telegram_id']}\n"

            f"Username: "
            f"@{user['username'] or '-'}\n"

            f"Имя Telegram: "
            f"{full_name or '-'}\n"

            f"Имя в заказе: "
            f"{user['profile_name'] or '-'}\n"

            f"Телефон: "
            f"{user['phone'] or '-'}\n"

            f"Адрес: "
            f"{user['address'] or '-'}\n"

            f"Ручная сумма: "
            f"{int(user['manual_spend'] or 0)} ฿\n"

            f"Обновил сумму: "
            f"{user['bonus_updated_by'] or '-'}\n"

            f"Дата обновления: "
            f"{user['bonus_updated_at'] or '-'}\n"

            f"Активен: "
            f"{'да' if user['is_active'] else 'нет'}\n"

            f"Реклама разрешена: "
            f"{'да' if user['marketing_allowed'] else 'нет'}\n"

            f"Создан: "
            f"{user['created_at']}\n"

            f"Последняя активность: "
            f"{user['last_bot_activity_at'] or '-'}\n"

            f"Последняя успешная отправка: "
            f"{user['last_successful_send_at'] or '-'}\n"

            f"Заблокирован: "
            f"{user['blocked_at'] or '-'}\n"

            f"Последняя ошибка: "
            f"{user['last_send_error'] or '-'}"
        )
    )


@dp.message(
    Command("export_users")
)
async def cmd_export_users(
    message: types.Message,
) -> None:
    if not is_admin(
        message.from_user.id
    ):
        return

    if not db_pool:
        return

    users = await db_pool.fetch(
        """
        SELECT
            telegram_id,
            username,
            telegram_first_name,
            telegram_last_name,
            profile_name,
            phone,
            address,
            manual_spend,
            bonus_updated_at,
            bonus_updated_by,
            is_active,
            marketing_allowed,
            created_at,
            last_bot_activity_at,
            last_broadcast_at,
            last_keyboard_sent_at,
            blocked_at,
            last_send_error

        FROM users

        ORDER BY created_at
        """
    )

    output = io.StringIO()

    writer = csv.writer(
        output
    )

    columns = [
        "telegram_id",
        "username",
        "telegram_first_name",
        "telegram_last_name",
        "profile_name",
        "phone",
        "address",
        "manual_spend",
        "bonus_updated_at",
        "bonus_updated_by",
        "is_active",
        "marketing_allowed",
        "created_at",
        "last_bot_activity_at",
        "last_broadcast_at",
        "last_keyboard_sent_at",
        "blocked_at",
        "last_send_error",
    ]

    writer.writerow(
        columns
    )

    for user in users:
        writer.writerow(
            [
                user[column]
                for column in columns
            ]
        )

    filename = (
        "smoke_factory_users_"
        + datetime.now(
            TIMEZONE
        ).strftime(
            "%Y%m%d_%H%M"
        )
        + ".csv"
    )

    file = BufferedInputFile(
        output.getvalue().encode(
            "utf-8-sig"
        ),
        filename=filename,
    )

    await message.answer_document(
        file,
        caption=(
            f"Пользователей в базе: "
            f"{len(users)}"
        ),
    )


# ============================================================================
# РЕКЛАМНАЯ РАССЫЛКА
# ============================================================================

@dp.message(
    Command("broadcast")
)
async def cmd_broadcast(
    message: types.Message,
) -> None:
    if not is_admin(
        message.from_user.id
    ):
        return

    if (
        broadcast_running
        or broadcast_lock.locked()
    ):
        await message.answer(
            "⚠️ Другая рассылка уже выполняется."
        )
        return

    waiting_broadcast.add(
        message.from_user.id
    )

    pending_broadcasts.pop(
        message.from_user.id,
        None,
    )

    await message.answer(
        (
            "📣 Пришли следующим сообщением рекламу.\n\n"

            "Можно отправить текст, фотографию, "
            "видео или документ.\n\n"

            "После этого бот покажет предпросмотр.\n"

            "Отмена: /cancel"
        )
    )


@dp.callback_query(
    F.data == "broadcast_confirm"
)
async def callback_broadcast_confirm(
    call: types.CallbackQuery,
) -> None:
    if not is_admin(
        call.from_user.id
    ):
        await call.answer(
            "Недостаточно прав",
            show_alert=True,
        )
        return

    prepared = pending_broadcasts.get(
        call.from_user.id
    )

    if not prepared:
        await call.answer(
            (
                "Рассылка не найдена. "
                "Создай её заново."
            ),
            show_alert=True,
        )
        return

    if (
        broadcast_running
        or broadcast_lock.locked()
    ):
        await call.answer(
            "Другая рассылка уже выполняется.",
            show_alert=True,
        )
        return

    await call.answer(
        "Рассылка запущена"
    )

    try:
        await call.message.edit_reply_markup(
            reply_markup=None
        )

    except TelegramBadRequest:
        pass

    pending_broadcasts.pop(
        call.from_user.id,
        None,
    )

    asyncio.create_task(
        run_broadcast(
            "advertising",
            int(
                prepared[
                    "source_chat_id"
                ]
            ),
            int(
                prepared[
                    "source_message_id"
                ]
            ),
        )
    )


@dp.callback_query(
    F.data == "broadcast_cancel"
)
async def callback_broadcast_cancel(
    call: types.CallbackQuery,
) -> None:
    if not is_admin(
        call.from_user.id
    ):
        return

    waiting_broadcast.discard(
        call.from_user.id
    )

    pending_broadcasts.pop(
        call.from_user.id,
        None,
    )

    await call.answer(
        "Рассылка отменена"
    )

    try:
        await call.message.edit_reply_markup(
            reply_markup=None
        )

    except TelegramBadRequest:
        pass


@dp.message(
    Command("broadcast_history")
)
async def cmd_broadcast_history(
    message: types.Message,
) -> None:
    if not is_admin(
        message.from_user.id
    ):
        return

    if not db_pool:
        return

    rows = await db_pool.fetch(
        """
        SELECT
            id,
            broadcast_type,
            total_targets,
            delivered,
            blocked,
            failed,
            status,
            created_at,
            completed_at

        FROM broadcast_logs

        ORDER BY id DESC

        LIMIT 10
        """
    )

    if not rows:
        await message.answer(
            "История рассылок пока пуста."
        )
        return

    lines = [
        "📊 Последние рассылки",
        "",
    ]

    for row in rows:
        lines.append(
            (
                f"№{row['id']} — "
                f"{row['broadcast_type']}\n"

                f"Статус: "
                f"{row['status']}\n"

                f"Всего: "
                f"{row['total_targets']}, "

                f"доставлено: "
                f"{row['delivered']}, "

                f"недоступны: "
                f"{row['blocked']}, "

                f"ошибки: "
                f"{row['failed']}\n"

                f"Дата: "
                f"{row['created_at']}\n"
            )
        )

    await message.answer(
        "\n".join(
            lines
        )[:4096]
    )


# ============================================================================
# ОБНОВЛЕНИЕ КЛАВИАТУРЫ
# ============================================================================

@dp.message(
    Command("update_keyboard")
)
async def cmd_update_keyboard(
    message: types.Message,
) -> None:
    if not is_admin(
        message.from_user.id
    ):
        return

    if (
        broadcast_running
        or broadcast_lock.locked()
    ):
        await message.answer(
            "⚠️ Другая рассылка уже выполняется."
        )
        return

    count = await get_broadcast_target_count(
        "keyboard"
    )

    await message.answer(
        (
            "🔄 Массовое обновление клавиатуры\n\n"

            f"Получателей: {count}\n\n"

            "Клавиатуру получат все активные "
            "пользователи, включая отключивших рекламу."
        ),
        reply_markup=(
            build_keyboard_update_confirm()
        ),
    )


@dp.callback_query(
    F.data == "keyboard_update_confirm"
)
async def callback_keyboard_update_confirm(
    call: types.CallbackQuery,
) -> None:
    if not is_admin(
        call.from_user.id
    ):
        await call.answer(
            "Недостаточно прав",
            show_alert=True,
        )
        return

    if (
        broadcast_running
        or broadcast_lock.locked()
    ):
        await call.answer(
            "Другая рассылка уже выполняется.",
            show_alert=True,
        )
        return

    await call.answer(
        "Отправка клавиатуры запущена"
    )

    try:
        await call.message.edit_reply_markup(
            reply_markup=None
        )

    except TelegramBadRequest:
        pass

    asyncio.create_task(
        run_broadcast(
            "keyboard"
        )
    )


@dp.callback_query(
    F.data == "keyboard_update_cancel"
)
async def callback_keyboard_update_cancel(
    call: types.CallbackQuery,
) -> None:
    if not is_admin(
        call.from_user.id
    ):
        return

    await call.answer(
        "Отменено"
    )

    try:
        await call.message.edit_reply_markup(
            reply_markup=None
        )

    except TelegramBadRequest:
        pass


# ============================================================================
# РУЧНАЯ ПОВТОРНАЯ ОТПРАВКА ЧЕКА
# ============================================================================

@dp.callback_query(
    F.data.startswith(
        "resend_receipt:"
    )
)
async def cb_resend_receipt(
    call: types.CallbackQuery,
) -> None:
    if not is_admin(
        call.from_user.id
    ):
        await call.answer(
            "Недостаточно прав",
            show_alert=True,
        )
        return

    try:
        order_id = int(
            call.data.split(
                ":",
                1,
            )[1]
        )
    except Exception:
        await call.answer(
            "Ошибка номера заказа",
            show_alert=True,
        )
        return

    # Сразу отвечаем Telegram, чтобы кнопка не зависла.
    await call.answer(
        "Отправляю чек…"
    )

    status_message = await call.message.answer(
        "⏳ Восстанавливаю заказ из базы и отправляю в чековую программу…"
    )

    try:
        print_payload = await build_print_payload_from_database(
            order_id
        )

        status_code, response_text = (
            await send_payload_to_receipt_program(
                print_payload,
                timeout_seconds=15,
            )
        )

        order_number = safe_str(
            print_payload.get(
                "order_number"
            )
        )

        await status_message.edit_text(
            (
                f"✅ Чек заказа {order_number} "
                "успешно отправлен в чековую программу.\n\n"
                f"HTTP: {status_code}\n"
                f"Ответ: {response_text[:300] or 'OK'}"
            )
        )

        logger.info(
            (
                "MANUAL PRINT SUCCESS: "
                "order_id=%s order_number=%s response=%s"
            ),
            order_id,
            order_number,
            response_text[:500],
        )

    except asyncio.TimeoutError:
        await status_message.edit_text(
            (
                "❌ Чек не отправлен: чековая программа "
                "не ответила вовремя.\n\n"
                "Проверьте интернет, ngrok и запущена ли программа, "
                "затем нажмите «🧾 Отправить чек» ещё раз."
            )
        )

        logger.exception(
            "MANUAL PRINT TIMEOUT: order_id=%s",
            order_id,
        )

    except aiohttp.ClientError as exc:
        await status_message.edit_text(
            (
                "❌ Не удалось подключиться к чековой программе.\n\n"
                f"Ошибка: {exc}\n\n"
                "Проверьте PRINT_URL, ngrok и интернет, "
                "затем нажмите кнопку ещё раз."
            )
        )

        logger.exception(
            "MANUAL PRINT NETWORK ERROR: order_id=%s",
            order_id,
        )

    except Exception as exc:
        await status_message.edit_text(
            (
                "❌ Чек не отправлен.\n\n"
                f"Ошибка: {exc}\n\n"
                "После восстановления связи нажмите "
                "«🧾 Отправить чек» ещё раз."
            )
        )

        logger.exception(
            "MANUAL PRINT ERROR: order_id=%s",
            order_id,
        )


# ============================================================================
# ОТВЕТ МЕНЕДЖЕРА КЛИЕНТУ
# ============================================================================

@dp.callback_query(
    F.data.startswith(
        "write_client:"
    )
)
async def cb_write_client(
    call: types.CallbackQuery,
) -> None:
    if not is_admin(
        call.from_user.id
    ):
        await call.answer(
            "Недостаточно прав",
            show_alert=True,
        )
        return

    try:
        client_id = int(
            call.data.split(
                ":",
                1,
            )[1]
        )

    except Exception:
        await call.answer(
            "Ошибка данных",
            show_alert=True,
        )
        return

    waiting_reply[
        call.from_user.id
    ] = {
        "client_id": client_id
    }

    await call.message.answer(
        (
            "✍️ Напишите текст клиенту.\n"
            "Отмена: /cancel"
        )
    )

    await call.answer(
        "Жду текст"
    )


@dp.message(
    Command("cancel")
)
async def cmd_cancel(
    message: types.Message,
) -> None:
    if not is_admin(
        message.from_user.id
    ):
        return

    admin_id = message.from_user.id

    cancelled = False

    if admin_id in waiting_reply:
        waiting_reply.pop(
            admin_id,
            None,
        )

        cancelled = True

    if admin_id in waiting_broadcast:
        waiting_broadcast.discard(
            admin_id
        )

        cancelled = True

    if admin_id in pending_broadcasts:
        pending_broadcasts.pop(
            admin_id,
            None,
        )

        cancelled = True

    if admin_id in waiting_bonus:
        waiting_bonus.pop(
            admin_id,
            None,
        )

        cancelled = True

    await message.answer(
        (
            "✅ Действие отменено."
            if cancelled
            else "Нет активного действия."
        )
    )


# ============================================================================
# ЗАКАЗЫ ИЗ TELEGRAM WEB APP
# ============================================================================

@dp.message(
    F.content_type
    == ContentType.WEB_APP_DATA
)
async def handle_order(
    message: types.Message,
) -> None:
    logger.info(
        "===== ПОЛУЧЕН ЗАКАЗ ОТ WEB APP ====="
    )

    raw = message.web_app_data.data

    logger.info(
        "RAW: %s",
        raw,
    )

    try:
        data = json.loads(
            raw
        )

    except Exception:
        logger.exception(
            "JSON parse error"
        )

        await message.answer(
            "⚠️ Ошибка данных заказа.",
            reply_markup=start_keyboard(
                message.from_user
            ),
        )

        return

    user = message.from_user

    client_id = user.id

    await upsert_user(
        user
    )

    pay_method = safe_str(
        data.get(
            "payMethod",
            "не выбран",
        ),
        "не выбран",
    )

    username = (
        f"@{user.username}"
        if user.username
        else (
            user.full_name
            or "Без имени"
        )
    )

    phone = safe_str(
        data.get(
            "phone",
            "не указан",
        ),
        "не указан",
    )

    address = safe_str(
        data.get(
            "address",
            "не указан",
        ),
        "не указан",
    )

    delivery = max(
        0,
        safe_int(
            data.get(
                "delivery",
                0,
            ),
            0,
        ),
    )

    requested_bonus = max(
        0,
        safe_int(
            data.get(
                "bonusRequested",
                data.get(
                    "bonus_requested",
                    data.get("discountAmount", 0),
                ),
            ),
            0,
        ),
    )

    discount_percent = 0
    discount_amount = 0

    items = (
        data.get(
            "items"
        )
        or {}
    )

    if not isinstance(
        items,
        dict,
    ):
        items = {}

    comment = (
        data.get(
            "comment"
        )
        or data.get(
            "comments"
        )
        or data.get(
            "comment_text"
        )
        or data.get(
            "note"
        )
        or data.get(
            "notes"
        )
        or ""
    )

    comment = (
        safe_str(
            comment,
            "",
        )
        .strip()
        .lstrip(";")
    )

    when_str = ""

    try:
        if data.get(
            "orderWhen"
        ) in (
            "soonest",
            "asap",
        ):
            raw_date = data.get(
                "orderDate"
            )

            dt = (
                datetime.strptime(
                    str(
                        raw_date
                    ),
                    "%Y-%m-%d",
                )
                if raw_date
                else datetime.now(
                    TIMEZONE
                )
            )

            when_str = (
                f"{dt.strftime('%d.%m')}, "
                "ближайшее"
            )

        elif (
            data.get(
                "orderDate"
            )
            and data.get(
                "orderTime"
            )
        ):
            dt = datetime.strptime(
                str(
                    data[
                        "orderDate"
                    ]
                ),
                "%Y-%m-%d",
            )

            when_str = (
                f"{dt.strftime('%d.%m')} "
                f"в {data['orderTime']}"
            )

    except Exception:
        logger.exception(
            "when_str parse error"
        )

        when_str = ""

    lines: list[str] = []

    order_items: list[
        dict
    ] = []

    for raw_name, info in items.items():
        if not isinstance(info, dict):
            continue

        name = safe_str(raw_name, "").strip()

        if name not in MENU_PRICE_MAP:
            logger.warning(
                "ORDER REJECTED: unknown item=%r user=%s",
                name,
                client_id,
            )
            await message.answer(
                (
                    "⚠️ В заказе обнаружено неизвестное блюдо. "
                    "Обновите меню и оформите заказ заново."
                ),
                reply_markup=start_keyboard(user),
            )
            return

        qty = safe_int(info.get("qty", 0), 0)

        if qty < 1 or qty > 50:
            await message.answer(
                "⚠️ Некорректное количество блюда.",
                reply_markup=start_keyboard(user),
            )
            return

        authoritative_price = int(MENU_PRICE_MAP[name])
        client_price = safe_int(
            info.get("price", authoritative_price),
            authoritative_price,
        )

        if client_price != authoritative_price:
            logger.warning(
                (
                    "PRICE TAMPERING BLOCKED: "
                    "user=%s item=%r client=%s server=%s"
                ),
                client_id,
                name,
                client_price,
                authoritative_price,
            )

        item_sum = qty * authoritative_price
        lines.append(f"- {name} ×{qty} = {item_sum} ฿")

        order_items.append(
            {
                "name": name,
                "qty": qty,
                "price": authoritative_price,
                "img": safe_str(info.get("img"), ""),
            }
        )

    items_text = (
        "\n".join(
            lines
        )
        if lines
        else "—"
    )

    items_total = sum(
        max(
            0,
            safe_int(
                item.get(
                    "qty"
                ),
                0,
            ),
        )
        *
        max(
            0,
            safe_int(
                item.get(
                    "price"
                ),
                0,
            ),
        )
        for item
        in order_items
    )

    max_requested_by_order = int(
        items_total * MAX_BONUS_REDEEM_PERCENT / 100
    )
    requested_bonus = min(requested_bonus, max_requested_by_order)

    total = items_total + delivery

    data["itemsTotal"] = items_total
    data["items_total"] = items_total
    data["bonusRequested"] = requested_bonus
    data["bonus_requested"] = requested_bonus
    data["discountPercent"] = 0
    data["discount_percent"] = 0
    data["discountAmount"] = 0
    data["discount_amount"] = 0
    data["discount"] = 0
    data["total"] = total

    order_request_id = safe_str(
        data.get("orderRequestId")
        or data.get("order_request_id")
        or f"tg-{client_id}-{message.message_id}",
        f"tg-{client_id}-{message.message_id}",
    )[:160]

    order_number = ""

    try:
        (
            saved_order_id,
            order_number,
        ) = await save_order_to_database(
            user,
            data,
            order_items,
        )

        data[
            "order_number"
        ] = order_number

        logger.info(
            (
                "Заказ сохранён в БД: "
                "id=%s order_number=%s"
            ),
            saved_order_id,
            order_number,
        )

    except Exception:
        logger.exception(
            "Не удалось сохранить заказ в БД"
        )

        await message.answer(
            (
                "⚠️ Произошла внутренняя ошибка сохранения заказа. "
                "Менеджер уже получил уведомление и свяжется с вами. "
                "Повторно оформлять заказ не нужно."
            ),
            reply_markup=start_keyboard(
                user
            ),
        )

        try:
            await bot.send_message(
                ADMIN_CHAT_ID,
                (
                    "🚨 ЗАКАЗ НЕ СОХРАНИЛСЯ В БАЗУ, "
                    "НО ЕГО НУЖНО ОБРАБОТАТЬ ВРУЧНУЮ!\n\n"
                    f"Telegram ID клиента: {client_id}\n"
                    f"Имя: {data.get('name') or username}\n"
                    f"Телефон: {phone}\n"
                    f"Адрес: {address}\n"
                    f"Оплата: {pay_method}\n"
                    f"Доставка: {delivery} ฿\n"
                    f"Состав заказа:\n{items_text}\n\n"
                    f"Предварительный итог: {total} ฿\n\n"
                    "Клиенту сообщено, что повторять заказ не нужно."
                ),
            )
        except Exception:
            logger.exception(
                "Не удалось сообщить менеджеру об ошибке заказа"
            )

        return

    try:
        loyalty_result = await settle_loyalty_order(
            telegram_id=client_id,
            order_ref=order_request_id,
            items_total=items_total,
            delivery=delivery,
            requested_bonus=requested_bonus,
        )

        bonus_used = max(0, safe_int(loyalty_result.get("bonusUsed"), 0))
        cashback_percent = max(
            0,
            min(100, safe_int(loyalty_result.get("cashbackPercent"), 0)),
        )
        cashback_earned = max(
            0,
            safe_int(loyalty_result.get("cashbackEarned"), 0),
        )
        bonus_balance_after = max(
            0,
            safe_int(loyalty_result.get("balanceAfter"), 0),
        )
        total = max(
            0,
            safe_int(
                loyalty_result.get("total"),
                items_total - bonus_used + delivery,
            ),
        )

        discount_percent = 0
        discount_amount = bonus_used

        data["bonusUsed"] = bonus_used
        data["bonus_used"] = bonus_used
        data["cashbackPercent"] = cashback_percent
        data["cashback_percent"] = cashback_percent
        data["cashbackEarned"] = cashback_earned
        data["cashback_earned"] = cashback_earned
        data["bonusBalanceAfter"] = bonus_balance_after
        data["discountPercent"] = 0
        data["discount_percent"] = 0
        data["discountAmount"] = bonus_used
        data["discount_amount"] = bonus_used
        data["discount"] = bonus_used
        data["total"] = total

        await update_saved_order_loyalty(
            saved_order_id,
            bonus_used,
            cashback_percent,
            cashback_earned,
            total,
            order_request_id,
        )

    except Exception:
        logger.exception("LOYALTY SETTLEMENT ERROR")
        await cancel_saved_order(saved_order_id)

        await message.answer(
            (
                "⚠️ Не удалось безопасно рассчитать бонусы. "
                "Деньги не списаны, заказ отменён. "
                "Попробуйте оформить заказ ещё раз."
            ),
            reply_markup=start_keyboard(user),
        )
        return

    # --------------------------------------------------------
    # НОВОЕ: время приготовления + автоматическая отправка на старые экраны.
    # Ошибка экрана не отменяет уже сохранённый заказ.
    # --------------------------------------------------------
    eta = await calculate_order_eta(data, order_items)
    screen_ok = await send_order_to_screen(
        order_number,
        int(eta["prep_minutes"]),
        order_items,
        data.get("cutlery") if isinstance(data.get("cutlery"), bool) else None,
    )
    if not screen_ok:
        logger.warning("Заказ %s не подтверждён экраном", order_number)
        try:
            await bot.send_message(
                ADMIN_CHAT_ID,
                f"⚠️ Заказ {order_number} принят, но экран кухни не подтвердил получение.",
            )
        except Exception:
            pass

    schedule_receipt_reminder(client_id, order_number, pay_method)

    # --------------------------------------------------------
    # СООБЩЕНИЕ КЛИЕНТУ
    # --------------------------------------------------------

    client_text = (
        f"📦 Ваш заказ {order_number} принят и уже готовится!\n\n"
        f"{eta_client_line(eta)}\n\n"

        f"Имя: "
        f"{data.get('name') or username}\n"

        f"Телефон: "
        f"{phone}\n"

        f"Адрес: "
        f"{address}\n"

        f"Оплата: "
        f"{pay_method}\n"
    )

    if when_str:
        client_text += (
            f"Время: "
            f"{when_str}\n"
        )

    if comment:
        client_text += (
            f"Комментарий: "
            f"{comment}\n"
        )

    client_text += (
        "\n🧾 Состав заказа:\n"

        f"{items_text}\n\n"

        f"Сумма блюд: "
        f"{items_total} ฿\n"
    )

    if bonus_used > 0:
        client_text += (
            f"Использовано бонусов: -{bonus_used} ฿\n"
        )

    if cashback_earned > 0:
        client_text += (
            f"Начислено кэшбэка {cashback_percent}%: "
            f"+{cashback_earned} ฿\n"
            f"Бонусный баланс: {bonus_balance_after} ฿\n"
        )

    client_text += (
        f"Доставка: "
        f"{delivery} ฿\n"

        f"💰 Итого: "
        f"{total} ฿"
    )

    await message.answer(
        client_text,
        reply_markup=start_keyboard(
            user
        ),
    )

    KEYBOARD_SHOWN_USERS.add(
        client_id
    )

    # --------------------------------------------------------
    # СООБЩЕНИЕ МЕНЕДЖЕРУ
    # --------------------------------------------------------

    customer_name = safe_str(
        data.get(
            "name"
        )
        or username
    )

    admin_text = (
        f"✅ <b>Новый заказ {order_number}</b>\n"

        f"• <i>Номер:</i> "
        f"<code>{order_number}</code>\n"

        f"• <i>Пользователь:</i> "
        f"{html.escape(username)}\n"

        f"• <i>User ID:</i> "
        f"<code>{client_id}</code>\n"

        f"• <i>Имя:</i> "
        f"{html.escape(customer_name)}\n"

        f"• <i>Телефон:</i> "
        f"{html.escape(phone)}\n"

        f"• <i>Адрес:</i> "
        f"{html.escape(address)}\n"

        f"• <i>Оплата:</i> "
        f"{html.escape(pay_method)}\n"
    )

    if when_str:
        admin_text += (
            f"• <i>Время заказа:</i> "
            f"{html.escape(when_str)}\n"
        )

    if comment:
        admin_text += (
            f"• <i>Комментарий:</i> "
            f"{html.escape(comment)}\n"
        )

    admin_text += (
        "\n🍽 <b>Состав заказа:</b>\n"

        f"{html.escape(items_text)}\n\n"

        f"• <i>Сумма блюд:</i> "
        f"{items_total} ฿\n"
    )

    if bonus_used > 0:
        admin_text += (
            f"• <i>Использовано бонусов:</i> "
            f"-{bonus_used} ฿\n"
        )

    if cashback_earned > 0:
        admin_text += (
            f"• <i>Начислено кэшбэка:</i> "
            f"{cashback_percent}% (+{cashback_earned} ฿)\n"
        )

    admin_text += (
        f"• <i>Доставка:</i> "
        f"{delivery} ฿\n"

        f"💰 <b>Итого:</b> "
        f"{total} ฿"
    )

    try:
        await send_order_to_admin(
            admin_text,
            client_id,
            saved_order_id,
        )

    except Exception:
        logger.exception(
            (
                "ADMIN send failed окончательно, "
                "даже без profile кнопки"
            )
        )

    # --------------------------------------------------------
    # ДАННЫЕ ДЛЯ ЧЕКОВОЙ ПРОГРАММЫ
    # --------------------------------------------------------

    print_payload = {
        # Единственный номер заказа.
        # Чековая программа должна принять его,
        # сохранить и напечатать без собственного подсчёта.
        "order_number":
            order_number,

        "orderNumber":
            order_number,

        "order_no":
            order_number,

        "orderNo":
            order_number,

        "name": customer_name,

        "phone": phone,

        "address": address,

        "delivery": delivery,

        "payment": pay_method,

        "items": order_items,

        # Сумма блюд до скидки.
        "items_total":
            items_total,

        "itemsTotal":
            items_total,

        "subtotal":
            items_total,

        # Формат чековой программы.
        "discount_percent":
            discount_percent,

        "discount_amount":
            discount_amount,

        # Дополнительные варианты
        # для совместимости.
        "discountPercent":
            discount_percent,

        "discountAmount":
            discount_amount,

        "discount":
            bonus_used,

        "bonus_used":
            bonus_used,

        "bonusUsed":
            bonus_used,

        "used_bonuses":
            bonus_used,

        "cashback_percent":
            cashback_percent,

        "cashback_earned":
            cashback_earned,

        "bonus_balance_after":
            bonus_balance_after,

        # Итог после списания бонусов
        # и с доставкой.
        "total": total,

        "fulfillment_type": normalize_fulfillment(
            data.get("fulfillmentType", data.get("fulfillment_type"))
        ),
        "customer_latitude": payload_coordinates(data)[0],
        "customer_longitude": payload_coordinates(data)[1],
        "prep_minutes": int(eta["prep_minutes"]),
        "travel_minutes": eta.get("travel_minutes"),
        "eta_minutes": eta.get("eta_minutes"),

        "date": datetime.now(
            TIMEZONE
        ).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),

        "order_time":
            when_str,

        "order_when":
            safe_str(
                data.get(
                    "orderWhen"
                )
            ),

        "comment":
            comment,

        "comments":
            comment,

        "comment_text":
            comment,

        "note":
            comment,

        "notes":
            comment,
    }

    logger.info(
        (
            "PRINT PAYLOAD: "
            "order_number=%s "
            "discount_percent=%s "
            "discount_amount=%s "
            "items_total=%s "
            "delivery=%s "
            "total=%s"
        ),
        print_payload[
            "order_number"
        ],
        print_payload[
            "discount_percent"
        ],
        print_payload[
            "discount_amount"
        ],
        print_payload[
            "items_total"
        ],
        print_payload[
            "delivery"
        ],
        print_payload[
            "total"
        ],
    )

    try:
        (
            print_status,
            print_response,
        ) = await send_payload_to_receipt_program(
            print_payload,
            timeout_seconds=7,
        )

        logger.info(
            (
                "Печать отправлена: "
                "HTTP %s, ответ: %s"
            ),
            print_status,
            print_response[:500],
        )

    except Exception as exc:
        logger.exception(
            (
                "PRINT DELIVERY FAILED (NON-FATAL). "
                "Заказ уже сохранён в базе и отправлен менеджеру. "
                "Чек можно дослать кнопкой «🧾 Отправить чек»."
            )
        )

        try:
            await bot.send_message(
                ADMIN_CHAT_ID,
                (
                    f"⚠️ Заказ {order_number} принят, "
                    "но чековая программа сейчас недоступна.\n\n"
                    "Заказ НЕ потерян. Нажмите кнопку "
                    "«🧾 Отправить чек» под заказом после "
                    "восстановления связи.\n\n"
                    f"Ошибка: {safe_str(exc)[:500]}"
                ),
            )
        except Exception:
            logger.exception(
                "Не удалось отправить менеджеру предупреждение о печати"
            )


# ============================================================================
# ЧЕКИ ОПЛАТЫ ОТ КЛИЕНТОВ
# Старый заказной handler не меняется: эти handlers срабатывают только на
# фото/документы обычного клиента.
# ============================================================================

async def handle_customer_receipt_message(message: types.Message) -> None:
    telegram_id = message.from_user.id
    order_number = await latest_promptpay_order_for_user(telegram_id)

    if message.document:
        mime = safe_str(message.document.mime_type, "").lower()
        if mime and mime != "application/pdf" and not mime.startswith("image/"):
            await message.answer("Пожалуйста, пришлите чек фотографией или PDF-файлом.")
            return

    if order_number:
        await mark_receipt_received(order_number)
        if pending_promptpay_by_user.get(telegram_id) == order_number:
            pending_promptpay_by_user.pop(telegram_id, None)

    header = (
        f"🧾 Чек от клиента\n"
        f"Заказ: {order_number or 'не удалось определить'}\n"
        f"Telegram ID: {telegram_id}"
    )

    try:
        await bot.send_message(ADMIN_CHAT_ID, header)
        await bot.copy_message(
            chat_id=ADMIN_CHAT_ID,
            from_chat_id=message.chat.id,
            message_id=message.message_id,
        )
        await message.answer(
            "Спасибо! Чек получил и сразу передал менеджеру."
        )
    except Exception:
        logger.exception("Не удалось переслать чек менеджеру")
        await message.answer(
            "Чек получил, но сейчас не удалось передать его менеджеру автоматически. Попробуйте ещё раз через минуту."
        )


@dp.message(
    F.photo,
    F.from_user.id != ADMIN_CHAT_ID,
)
async def customer_receipt_photo(message: types.Message) -> None:
    await handle_customer_receipt_message(message)


@dp.message(
    F.document,
    F.from_user.id != ADMIN_CHAT_ID,
)
async def customer_receipt_document(message: types.Message) -> None:
    await handle_customer_receipt_message(message)


# ============================================================================
# СООБЩЕНИЯ АДМИНИСТРАТОРА
# ============================================================================

@dp.message(
    F.from_user.id
    == ADMIN_CHAT_ID
)
async def admin_message_router(
    message: types.Message,
) -> None:
    admin_id = message.from_user.id

    if (
        message.text
        and message.text.startswith(
            "/"
        )
    ):
        return

    # --------------------------------------------------------
    # Ссылка отслеживания такси: SM-877 https://...
    # --------------------------------------------------------
    if message.text:
        tracking_match = re.fullmatch(
            r"\s*(SM-\d+)\s+(https?://\S+)\s*",
            message.text,
            flags=re.IGNORECASE,
        )
        if tracking_match:
            order_number = tracking_match.group(1).upper()
            tracking_url = tracking_match.group(2)
            telegram_id = None
            if db_pool:
                row = await db_pool.fetchrow(
                    "SELECT telegram_id FROM orders WHERE order_number=$1 LIMIT 1",
                    order_number,
                )
                if row:
                    telegram_id = int(row["telegram_id"])
                    try:
                        await db_pool.execute(
                            "UPDATE orders SET tracking_url=$2 WHERE order_number=$1",
                            order_number,
                            tracking_url,
                        )
                    except Exception:
                        pass

            if not telegram_id:
                await message.answer(f"⚠️ Заказ {order_number} не найден в базе.")
                return

            try:
                await bot.send_message(
                    telegram_id,
                    (
                        f"🚚 Курьер выехал с заказом {order_number}.\n"
                        f"Следить за доставкой: {tracking_url}"
                    ),
                    disable_web_page_preview=True,
                )
                await message.answer(f"✅ Ссылка отправлена клиенту {order_number}.")
            except Exception as exc:
                logger.exception("Tracking link send failed")
                await message.answer(f"⚠️ Не удалось отправить ссылку: {exc}")
            return

    # --------------------------------------------------------
    # /bonus
    # --------------------------------------------------------

    if admin_id in waiting_bonus:
        state = waiting_bonus[
            admin_id
        ]

        if not message.text:
            await message.answer(
                "Отправь ID и сумму обычным текстом."
            )
            return

        text = message.text.strip()

        parts = text.split()

        if (
            state.get(
                "stage"
            ) == "telegram_id"
            and len(parts) >= 2
        ):
            try:
                telegram_id = int(
                    parts[0]
                )

            except ValueError:
                await message.answer(
                    "Telegram ID должен состоять из цифр."
                )
                return

            amount = parse_money_amount(
                "".join(
                    parts[1:]
                )
            )

            if amount is None:
                await message.answer(
                    (
                        "Сумма указана неправильно.\n"
                        "Пример: 123456789 5000"
                    )
                )
                return

            await process_bonus_amount(
                message,
                telegram_id,
                amount,
            )

            return

        if (
            state.get(
                "stage"
            ) == "telegram_id"
        ):
            try:
                telegram_id = int(
                    text
                )

            except ValueError:
                await message.answer(
                    (
                        "Telegram ID должен состоять из цифр.\n"
                        "Пример: 123456789"
                    )
                )
                return

            state[
                "telegram_id"
            ] = telegram_id

            state[
                "stage"
            ] = "amount"

            known_user = None

            if db_pool:
                known_user = await db_pool.fetchrow(
                    """
                    SELECT
                        telegram_first_name,
                        telegram_last_name,
                        username,
                        manual_spend

                    FROM users

                    WHERE telegram_id = $1
                    """,
                    telegram_id,
                )

            if known_user:
                full_name = " ".join(
                    part
                    for part in (
                        known_user[
                            "telegram_first_name"
                        ],
                        known_user[
                            "telegram_last_name"
                        ],
                    )
                    if part
                ).strip()

                username = (
                    f"@{known_user['username']}"
                    if known_user[
                        "username"
                    ]
                    else "без username"
                )

                current_manual = int(
                    known_user[
                        "manual_spend"
                    ]
                    or 0
                )

                await message.answer(
                    (
                        "✅ Пользователь найден\n\n"

                        f"ID: {telegram_id}\n"

                        f"Имя: "
                        f"{full_name or '-'}\n"

                        f"Username: "
                        f"{username}\n"

                        f"Текущая ручная сумма: "
                        f"{current_manual:,} ฿\n\n"

                        "Теперь отправь новую сумму.\n"

                        "Пример: 5000"
                    ).replace(
                        ",",
                        " ",
                    )
                )

            else:
                await message.answer(
                    (
                        f"ID принят: {telegram_id}\n\n"

                        "Пользователь в базе бота пока "
                        "не найден, но сумма будет записана "
                        "в его личный кабинет.\n\n"

                        "Теперь отправь сумму.\n"

                        "Пример: 5000"
                    )
                )

            return

        if (
            state.get(
                "stage"
            ) == "amount"
        ):
            amount = parse_money_amount(
                text
            )

            if amount is None:
                await message.answer(
                    (
                        "Сумма указана неправильно.\n"

                        "Отправь целое число от 0 "
                        "до 10 000 000.\n"

                        "Пример: 5000"
                    )
                )
                return

            telegram_id = int(
                state[
                    "telegram_id"
                ]
            )

            await process_bonus_amount(
                message,
                telegram_id,
                amount,
            )

            return

    # --------------------------------------------------------
    # Ответ клиенту
    # --------------------------------------------------------

    if admin_id in waiting_reply:
        if not message.text:
            await message.answer(
                "Для ответа клиенту отправь текст."
            )
            return

        client_id = waiting_reply.pop(
            admin_id
        )[
            "client_id"
        ]

        try:
            await bot.send_message(
                client_id,
                (
                    "💬 Сообщение от менеджера:\n\n"
                    f"{message.text}"
                ),
            )

            await message.answer(
                "✅ Отправлено клиенту."
            )

            await mark_send_success(
                client_id,
                "direct",
            )

        except Exception as exc:
            blocked = is_blocking_error(
                exc
            )

            await mark_send_error(
                client_id,
                str(exc),
                blocked,
            )

            logger.exception(
                "Не удалось отправить клиенту %s",
                client_id,
            )

            await message.answer(
                (
                    "⚠️ Не получилось отправить. "
                    "Клиент мог заблокировать бота."
                )
            )

        return

    # --------------------------------------------------------
    # Рекламная рассылка
    # --------------------------------------------------------

    if admin_id in waiting_broadcast:
        waiting_broadcast.discard(
            admin_id
        )

        pending_broadcasts[
            admin_id
        ] = {
            "source_chat_id":
                message.chat.id,

            "source_message_id":
                message.message_id,
        }

        target_count = (
            await get_broadcast_target_count(
                "advertising"
            )
        )

        try:
            await bot.copy_message(
                chat_id=ADMIN_CHAT_ID,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
                reply_markup=(
                    build_broadcast_confirm_keyboard()
                ),
            )

            await message.answer(
                (
                    "👆 Предпросмотр рекламного сообщения.\n\n"

                    f"Получателей: {target_count}\n\n"

                    "Проверь сообщение и нажми кнопку "
                    "под предпросмотром."
                )
            )

        except Exception as exc:
            pending_broadcasts.pop(
                admin_id,
                None,
            )

            logger.exception(
                "Не удалось создать предпросмотр рассылки"
            )

            await message.answer(
                (
                    "⚠️ Не удалось подготовить сообщение.\n"
                    f"Ошибка: {exc}"
                )
            )

        return


# ============================================================================
# ОБЫЧНЫЕ СООБЩЕНИЯ
# ============================================================================

@dp.message()
async def ensure_keyboard_if_missing(
    message: types.Message,
) -> None:
    if (
        message.content_type
        == ContentType.WEB_APP_DATA
    ):
        return

    if message.text in (
        ASK_BTN_TEXT,
        MENU_BTN_TEXT,
    ):
        return

    await send_main_keyboard(
        message,
        "Выберите действие 👇",
        force=False,
    )


# ============================================================================
# ЗАПУСК
# ============================================================================

async def main() -> None:
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()

    logger.info(
        "=== Запуск бота Smoke Factory BBQ ==="
    )

    logger.info(
        "WEBAPP_URL=%s",
        WEBAPP_URL,
    )

    try:
        await bot.delete_webhook(
            drop_pending_updates=True
        )

    except Exception as exc:
        logger.error(
            "delete_webhook error: %s",
            exc,
        )

    await init_database()

    run_fake_server(
        PORT
    )

    schedule_restart()

    logger.info(
        "Бот запущен и готов сохранять пользователей"
    )

    try:
        await dp.start_polling(
            bot
        )

    finally:
        if db_pool:
            await db_pool.close()

        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(
        main()
    )
