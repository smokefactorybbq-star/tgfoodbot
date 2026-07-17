import os
import sys
import csv
import io
import json
import html
import asyncio
import logging
import threading

from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Optional
from zoneinfo import ZoneInfo

import aiohttp
import asyncpg

from aiogram import Bot, Dispatcher, F, types
from aiogram.dispatcher.middlewares.base import BaseMiddleware
from aiogram.enums import ContentType
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
)
from aiogram.filters import Command
from aiogram.types import (
    BotCommand,
    BotCommandScopeChat,
    BufferedInputFile,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder


# ============================================================
# НАСТРОЙКИ
# ============================================================

# Вставь сюда существующий токен бота.
TELEGRAM_BOT_TOKEN_IN_CODE = "ВСТАВЬ_ТОКЕН_БОТА"

# Вставь DATABASE_URL PostgreSQL именно из проекта БОТА.
DATABASE_URL_IN_CODE = "ВСТАВЬ_DATABASE_URL_БОТА"

ADMIN_CHAT_ID_IN_CODE = 7309681026

WEBAPP_URL_IN_CODE = (
    "https://mini-app-production-67f2.up.railway.app"
)

MANAGER_URL_IN_CODE = (
    "https://t.me/SmokefactoryBBQ"
)

# Вставь актуальный адрес программы печати.
PRINT_URL_IN_CODE = (
    "https://ВСТАВЬ-АКТУАЛЬНЫЙ-NGROK/order"
)

# Пауза между сообщениями.
# 0.06 — примерно 16 сообщений в секунду.
BROADCAST_DELAY_IN_CODE = 0.06

# Через сколько минут перезапускать бот.
# 0 — отключить автоматический перезапуск.
RESTART_MINUTES_IN_CODE = 420


def get_setting(name: str, fallback: Any) -> Any:
    """
    Сначала берём Railway Variable.
    Если её нет — используем значение из bot.py.
    """
    value = os.getenv(name)

    if value is None:
        return fallback

    if isinstance(value, str) and not value.strip():
        return fallback

    return value


API_TOKEN = str(
    get_setting(
        "TELEGRAM_BOT_TOKEN",
        TELEGRAM_BOT_TOKEN_IN_CODE,
    )
).strip()

DATABASE_URL = str(
    get_setting(
        "DATABASE_URL",
        DATABASE_URL_IN_CODE,
    )
).strip()

ADMIN_CHAT_ID = int(
    get_setting(
        "ADMIN_CHAT_ID",
        ADMIN_CHAT_ID_IN_CODE,
    )
)

WEBAPP_URL = str(
    get_setting(
        "WEBAPP_URL",
        WEBAPP_URL_IN_CODE,
    )
).strip()

MANAGER_URL = str(
    get_setting(
        "MANAGER_URL",
        MANAGER_URL_IN_CODE,
    )
).strip()

PRINT_URL = str(
    get_setting(
        "PRINT_URL",
        PRINT_URL_IN_CODE,
    )
).strip()

BROADCAST_DELAY = float(
    get_setting(
        "BROADCAST_DELAY",
        BROADCAST_DELAY_IN_CODE,
    )
)

RESTART_MINUTES = int(
    get_setting(
        "RESTART_MINUTES",
        RESTART_MINUTES_IN_CODE,
    )
)

PORT = int(
    get_setting(
        "PORT",
        8080,
    )
)


if (
    not API_TOKEN
    or API_TOKEN == "ВСТАВЬ_ТОКЕН_БОТА"
):
    print(
        "ERROR: не установлен токен Telegram-бота",
        flush=True,
    )
    sys.exit(1)


if (
    not DATABASE_URL
    or DATABASE_URL == "ВСТАВЬ_DATABASE_URL_БОТА"
):
    print(
        "ERROR: не установлен DATABASE_URL",
        flush=True,
    )
    sys.exit(1)


MENU_BTN_TEXT = "📋 Открыть меню"
ASK_BTN_TEXT = "💬 Задать вопрос менеджеру"

TIMEZONE = ZoneInfo("Asia/Bangkok")


# ============================================================
# ЛОГИРОВАНИЕ
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s "
        "[%(levelname)s] "
        "%(message)s"
    ),
    handlers=[
        logging.StreamHandler(sys.stdout)
    ],
)

logger = logging.getLogger(
    "smoke_factory_bot"
)


# ============================================================
# БОТ И СОСТОЯНИЯ
# ============================================================

bot = Bot(
    token=API_TOKEN
)

dp = Dispatcher()

db_pool: Optional[asyncpg.Pool] = None

keyboard_shown_users: set[int] = set()

# Состояния администратора.
admin_state: dict[int, dict[str, Any]] = {}

# Сообщение, подготовленное для рассылки.
pending_broadcast: dict[str, Any] = {}

broadcast_lock = asyncio.Lock()

broadcast_running = False


# ============================================================
# БАЗА ДАННЫХ
# ============================================================

async def init_database() -> None:
    global db_pool

    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=8,
        command_timeout=60,
    )

    async with db_pool.acquire() as connection:
        await connection.execute(
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

                created_at TIMESTAMPTZ
                    NOT NULL DEFAULT NOW(),

                updated_at TIMESTAMPTZ
                    NOT NULL DEFAULT NOW(),

                last_bot_activity_at TIMESTAMPTZ,
                last_site_visit_at TIMESTAMPTZ
            );


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS
                is_active BOOLEAN
                NOT NULL DEFAULT TRUE;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS
                marketing_allowed BOOLEAN
                NOT NULL DEFAULT TRUE;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS
                blocked_at TIMESTAMPTZ;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS
                last_send_error TEXT;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS
                last_successful_send_at TIMESTAMPTZ;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS
                last_broadcast_at TIMESTAMPTZ;


            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS
                last_keyboard_sent_at TIMESTAMPTZ;


            CREATE INDEX IF NOT EXISTS
                idx_users_active
            ON users(is_active);


            CREATE INDEX IF NOT EXISTS
                idx_users_marketing
            ON users(marketing_allowed);


            CREATE INDEX IF NOT EXISTS
                idx_users_bot_activity
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


            CREATE INDEX IF NOT EXISTS
                idx_visits_date
            ON visits(visited_at);


            CREATE TABLE IF NOT EXISTS orders (
                id BIGSERIAL PRIMARY KEY,

                telegram_id BIGINT
                    NOT NULL
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


            CREATE INDEX IF NOT EXISTS
                idx_orders_date
            ON orders(created_at);


            CREATE TABLE IF NOT EXISTS order_items (
                id BIGSERIAL PRIMARY KEY,

                order_id BIGINT
                    NOT NULL
                    REFERENCES orders(id)
                    ON DELETE CASCADE,

                item_name TEXT
                    NOT NULL,

                quantity INTEGER
                    NOT NULL,

                unit_price INTEGER
                    NOT NULL,

                image_url TEXT
            );


            CREATE TABLE IF NOT EXISTS broadcast_logs (
                id BIGSERIAL PRIMARY KEY,

                kind TEXT
                    NOT NULL,

                created_by BIGINT
                    NOT NULL,

                source_chat_id BIGINT,
                source_message_id BIGINT,

                total_targets INTEGER
                    NOT NULL DEFAULT 0,

                delivered INTEGER
                    NOT NULL DEFAULT 0,

                failed INTEGER
                    NOT NULL DEFAULT 0,

                blocked INTEGER
                    NOT NULL DEFAULT 0,

                status TEXT
                    NOT NULL DEFAULT 'created',

                created_at TIMESTAMPTZ
                    NOT NULL DEFAULT NOW(),

                completed_at TIMESTAMPTZ
            );


            CREATE INDEX IF NOT EXISTS
                idx_broadcast_logs_date
            ON broadcast_logs(created_at DESC);
            """
        )

        # Если Railway перезапустился посреди рассылки.
        await connection.execute(
            """
            UPDATE broadcast_logs
            SET
                status = 'interrupted',
                completed_at = NOW()
            WHERE status = 'running'
            """
        )

    logger.info(
        "DATABASE READY: таблицы готовы"
    )


async def upsert_user(
    user: Optional[types.User],
) -> Optional[asyncpg.Record]:
    """
    Сохраняет пользователя в базу бота.
    """

    if not db_pool:
        logger.error(
            "USER NOT SAVED: база не подключена"
        )
        return None

    if not user:
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
                username =
                    EXCLUDED.username,

                telegram_first_name =
                    EXCLUDED.telegram_first_name,

                telegram_last_name =
                    EXCLUDED.telegram_last_name,

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
                last_bot_activity_at
            """,
            user.id,
            user.username,
            user.first_name,
            user.last_name,
        )

        logger.info(
            "USER SAVED: "
            "id=%s "
            "username=@%s "
            "name=%s %s",
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
    kind: str,
) -> None:
    if not db_pool:
        return

    if kind == "marketing":
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

    else:
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
            is_active =
                CASE
                    WHEN $3
                    THEN FALSE
                    ELSE is_active
                END,

            blocked_at =
                CASE
                    WHEN $3
                    THEN NOW()
                    ELSE blocked_at
                END,

            last_send_error =
                LEFT($2, 1000),

            updated_at = NOW()

        WHERE telegram_id = $1
        """,
        telegram_id,
        error_text,
        deactivate,
    )


# ============================================================
# АВТОМАТИЧЕСКОЕ СОХРАНЕНИЕ ПОЛЬЗОВАТЕЛЯ
# ============================================================

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
            await upsert_user(user)

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


# ============================================================
# КЛАВИАТУРЫ
# ============================================================

def main_keyboard() -> (
    types.ReplyKeyboardMarkup
):
    menu_button = types.KeyboardButton(
        text=MENU_BTN_TEXT,
        web_app=types.WebAppInfo(
            url=WEBAPP_URL
        ),
    )

    manager_button = (
        types.KeyboardButton(
            text=ASK_BTN_TEXT
        )
    )

    return types.ReplyKeyboardMarkup(
        keyboard=[
            [menu_button],
            [manager_button],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def start_keyboard() -> (
    types.ReplyKeyboardMarkup
):
    return main_keyboard()


def updated_keyboard() -> (
    types.ReplyKeyboardMarkup
):
    return main_keyboard()


async def send_main_keyboard(
    message: types.Message,
    text: str,
    force: bool = False,
) -> bool:
    user_id = message.from_user.id

    if (
        user_id
        not in keyboard_shown_users
        or force
    ):
        await message.answer(
            text,
            reply_markup=main_keyboard(),
        )

        keyboard_shown_users.add(
            user_id
        )

        return True

    return False


def unsubscribe_keyboard() -> (
    types.InlineKeyboardMarkup
):
    builder = InlineKeyboardBuilder()

    builder.button(
        text="🔕 Не получать рекламу",
        callback_data=(
            "marketing_unsubscribe"
        ),
    )

    return builder.as_markup()


def broadcast_confirm_keyboard() -> (
    types.InlineKeyboardMarkup
):
    builder = InlineKeyboardBuilder()

    builder.button(
        text="✅ Начать рассылку",
        callback_data=(
            "broadcast_confirm"
        ),
    )

    builder.button(
        text="❌ Отмена",
        callback_data=(
            "broadcast_cancel"
        ),
    )

    builder.adjust(1)

    return builder.as_markup()


def keyboard_confirm_keyboard() -> (
    types.InlineKeyboardMarkup
):
    builder = InlineKeyboardBuilder()

    builder.button(
        text=(
            "✅ Разослать новую клавиатуру"
        ),
        callback_data=(
            "keyboard_confirm"
        ),
    )

    builder.button(
        text="❌ Отмена",
        callback_data=(
            "keyboard_cancel"
        ),
    )

    builder.adjust(1)

    return builder.as_markup()


def manager_order_keyboard(
    client_id: int,
    include_profile: bool = True,
) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    if include_profile:
        builder.button(
            text=(
                "👤 Открыть профиль клиента"
            ),
            url=(
                f"tg://user?id={client_id}"
            ),
        )

    builder.button(
        text="✍️ Написать клиенту",
        callback_data=(
            f"write_client:{client_id}"
        ),
    )

    builder.adjust(1)

    return builder.as_markup()


# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def safe_int(
    value: Any,
    default: int = 0,
) -> int:
    try:
        return int(value)

    except (
        TypeError,
        ValueError,
    ):
        return default


def safe_str(
    value: Any,
    default: str = "",
) -> str:
    if value is None:
        return default

    try:
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


def is_block_error(
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

    phrases = (
        "bot was blocked",
        "chat not found",
        "user is deactivated",
        "bot was kicked",
    )

    return any(
        phrase in error_text
        for phrase in phrases
    )


# ============================================================
# СОХРАНЕНИЕ ЗАКАЗА
# ============================================================

async def save_order_to_database(
    user: types.User,
    data: dict[str, Any],
    order_items: list[dict[str, Any]],
) -> Optional[int]:
    if not db_pool:
        return None

    await upsert_user(user)

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
                "discount",
                data.get(
                    "discountAmount",
                    0,
                ),
            )
        ),
    )

    total = max(
        0,
        safe_int(
            data.get(
                "total",
                (
                    items_total
                    + delivery
                    - discount_amount
                ),
            )
        ),
    )

    order_date = None

    raw_order_date = data.get(
        "orderDate"
    )

    if raw_order_date:
        try:
            order_date = (
                datetime.strptime(
                    str(
                        raw_order_date
                    ),
                    "%Y-%m-%d",
                ).date()
            )

        except ValueError:
            order_date = None

    async with db_pool.acquire() as connection:
        async with connection.transaction():
            order_id = await connection.fetchval(
                """
                INSERT INTO orders (
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
                    comment
                )
                VALUES (
                    $1,$2,$3,$4,$5,
                    $6,$7,$8,$9,$10,
                    $11,$12,$13,$14,$15
                )
                RETURNING id
                """,
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
                    or data.get("comments")
                    or data.get("note")
                ),
            )

            if order_items:
                await connection.executemany(
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

    return int(order_id)


# ============================================================
# СТАТИСТИКА
# ============================================================

async def build_daily_report() -> str:
    if not db_pool:
        return (
            "База данных не подключена."
        )

    async with db_pool.acquire() as connection:
        row = await connection.fetchrow(
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
                        AND
                        marketing_allowed = TRUE
                ) AS marketing_users,

                (
                    SELECT COUNT(*)
                    FROM users
                    WHERE is_active = FALSE
                ) AS inactive_users,

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
                ) AS bot_users_today,

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
                ) AS revenue
            """
        )

    today = datetime.now(
        TIMEZONE
    ).strftime(
        "%d.%m.%Y"
    )

    return (
        f"📊 Статистика за {today}\n\n"

        f"👥 Всего ID в базе бота: "
        f"{int(row['total_users'] or 0)}\n"

        f"✅ Активных: "
        f"{int(row['active_users'] or 0)}\n"

        f"📣 Для рекламной рассылки: "
        f"{int(row['marketing_users'] or 0)}\n"

        f"🚫 Недоступны: "
        f"{int(row['inactive_users'] or 0)}\n"

        f"💬 Активность в боте сегодня: "
        f"{int(row['bot_users_today'] or 0)}\n"

        f"🆕 Новых сегодня: "
        f"{int(row['new_users'] or 0)}\n\n"

        f"🌐 Открытий сайта в этой базе: "
        f"{int(row['visits'] or 0)}\n"

        f"🧾 Заказов: "
        f"{int(row['orders_count'] or 0)}\n"

        f"💰 Выручка: "
        f"{int(row['revenue'] or 0)} ฿"
    )


# ============================================================
# ОТПРАВКА ЗАКАЗА МЕНЕДЖЕРУ
# ============================================================

async def send_order_to_admin(
    admin_text_html: str,
    client_id: int,
) -> None:
    try:
        await bot.send_message(
            ADMIN_CHAT_ID,
            admin_text_html,
            parse_mode="HTML",
            reply_markup=(
                manager_order_keyboard(
                    client_id,
                    include_profile=True,
                )
            ),
        )

    except TelegramBadRequest as error:
        if (
            "BUTTON_USER_PRIVACY_RESTRICTED"
            not in str(error)
        ):
            raise

        await bot.send_message(
            ADMIN_CHAT_ID,
            admin_text_html,
            parse_mode="HTML",
            reply_markup=(
                manager_order_keyboard(
                    client_id,
                    include_profile=False,
                )
            ),
        )


# ============================================================
# РАССЫЛКИ
# ============================================================

async def get_target_count(
    kind: str,
) -> int:
    if not db_pool:
        return 0

    if kind == "marketing":
        count = await db_pool.fetchval(
            """
            SELECT COUNT(*)
            FROM users
            WHERE
                is_active = TRUE
                AND
                marketing_allowed = TRUE
                AND
                telegram_id <> $1
            """,
            ADMIN_CHAT_ID,
        )

    else:
        count = await db_pool.fetchval(
            """
            SELECT COUNT(*)
            FROM users
            WHERE
                is_active = TRUE
                AND
                telegram_id <> $1
            """,
            ADMIN_CHAT_ID,
        )

    return int(
        count or 0
    )


async def get_targets(
    kind: str,
) -> list[asyncpg.Record]:
    if not db_pool:
        return []

    if kind == "marketing":
        return await db_pool.fetch(
            """
            SELECT telegram_id
            FROM users
            WHERE
                is_active = TRUE
                AND
                marketing_allowed = TRUE
                AND
                telegram_id <> $1
            ORDER BY telegram_id
            """,
            ADMIN_CHAT_ID,
        )

    return await db_pool.fetch(
        """
        SELECT telegram_id
        FROM users
        WHERE
            is_active = TRUE
            AND
            telegram_id <> $1
        ORDER BY telegram_id
        """,
        ADMIN_CHAT_ID,
    )


async def create_broadcast_log(
    kind: str,
    source_chat_id: Optional[int],
    source_message_id: Optional[int],
    total_targets: int,
) -> int:
    if not db_pool:
        return 0

    log_id = await db_pool.fetchval(
        """
        INSERT INTO broadcast_logs (
            kind,
            created_by,
            source_chat_id,
            source_message_id,
            total_targets,
            status
        )
        VALUES (
            $1,$2,$3,$4,$5,'running'
        )
        RETURNING id
        """,
        kind,
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
    failed: int,
    blocked: int,
    status: str,
) -> None:
    if not db_pool:
        return

    if not log_id:
        return

    await db_pool.execute(
        """
        UPDATE broadcast_logs
        SET
            delivered = $2,
            failed = $3,
            blocked = $4,
            status = $5,
            completed_at = NOW()
        WHERE id = $1
        """,
        log_id,
        delivered,
        failed,
        blocked,
        status,
    )


async def send_marketing_to_user(
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
                unsubscribe_keyboard()
            ),
        )

        await mark_send_success(
            telegram_id,
            "marketing",
        )

        return "delivered"

    except TelegramRetryAfter as error:
        await asyncio.sleep(
            float(
                error.retry_after
            )
            + 1
        )

        return await send_marketing_to_user(
            telegram_id,
            source_chat_id,
            source_message_id,
        )

    except Exception as error:
        blocked = is_block_error(
            error
        )

        await mark_send_error(
            telegram_id,
            str(error),
            blocked,
        )

        if blocked:
            return "blocked"

        return "failed"


async def send_keyboard_to_user(
    telegram_id: int,
) -> str:
    try:
        await bot.send_message(
            telegram_id,
            (
                "🔄 Меню Smoke Factory BBQ "
                "обновлено.\n"
                "Используйте новую кнопку ниже 👇"
            ),
            reply_markup=main_keyboard(),
        )

        await mark_send_success(
            telegram_id,
            "keyboard",
        )

        return "delivered"

    except TelegramRetryAfter as error:
        await asyncio.sleep(
            float(
                error.retry_after
            )
            + 1
        )

        return await send_keyboard_to_user(
            telegram_id
        )

    except Exception as error:
        blocked = is_block_error(
            error
        )

        await mark_send_error(
            telegram_id,
            str(error),
            blocked,
        )

        if blocked:
            return "blocked"

        return "failed"


async def run_mass_delivery(
    kind: str,
    source_chat_id: Optional[int] = None,
    source_message_id: Optional[int] = None,
) -> None:
    global broadcast_running

    if broadcast_lock.locked():
        raise RuntimeError(
            "Другая рассылка уже выполняется"
        )

    async with broadcast_lock:
        broadcast_running = True

        targets = await get_targets(
            kind
        )

        total = len(
            targets
        )

        log_id = await create_broadcast_log(
            kind,
            source_chat_id,
            source_message_id,
            total,
        )

        delivered = 0
        blocked = 0
        failed = 0

        progress_message = (
            await bot.send_message(
                ADMIN_CHAT_ID,
                (
                    "🚀 Рассылка началась\n"
                    f"Получателей: {total}\n"
                    "Обработано: 0"
                ),
            )
        )

        try:
            for index, row in enumerate(
                targets,
                start=1,
            ):
                telegram_id = int(
                    row["telegram_id"]
                )

                if kind == "marketing":
                    if (
                        source_chat_id
                        is None
                        or
                        source_message_id
                        is None
                    ):
                        raise RuntimeError(
                            "Нет исходного сообщения"
                        )

                    result = (
                        await send_marketing_to_user(
                            telegram_id,
                            source_chat_id,
                            source_message_id,
                        )
                    )

                else:
                    result = (
                        await send_keyboard_to_user(
                            telegram_id
                        )
                    )

                if result == "delivered":
                    delivered += 1

                elif result == "blocked":
                    blocked += 1

                else:
                    failed += 1

                if (
                    index % 25 == 0
                    or
                    index == total
                ):
                    try:
                        await bot.edit_message_text(
                            chat_id=(
                                ADMIN_CHAT_ID
                            ),
                            message_id=(
                                progress_message
                                .message_id
                            ),
                            text=(
                                "🚀 Рассылка выполняется\n"
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
                    max(
                        0.03,
                        BROADCAST_DELAY,
                    )
                )

            await finish_broadcast_log(
                log_id,
                delivered,
                failed,
                blocked,
                "completed",
            )

            final_text = (
                "✅ Рассылка завершена\n\n"
                f"Всего: {total}\n"
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
                    text=final_text,
                )

            except TelegramBadRequest:
                await bot.send_message(
                    ADMIN_CHAT_ID,
                    final_text,
                )

        except Exception:
            logger.exception(
                "BROADCAST ERROR"
            )

            await finish_broadcast_log(
                log_id,
                delivered,
                failed,
                blocked,
                "failed",
            )

            raise

        finally:
            broadcast_running = False


# ============================================================
# ПОЛЬЗОВАТЕЛЬСКИЕ КОМАНДЫ
# ============================================================

@dp.message(
    Command("start")
)
async def cmd_start(
    message: types.Message,
) -> None:
    await upsert_user(
        message.from_user
    )

    await message.answer(
        (
            "Добро пожаловать "
            "в Smoke Factory BBQ!\n\n"
            "Откройте меню кнопкой ниже.\n\n"
            "Рекламные сообщения можно "
            "отключить командой /stop."
        ),
        reply_markup=main_keyboard(),
    )

    keyboard_shown_users.add(
        message.from_user.id
    )


@dp.message(
    Command("stop")
)
async def cmd_stop_marketing(
    message: types.Message,
) -> None:
    await set_marketing_allowed(
        message.from_user.id,
        False,
    )

    await message.answer(
        (
            "🔕 Рекламные сообщения "
            "отключены.\n\n"
            "Включить снова: /ads_on"
        ),
        reply_markup=main_keyboard(),
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
        reply_markup=main_keyboard(),
    )


@dp.callback_query(
    F.data
    == "marketing_unsubscribe"
)
async def unsubscribe_callback(
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


# Эта функция принимает нажатие старой
# текстовой кнопки и присылает новую.
@dp.message(
    F.text == MENU_BTN_TEXT
)
async def refresh_old_keyboard(
    message: types.Message,
) -> None:
    await message.answer(
        (
            "✅ Меню обновлено.\n"
            "Используйте новую кнопку ниже."
        ),
        reply_markup=main_keyboard(),
    )

    keyboard_shown_users.add(
        message.from_user.id
    )


@dp.message(
    F.text == ASK_BTN_TEXT
)
async def open_manager_chat(
    message: types.Message,
) -> None:
    builder = InlineKeyboardBuilder()

    builder.button(
        text="👉 Открыть чат менеджера",
        url=MANAGER_URL,
    )

    builder.button(
        text="⬅️ Вернуть меню",
        callback_data="back_to_menu",
    )

    builder.adjust(1)

    await message.answer(
        "Откройте чат менеджера 👇",
        reply_markup=builder.as_markup(),
    )


@dp.callback_query(
    F.data == "back_to_menu"
)
async def back_to_menu(
    call: types.CallbackQuery,
) -> None:
    await call.message.answer(
        "Возвращаю меню 👇",
        reply_markup=main_keyboard(),
    )

    keyboard_shown_users.add(
        call.from_user.id
    )

    await call.answer()


# ============================================================
# АДМИНИСТРАТИВНЫЕ КОМАНДЫ
# ============================================================

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

            "/nu4etam — статистика\n"

            "/users — пользователи\n"

            "/checkuser ID — проверить ID\n"

            "/export_users — выгрузить CSV\n"

            "/broadcast — реклама\n"

            "/broadcast_status — "
            "последняя рассылка\n"

            "/update_keyboard — "
            "обновить клавиатуру всем\n"

            "/keyboard_test — "
            "проверить клавиатуру на себе\n"

            "/cancel — отмена"
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
            "REPORT ERROR"
        )

        await message.answer(
            "⚠️ Ошибка статистики."
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
                    AND
                    marketing_allowed = TRUE
            ) AS marketing,

            COUNT(*) FILTER (
                WHERE
                    marketing_allowed = FALSE
            ) AS unsubscribed,

            COUNT(*) FILTER (
                WHERE
                    is_active = FALSE
            ) AS blocked

        FROM users
        """
    )

    users = await db_pool.fetch(
        """
        SELECT
            telegram_id,
            username,
            telegram_first_name,
            telegram_last_name,
            marketing_allowed,
            is_active,
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
        (
            "Всего ID: "
            f"{int(stats['total'] or 0)}"
        ),
        (
            "Активных: "
            f"{int(stats['active'] or 0)}"
        ),
        (
            "Для рекламы: "
            f"{int(stats['marketing'] or 0)}"
        ),
        (
            "Отказались от рекламы: "
            f"{int(stats['unsubscribed'] or 0)}"
        ),
        (
            "Недоступны: "
            f"{int(stats['blocked'] or 0)}"
        ),
        "",
        "Последние 15:",
    ]

    for user in users:
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

        lines.append(
            (
                f"{active_icon}{ads_icon} "
                f"{user['telegram_id']} — "
                f"{full_name or 'Без имени'} — "
                f"{username}"
            )
        )

    await message.answer(
        "\n".join(lines)[:4096]
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
        message.text or ""
    ).split(
        maxsplit=1
    )

    if len(parts) != 2:
        await message.answer(
            (
                "Использование:\n"
                "/checkuser 123456789"
            )
        )
        return

    try:
        telegram_id = int(
            parts[1].strip()
        )

    except ValueError:
        await message.answer(
            "ID должен состоять из цифр."
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
            (
                "❌ Пользователь "
                f"{telegram_id} "
                "не найден."
            )
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

            f"Активен: "
            f"{'да' if user['is_active'] else 'нет'}\n"

            f"Реклама: "
            f"{'да' if user['marketing_allowed'] else 'нет'}\n"

            f"Создан: "
            f"{user['created_at']}\n"

            f"Последняя активность: "
            f"{user['last_bot_activity_at']}\n"

            f"Последняя отправка: "
            f"{user['last_successful_send_at'] or '-'}\n"

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
            is_active,
            marketing_allowed,
            created_at,
            last_bot_activity_at,
            last_broadcast_at,
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
        "is_active",
        "marketing_allowed",
        "created_at",
        "last_bot_activity_at",
        "last_broadcast_at",
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

    file_name = (
        "smoke_factory_users_"
        + datetime.now(
            TIMEZONE
        ).strftime(
            "%Y%m%d_%H%M"
        )
        + ".csv"
    )

    document = BufferedInputFile(
        output.getvalue().encode(
            "utf-8-sig"
        ),
        filename=file_name,
    )

    await message.answer_document(
        document,
        caption=(
            f"Пользователей: {len(users)}"
        ),
    )


# ============================================================
# СОЗДАНИЕ РЕКЛАМНОЙ РАССЫЛКИ
# ============================================================

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

    if broadcast_lock.locked():
        await message.answer(
            (
                "⚠️ Другая рассылка "
                "уже выполняется."
            )
        )
        return

    admin_state[
        ADMIN_CHAT_ID
    ] = {
        "mode": "broadcast"
    }

    await message.answer(
        (
            "📣 Пришли следующим сообщением "
            "рекламу.\n\n"

            "Можно отправить текст, фото, "
            "видео или документ.\n"

            "После этого появится "
            "предпросмотр.\n\n"

            "Отмена: /cancel"
        )
    )


@dp.callback_query(
    F.data == "broadcast_confirm"
)
async def confirm_broadcast(
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

    source_chat_id = (
        pending_broadcast.get(
            "source_chat_id"
        )
    )

    source_message_id = (
        pending_broadcast.get(
            "source_message_id"
        )
    )

    if (
        not source_chat_id
        or
        not source_message_id
    ):
        await call.answer(
            (
                "Предпросмотр устарел. "
                "Создай рассылку заново."
            ),
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

    try:
        await run_mass_delivery(
            "marketing",
            int(source_chat_id),
            int(source_message_id),
        )

    except Exception as error:
        await bot.send_message(
            ADMIN_CHAT_ID,
            (
                "⚠️ Рассылка остановлена:\n"
                f"{error}"
            ),
        )

    finally:
        pending_broadcast.clear()


@dp.callback_query(
    F.data == "broadcast_cancel"
)
async def cancel_broadcast_callback(
    call: types.CallbackQuery,
) -> None:
    if not is_admin(
        call.from_user.id
    ):
        return

    pending_broadcast.clear()

    admin_state.pop(
        ADMIN_CHAT_ID,
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
    Command("broadcast_status")
)
async def cmd_broadcast_status(
    message: types.Message,
) -> None:
    if not is_admin(
        message.from_user.id
    ):
        return

    if not db_pool:
        return

    row = await db_pool.fetchrow(
        """
        SELECT *
        FROM broadcast_logs
        ORDER BY id DESC
        LIMIT 1
        """
    )

    if not row:
        await message.answer(
            "Рассылок пока не было."
        )
        return

    await message.answer(
        (
            "📊 Последняя рассылка\n\n"

            f"ID: {row['id']}\n"

            f"Тип: {row['kind']}\n"

            f"Статус: {row['status']}\n"

            f"Получателей: "
            f"{row['total_targets']}\n"

            f"Доставлено: "
            f"{row['delivered']}\n"

            f"Недоступны: "
            f"{row['blocked']}\n"

            f"Другие ошибки: "
            f"{row['failed']}\n"

            f"Начата: "
            f"{row['created_at']}\n"

            f"Завершена: "
            f"{row['completed_at'] or '-'}"
        )
    )


# ============================================================
# МАССОВОЕ ОБНОВЛЕНИЕ КЛАВИАТУРЫ
# ============================================================

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

    if broadcast_lock.locked():
        await message.answer(
            (
                "⚠️ Другая рассылка "
                "уже выполняется."
            )
        )
        return

    total = await get_target_count(
        "keyboard"
    )

    await message.answer(
        (
            "🔄 Обновление клавиатуры\n\n"

            f"Получателей: {total}\n\n"

            "Клавиатуру получат все "
            "активные пользователи, "
            "даже отключившие рекламу."
        ),
        reply_markup=(
            keyboard_confirm_keyboard()
        ),
    )


@dp.callback_query(
    F.data == "keyboard_confirm"
)
async def confirm_keyboard_update(
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

    await call.answer(
        "Обновление запущено"
    )

    try:
        await call.message.edit_reply_markup(
            reply_markup=None
        )

    except TelegramBadRequest:
        pass

    try:
        await run_mass_delivery(
            "keyboard"
        )

    except Exception as error:
        await bot.send_message(
            ADMIN_CHAT_ID,
            (
                "⚠️ Отправка остановлена:\n"
                f"{error}"
            ),
        )


@dp.callback_query(
    F.data == "keyboard_cancel"
)
async def cancel_keyboard_update(
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


@dp.message(
    Command("keyboard_test")
)
async def cmd_keyboard_test(
    message: types.Message,
) -> None:
    if not is_admin(
        message.from_user.id
    ):
        return

    await message.answer(
        "🧪 Тест новой клавиатуры",
        reply_markup=main_keyboard(),
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

    state = admin_state.pop(
        ADMIN_CHAT_ID,
        None,
    )

    pending_broadcast.clear()

    if state:
        await message.answer(
            "✅ Действие отменено."
        )

    else:
        await message.answer(
            "Нет активного действия."
        )


# ============================================================
# КНОПКА «НАПИСАТЬ КЛИЕНТУ»
# ============================================================

@dp.callback_query(
    F.data.startswith(
        "write_client:"
    )
)
async def callback_write_client(
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

    except (
        ValueError,
        IndexError,
    ):
        await call.answer(
            "Ошибка ID",
            show_alert=True,
        )
        return

    admin_state[
        ADMIN_CHAT_ID
    ] = {
        "mode": "reply",
        "client_id": client_id,
    }

    await call.message.answer(
        (
            "✍️ Напиши текст клиенту.\n"
            "Отмена: /cancel"
        )
    )

    await call.answer(
        "Жду текст"
    )


# ============================================================
# ЗАКАЗЫ ИЗ WEB APP
# ============================================================

@dp.message(
    F.content_type
    == ContentType.WEB_APP_DATA
)
async def handle_order(
    message: types.Message,
) -> None:
    logger.info(
        "===== ПОЛУЧЕН ЗАКАЗ ====="
    )

    raw_data = (
        message.web_app_data.data
    )

    logger.info(
        "RAW ORDER: %s",
        raw_data,
    )

    try:
        data = json.loads(
            raw_data
        )

    except json.JSONDecodeError:
        logger.exception(
            "ORDER JSON ERROR"
        )

        await message.answer(
            "⚠️ Ошибка данных заказа.",
            reply_markup=main_keyboard(),
        )

        return

    user = message.from_user

    await upsert_user(
        user
    )

    client_id = user.id

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
        else
        (
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

    delivery = safe_int(
        data.get(
            "delivery",
            0,
        )
    )

    total = safe_int(
        data.get(
            "total",
            0,
        )
    )

    raw_items = (
        data.get("items")
        or {}
    )

    items = (
        raw_items
        if isinstance(
            raw_items,
            dict,
        )
        else {}
    )

    comment = safe_str(
        data.get("comment")
        or data.get("comments")
        or data.get("comment_text")
        or data.get("note")
        or data.get("notes")
        or ""
    ).strip().lstrip(";")


    when_text = ""

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

            order_datetime = (
                datetime.strptime(
                    str(raw_date),
                    "%Y-%m-%d",
                )
                if raw_date
                else datetime.now(
                    TIMEZONE
                )
            )

            when_text = (
                order_datetime
                .strftime("%d.%m")
                + ", ближайшее"
            )

        elif (
            data.get("orderDate")
            and
            data.get("orderTime")
        ):
            order_datetime = (
                datetime.strptime(
                    str(
                        data[
                            "orderDate"
                        ]
                    ),
                    "%Y-%m-%d",
                )
            )

            when_text = (
                order_datetime
                .strftime("%d.%m")
                + " в "
                + str(
                    data[
                        "orderTime"
                    ]
                )
            )

    except Exception:
        logger.exception(
            "ORDER TIME ERROR"
        )


    order_lines: list[str] = []

    order_items: list[
        dict[str, Any]
    ] = []

    for name, item in items.items():
        if not isinstance(
            item,
            dict,
        ):
            continue

        quantity = max(
            0,
            safe_int(
                item.get(
                    "qty",
                    0,
                )
            ),
        )

        price = max(
            0,
            safe_int(
                item.get(
                    "price",
                    0,
                )
            ),
        )

        order_lines.append(
            (
                f"- {name} ×{quantity} "
                f"= {quantity * price} ฿"
            )
        )

        order_items.append(
            {
                "name": safe_str(name),
                "qty": quantity,
                "price": price,
                "img": safe_str(
                    item.get("img")
                ),
            }
        )


    items_text = (
        "\n".join(order_lines)
        if order_lines
        else "—"
    )


    try:
        order_id = (
            await save_order_to_database(
                user,
                data,
                order_items,
            )
        )

        logger.info(
            "ORDER SAVED: id=%s",
            order_id,
        )

    except Exception:
        logger.exception(
            "ORDER DATABASE ERROR"
        )


    client_text = (
        "📦 Ваш заказ принят!\n\n"

        f"Имя: {username}\n"

        f"Телефон: {phone}\n"

        f"Адрес: {address}\n"

        f"Оплата: {pay_method}\n"

        f"Доставка: {delivery} ฿\n"
    )

    if when_text:
        client_text += (
            f"Время: {when_text}\n"
        )

    if comment:
        client_text += (
            f"Комментарий: {comment}\n"
        )

    client_text += (
        "\n🧾 Состав заказа:\n"
        f"{items_text}\n\n"
        f"💰 Итого: {total} ฿"
    )


    await message.answer(
        client_text,
        reply_markup=main_keyboard(),
    )

    keyboard_shown_users.add(
        client_id
    )


    admin_text = (
        "✅ <b>Новый заказ</b>\n"

        f"• <i>Пользователь:</i> "
        f"{html.escape(username)}\n"

        f"• <i>User ID:</i> "
        f"<code>{client_id}</code>\n"

        f"• <i>Телефон:</i> "
        f"{html.escape(phone)}\n"

        f"• <i>Адрес:</i> "
        f"{html.escape(address)}\n"

        f"• <i>Доставка:</i> "
        f"{delivery} ฿\n"

        f"• <i>Оплата:</i> "
        f"{html.escape(pay_method)}\n"
    )

    if when_text:
        admin_text += (
            f"• <i>Время:</i> "
            f"{html.escape(when_text)}\n"
        )

    if comment:
        admin_text += (
            f"• <i>Комментарий:</i> "
            f"{html.escape(comment)}\n"
        )

    admin_text += (
        "\n🍽 <b>Состав заказа:</b>\n"
        f"{html.escape(items_text)}\n\n"
        f"💰 <b>Итого:</b> {total} ฿"
    )


    try:
        await send_order_to_admin(
            admin_text,
            client_id,
        )

    except Exception:
        logger.exception(
            "ADMIN ORDER SEND ERROR"
        )


    print_payload = {
        "name": username,
        "phone": phone,
        "address": address,
        "delivery": delivery,
        "payment": pay_method,
        "items": order_items,
        "total": total,

        "date": datetime.now(
            TIMEZONE
        ).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),

        "order_time": when_text,

        "comment": comment,
        "comments": comment,
        "comment_text": comment,
        "note": comment,
        "notes": comment,
    }


    try:
        timeout = aiohttp.ClientTimeout(
            total=7
        )

        async with aiohttp.ClientSession(
            timeout=timeout
        ) as session:

            async with session.post(
                PRINT_URL,
                json=print_payload,
            ) as response:

                await response.text()

                if response.status == 200:
                    logger.info(
                        "PRINT SENT"
                    )

                else:
                    logger.error(
                        "PRINT ERROR: HTTP %s",
                        response.status,
                    )

    except Exception:
        logger.exception(
            "PRINT SEND ERROR"
        )


# ============================================================
# СЛЕДУЮЩЕЕ СООБЩЕНИЕ АДМИНА
# ============================================================

@dp.message(
    F.from_user.id
    == ADMIN_CHAT_ID
)
async def admin_message_router(
    message: types.Message,
) -> None:
    state = admin_state.get(
        ADMIN_CHAT_ID
    )

    if not state:
        return


    if state.get("mode") == "reply":
        if not message.text:
            await message.answer(
                (
                    "Для ответа клиенту "
                    "отправь текст."
                )
            )
            return

        client_id = int(
            state["client_id"]
        )

        admin_state.pop(
            ADMIN_CHAT_ID,
            None,
        )

        try:
            await bot.send_message(
                client_id,
                (
                    "💬 Сообщение "
                    "от менеджера:\n\n"
                    f"{message.text}"
                ),
            )

            await message.answer(
                "✅ Отправлено клиенту."
            )

        except Exception as error:
            blocked = is_block_error(
                error
            )

            await mark_send_error(
                client_id,
                str(error),
                blocked,
            )

            logger.exception(
                "DIRECT MESSAGE ERROR"
            )

            await message.answer(
                (
                    "⚠️ Не удалось отправить "
                    "сообщение клиенту."
                )
            )

        return


    if state.get("mode") == "broadcast":
        admin_state.pop(
            ADMIN_CHAT_ID,
            None,
        )

        pending_broadcast.clear()

        pending_broadcast.update(
            {
                "source_chat_id": (
                    message.chat.id
                ),

                "source_message_id": (
                    message.message_id
                ),
            }
        )

        total = await get_target_count(
            "marketing"
        )

        try:
            await bot.copy_message(
                chat_id=ADMIN_CHAT_ID,

                from_chat_id=(
                    message.chat.id
                ),

                message_id=(
                    message.message_id
                ),

                reply_markup=(
                    broadcast_confirm_keyboard()
                ),
            )

            await message.answer(
                (
                    "👆 Предпросмотр рекламы.\n"
                    f"Получателей: {total}\n\n"
                    "Проверь сообщение и нажми "
                    "кнопку под предпросмотром."
                )
            )

        except TelegramBadRequest as error:
            pending_broadcast.clear()

            await message.answer(
                (
                    "⚠️ Этот тип сообщения "
                    "нельзя использовать.\n"
                    f"Ошибка: {error}"
                )
            )

        return


# ============================================================
# ОБЫЧНЫЕ СООБЩЕНИЯ
# ============================================================

@dp.message()
async def fallback_handler(
    message: types.Message,
) -> None:
    if (
        message.content_type
        == ContentType.WEB_APP_DATA
    ):
        return

    if message.text in (
        MENU_BTN_TEXT,
        ASK_BTN_TEXT,
    ):
        return

    await send_main_keyboard(
        message,
        "Выберите действие 👇",
        force=False,
    )


# ============================================================
# HEALTH SERVER
# ============================================================

def run_health_server(
    port: int,
) -> None:
    class HealthHandler(
        BaseHTTPRequestHandler
    ):
        def do_GET(self):
            self.send_response(
                200
            )

            self.send_header(
                "Content-Type",
                "text/plain; charset=utf-8",
            )

            self.end_headers()

            self.wfile.write(
                b"Smoke Factory BBQ bot is running"
            )

        def log_message(
            self,
            format,
            *args,
        ):
            return


    server = HTTPServer(
        (
            "0.0.0.0",
            port,
        ),
        HealthHandler,
    )

    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
    )

    thread.start()

    logger.info(
        "HEALTH SERVER: port=%s",
        port,
    )


# ============================================================
# АВТОПЕРЕЗАПУСК
# ============================================================

def schedule_restart() -> None:
    if RESTART_MINUTES <= 0:
        logger.info(
            "AUTO RESTART DISABLED"
        )
        return


    def restart_or_delay():
        if broadcast_running:
            logger.warning(
                (
                    "AUTO RESTART DELAYED: "
                    "broadcast is running"
                )
            )

            timer = threading.Timer(
                600,
                restart_or_delay,
            )

            timer.daemon = True

            timer.start()

            return


        logger.info(
            "AUTO RESTART"
        )

        os.execv(
            sys.executable,
            [
                sys.executable,
                *sys.argv,
            ],
        )


    timer = threading.Timer(
        RESTART_MINUTES * 60,
        restart_or_delay,
    )

    timer.daemon = True

    timer.start()


# ============================================================
# КОМАНДЫ В МЕНЮ TELEGRAM
# ============================================================

async def set_bot_commands() -> None:
    await bot.set_my_commands(
        [
            BotCommand(
                command="start",
                description="Открыть меню",
            ),

            BotCommand(
                command="stop",
                description=(
                    "Отключить рекламу"
                ),
            ),

            BotCommand(
                command="ads_on",
                description=(
                    "Включить рекламу"
                ),
            ),
        ]
    )


    await bot.set_my_commands(
        [
            BotCommand(
                command="adminhelp",
                description=(
                    "Команды администратора"
                ),
            ),

            BotCommand(
                command="nu4etam",
                description="Статистика",
            ),

            BotCommand(
                command="users",
                description=(
                    "Пользователи бота"
                ),
            ),

            BotCommand(
                command="broadcast",
                description=(
                    "Создать рассылку"
                ),
            ),

            BotCommand(
                command="broadcast_status",
                description=(
                    "Статус рассылки"
                ),
            ),

            BotCommand(
                command="update_keyboard",
                description=(
                    "Обновить клавиатуру"
                ),
            ),

            BotCommand(
                command="export_users",
                description=(
                    "Выгрузить пользователей"
                ),
            ),
        ],

        scope=BotCommandScopeChat(
            chat_id=ADMIN_CHAT_ID
        ),
    )


# ============================================================
# ЗАПУСК
# ============================================================

async def main() -> None:
    logger.info(
        "=== START SMOKE FACTORY BBQ BOT ==="
    )

    try:
        await bot.delete_webhook(
            drop_pending_updates=True
        )

    except Exception:
        logger.exception(
            "DELETE WEBHOOK ERROR"
        )


    await init_database()

    await set_bot_commands()

    run_health_server(
        PORT
    )

    schedule_restart()

    logger.info(
        "BOT READY: polling started"
    )

    await dp.start_polling(
        bot
    )


if __name__ == "__main__":
    asyncio.run(
        main()
    )
