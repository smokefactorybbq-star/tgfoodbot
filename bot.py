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
# НАСТРОЙКИ — ОСТАВЛЕНЫ БЕЗ ИЗМЕНЕНИЙ
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

ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "7309681026"))
RESTART_MINUTES = int(os.getenv("RESTART_MINUTES", "420"))
PORT = int(os.getenv("PORT", "8080"))

MANAGER_URL = os.getenv("MANAGER_URL", "https://t.me/SmokefactoryBBQ")
WEBAPP_URL = os.getenv(
    "WEBAPP_URL",
    "https://mini-app-production-67f2.up.railway.app",
).rstrip("/")

MENU_BTN_TEXT = "📋 Открыть меню"
ASK_BTN_TEXT = "💬 Задать вопрос менеджеру"

PRINT_URL = os.getenv(
    "PRINT_URL",
    "https://6b6b-171-6-244-48.ngrok-free.app/order",
)


# ============================================================================
# ИНИЦИАЛИЗАЦИЯ
# ============================================================================

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

KEYBOARD_SHOWN_USERS: set[int] = set()

waiting_reply: dict[int, dict[str, int]] = {}
waiting_broadcast: set[int] = set()
pending_broadcasts: dict[int, dict] = {}

# Состояние команды /bonus.
# Формат:
# waiting_bonus[manager_id] = {
#     "stage": "telegram_id" или "amount",
#     "telegram_id": 123
# }
waiting_bonus: dict[int, dict] = {}

broadcast_lock = asyncio.Lock()
broadcast_running = False
BROADCAST_DELAY = 0.06

db_pool: asyncpg.Pool | None = None

TIMEZONE = ZoneInfo("Asia/Bangkok")


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
            ADD COLUMN IF NOT EXISTS
                is_active BOOLEAN NOT NULL DEFAULT TRUE;

            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS
                marketing_allowed BOOLEAN NOT NULL DEFAULT TRUE;

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

            /*
             * Ручная сумма покупок для старых клиентов.
             * Это не деньги к списанию, а исторический оборот
             * для определения уровня скидки.
             */
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS
                manual_spend BIGINT NOT NULL DEFAULT 0;

            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS
                bonus_updated_at TIMESTAMPTZ;

            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS
                bonus_updated_by BIGINT;

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
                visited_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
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
                source TEXT NOT NULL DEFAULT 'mini_app',
                customer_name TEXT,
                phone TEXT,
                address TEXT,
                address_plain TEXT,
                payment_method TEXT,
                delivery_fee INTEGER NOT NULL DEFAULT 0,
                items_total INTEGER NOT NULL DEFAULT 0,
                discount_percent INTEGER NOT NULL DEFAULT 0,
                discount_amount INTEGER NOT NULL DEFAULT 0,
                total INTEGER NOT NULL DEFAULT 0,
                order_when TEXT,
                order_date DATE,
                order_time TEXT,
                comment TEXT,
                status TEXT NOT NULL DEFAULT 'created',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

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
             * Таблица истории рассылок.
             * broadcast_type сначала nullable, чтобы выполнить
             * миграцию со старой колонки kind.
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
            ADD COLUMN IF NOT EXISTS total_targets INTEGER NOT NULL DEFAULT 0;

            ALTER TABLE broadcast_logs
            ADD COLUMN IF NOT EXISTS delivered INTEGER NOT NULL DEFAULT 0;

            ALTER TABLE broadcast_logs
            ADD COLUMN IF NOT EXISTS blocked INTEGER NOT NULL DEFAULT 0;

            ALTER TABLE broadcast_logs
            ADD COLUMN IF NOT EXISTS failed INTEGER NOT NULL DEFAULT 0;

            ALTER TABLE broadcast_logs
            ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'created';

            ALTER TABLE broadcast_logs
            ADD COLUMN IF NOT EXISTS created_at
                TIMESTAMPTZ NOT NULL DEFAULT NOW();

            ALTER TABLE broadcast_logs
            ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;

            /*
             * Исправление ошибки:
             * column broadcast_type does not exist.
             *
             * В старой версии колонка называлась kind.
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
             * История ручных начислений.
             * request_id не позволяет дважды записать
             * одно начисление при повторном запросе.
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

    logger.info("База данных подключена, таблицы готовы")


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
            getattr(user, "id", "unknown"),
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
# АВТОМАТИЧЕСКОЕ СОХРАНЕНИЕ ПОЛЬЗОВАТЕЛЕЙ
# ============================================================================

class UserTrackingMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler,
        event,
        data,
    ):
        user = data.get("event_from_user")

        if user:
            await upsert_user(user)

        return await handler(event, data)


dp.message.outer_middleware(UserTrackingMiddleware())
dp.callback_query.outer_middleware(UserTrackingMiddleware())


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
    return telegram_id == ADMIN_CHAT_ID


def is_blocking_error(
    error: Exception,
) -> bool:
    if isinstance(error, TelegramForbiddenError):
        return True

    error_text = str(error).lower()

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

    amount = int(cleaned)

    if amount < 0 or amount > 10_000_000:
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
# HEALTHCHECK И ПЛАНОВЫЙ ПЕРЕЗАПУСК
# ============================================================================

def run_fake_server(
    port: int = PORT,
) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")

        def log_message(
            self,
            format: str,
            *args,
        ) -> None:
            return

    server = HTTPServer(
        ("", port),
        Handler,
    )

    threading.Thread(
        target=server.serve_forever,
        daemon=True,
    ).start()


def schedule_restart() -> None:
    if RESTART_MINUTES <= 0:
        logger.info("Плановый перезапуск отключён")
        return

    def _restart() -> None:
        global broadcast_running

        if broadcast_running:
            logger.warning(
                "Перезапуск отложен: сейчас выполняется рассылка"
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
            [sys.executable] + sys.argv,
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
    ).encode("utf-8")

    token = (
        base64.urlsafe_b64encode(payload_json)
        .decode("ascii")
        .rstrip("=")
    )

    signature = hmac.new(
        API_TOKEN.encode("utf-8"),
        token.encode("ascii"),
        hashlib.sha256,
    ).hexdigest()

    parts = urlsplit(WEBAPP_URL)

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
            url=build_signed_webapp_url(user)
        ),
    )

    ask_btn = types.KeyboardButton(
        text=ASK_BTN_TEXT
    )

    return types.ReplyKeyboardMarkup(
        keyboard=[
            [web_app_btn],
            [ask_btn],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def updated_keyboard(
    user: types.User,
) -> types.ReplyKeyboardMarkup:
    return start_keyboard(user)


async def send_main_keyboard(
    message: types.Message,
    text: str,
    force: bool = False,
) -> bool:
    uid = message.from_user.id

    if uid in KEYBOARD_SHOWN_USERS and not force:
        return False

    await message.answer(
        text,
        reply_markup=start_keyboard(message.from_user),
    )

    KEYBOARD_SHOWN_USERS.add(uid)

    return True


def make_user_from_database(
    row: asyncpg.Record,
) -> types.User:
    return types.User(
        id=int(row["telegram_id"]),
        is_bot=False,
        first_name=(
            row["telegram_first_name"]
            or "Пользователь"
        ),
        last_name=row["telegram_last_name"],
        username=row["username"],
    )


# ============================================================================
# КНОПКИ
# ============================================================================

def build_admin_kb_full(
    client_id: int,
) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()

    kb.button(
        text="👤 Открыть профиль клиента",
        url=f"tg://user?id={client_id}",
    )

    kb.button(
        text="✍️ Написать клиенту",
        callback_data=f"write_client:{client_id}",
    )

    kb.adjust(1)

    return kb.as_markup()


def build_admin_kb_safe(
    client_id: int,
) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()

    kb.button(
        text="✍️ Написать клиенту",
        callback_data=f"write_client:{client_id}",
    )

    kb.adjust(1)

    return kb.as_markup()


def build_unsubscribe_keyboard() -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()

    kb.button(
        text="🔕 Не получать рекламу",
        callback_data="unsubscribe_ads",
    )

    return kb.as_markup()


def build_broadcast_confirm_keyboard() -> types.InlineKeyboardMarkup:
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


def build_keyboard_update_confirm() -> types.InlineKeyboardMarkup:
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
) -> None:
    try:
        await bot.send_message(
            ADMIN_CHAT_ID,
            admin_text_html,
            parse_mode="HTML",
            reply_markup=build_admin_kb_full(client_id),
        )

        logger.info(
            "ADMIN: sent with full kb (profile+reply)"
        )

    except Exception as exc:
        error_text = str(exc)

        logger.error(
            "ADMIN send failed (full kb): %s",
            error_text,
        )

        if "BUTTON_USER_PRIVACY_RESTRICTED" in error_text:
            await bot.send_message(
                ADMIN_CHAT_ID,
                admin_text_html,
                parse_mode="HTML",
                reply_markup=build_admin_kb_safe(client_id),
            )
            return

        raise


# ============================================================================
# СОХРАНЕНИЕ ЗАКАЗОВ
# ============================================================================

async def save_order_to_database(
    user: types.User,
    data: dict,
    order_items: list[dict],
) -> int | None:
    if not db_pool:
        return None

    await upsert_user(user)

    items_total = sum(
        max(0, safe_int(item.get("qty")))
        * max(0, safe_int(item.get("price")))
        for item in order_items
    )

    delivery = max(
        0,
        safe_int(data.get("delivery", 0)),
    )

    discount_percent = max(
        0,
        min(
            100,
            safe_int(
                data.get(
                    "discountPercent",
                    data.get("discount_percent", 0),
                )
            ),
        ),
    )

    discount_amount = max(
        0,
        safe_int(
            data.get(
                "discount",
                data.get("discountAmount", 0),
            )
        ),
    )

    total = max(
        0,
        safe_int(
            data.get(
                "total",
                items_total + delivery - discount_amount,
            )
        ),
    )

    order_date = None
    raw_order_date = data.get("orderDate")

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
            order_id = await conn.fetchval(
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
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                    $11,$12,$13,$14,$15
                )
                RETURNING id
                """,
                user.id,
                safe_str(data.get("name") or user.full_name),
                safe_str(data.get("phone")),
                safe_str(data.get("address")),
                safe_str(data.get("address_plain")),
                safe_str(data.get("payMethod")),
                delivery,
                items_total,
                discount_percent,
                discount_amount,
                total,
                safe_str(data.get("orderWhen")),
                order_date,
                safe_str(data.get("orderTime")),
                safe_str(
                    data.get("comment")
                    or data.get("comments")
                    or data.get("note")
                ),
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
                    VALUES ($1,$2,$3,$4,$5)
                    """,
                    [
                        (
                            order_id,
                            item["name"],
                            item["qty"],
                            item["price"],
                            item.get("img"),
                        )
                        for item in order_items
                    ],
                )

    return order_id


# ============================================================================
# СТАТИСТИКА
# ============================================================================

async def build_daily_report() -> str:
    if not db_pool:
        raise RuntimeError("База данных не подключена")

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            WITH day_bounds AS (
                SELECT
                    (
                        date_trunc(
                            'day',
                            NOW() AT TIME ZONE 'Asia/Bangkok'
                        ) AT TIME ZONE 'Asia/Bangkok'
                    ) AS start_utc,
                    (
                        (
                            date_trunc(
                                'day',
                                NOW() AT TIME ZONE 'Asia/Bangkok'
                            ) + INTERVAL '1 day'
                        ) AT TIME ZONE 'Asia/Bangkok'
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
                    FROM users u, day_bounds d
                    WHERE
                        u.last_bot_activity_at >= d.start_utc
                        AND u.last_bot_activity_at < d.end_utc
                ) AS active_today,

                (
                    SELECT COUNT(*)
                    FROM visits v, day_bounds d
                    WHERE
                        v.visited_at >= d.start_utc
                        AND v.visited_at < d.end_utc
                ) AS visits,

                (
                    SELECT COUNT(DISTINCT telegram_id)
                    FROM visits v, day_bounds d
                    WHERE
                        v.visited_at >= d.start_utc
                        AND v.visited_at < d.end_utc
                        AND telegram_id IS NOT NULL
                ) AS unique_visitors,

                (
                    SELECT COUNT(*)
                    FROM users u, day_bounds d
                    WHERE
                        u.created_at >= d.start_utc
                        AND u.created_at < d.end_utc
                ) AS new_users,

                (
                    SELECT COUNT(*)
                    FROM orders o, day_bounds d
                    WHERE
                        o.created_at >= d.start_utc
                        AND o.created_at < d.end_utc
                ) AS orders_count,

                (
                    SELECT COUNT(DISTINCT telegram_id)
                    FROM orders o, day_bounds d
                    WHERE
                        o.created_at >= d.start_utc
                        AND o.created_at < d.end_utc
                ) AS buyers,

                (
                    SELECT COALESCE(SUM(total), 0)
                    FROM orders o, day_bounds d
                    WHERE
                        o.created_at >= d.start_utc
                        AND o.created_at < d.end_utc
                ) AS revenue,

                (
                    SELECT COALESCE(AVG(total), 0)
                    FROM orders o, day_bounds d
                    WHERE
                        o.created_at >= d.start_utc
                        AND o.created_at < d.end_utc
                ) AS avg_check
            """
        )

    visits = int(row["visits"] or 0)
    unique_visitors = int(row["unique_visitors"] or 0)
    orders_count = int(row["orders_count"] or 0)

    conversion = (
        orders_count / unique_visitors * 100
        if unique_visitors
        else 0
    )

    today = datetime.now(TIMEZONE).strftime("%d.%m.%Y")

    return (
        f"📊 Статистика за {today}\n\n"
        f"👥 Всего ID в базе: {int(row['total_users'] or 0)}\n"
        f"✅ Активных пользователей: {int(row['active_users'] or 0)}\n"
        f"📣 Доступно для рекламы: {int(row['marketing_users'] or 0)}\n"
        f"🚫 Заблокировали/недоступны: {int(row['blocked_users'] or 0)}\n"
        f"💬 Пользователей бота сегодня: {int(row['active_today'] or 0)}\n"
        f"🆕 Новых пользователей: {int(row['new_users'] or 0)}\n\n"
        f"Открытий сайта: {visits}\n"
        f"Уникальных посетителей: {unique_visitors}\n\n"
        f"Заказов: {orders_count}\n"
        f"Покупателей: {int(row['buyers'] or 0)}\n"
        f"Конверсия: {conversion:.1f}%\n\n"
        f"Выручка: {int(row['revenue'] or 0)} ฿\n"
        f"Средний чек: {round(float(row['avg_check'] or 0))} ฿"
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

    return len(targets)


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
            $1,
            $2,
            $3,
            $4,
            $5,
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

    return int(log_id)


async def finish_broadcast_log(
    log_id: int,
    delivered: int,
    blocked: int,
    failed: int,
    status: str,
) -> None:
    if not db_pool or not log_id:
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
            reply_markup=build_unsubscribe_keyboard(),
        )

        await mark_send_success(
            telegram_id,
            "broadcast",
        )

        return "delivered"

    except TelegramRetryAfter as exc:
        await asyncio.sleep(
            float(exc.retry_after) + 1
        )

        return await send_advertising_message(
            telegram_id,
            source_chat_id,
            source_message_id,
        )

    except Exception as exc:
        blocked = is_blocking_error(exc)

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
        user_row["telegram_id"]
    )

    try:
        telegram_user = make_user_from_database(
            user_row
        )

        await bot.send_message(
            telegram_id,
            (
                "🔄 Меню Smoke Factory BBQ обновлено.\n"
                "Используйте новую кнопку ниже 👇"
            ),
            reply_markup=start_keyboard(telegram_user),
        )

        await mark_send_success(
            telegram_id,
            "keyboard",
        )

        return "delivered"

    except TelegramRetryAfter as exc:
        await asyncio.sleep(
            float(exc.retry_after) + 1
        )

        return await send_new_keyboard(
            user_row
        )

    except Exception as exc:
        blocked = is_blocking_error(exc)

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

            total = len(targets)

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
                    user_row["telegram_id"]
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
                            message_id=progress_message.message_id,
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
                    message_id=progress_message.message_id,
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
# РУЧНАЯ СУММА ЛОЯЛЬНОСТИ /bonus
# ============================================================================

def make_bonus_request_payload(
    telegram_id: int,
    amount: int,
    manager_id: int,
    timestamp: int,
    request_id: str,
) -> str:
    """
    JSON с сортировкой ключей.
    server.js создаёт строку в таком же порядке.
    """
    return json.dumps(
        {
            "amount": amount,
            "managerId": manager_id,
            "requestId": request_id,
            "telegramId": str(telegram_id),
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
    """
    Обновляет сумму именно в базе mini-app.

    Даже если бот и mini-app подключены к разным PostgreSQL,
    личный кабинет увидит сумму.
    """

    timestamp = int(time.time())

    payload_string = make_bonus_request_payload(
        telegram_id,
        amount,
        manager_id,
        timestamp,
        request_id,
    )

    signature = hmac.new(
        API_TOKEN.encode("utf-8"),
        payload_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    request_body = {
        "telegramId": str(telegram_id),
        "amount": amount,
        "managerId": manager_id,
        "timestamp": timestamp,
        "requestId": request_id,
    }

    url = f"{WEBAPP_URL}/api/admin/bonus"

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
                "X-Bonus-Signature": signature,
            },
        ) as response:
            response_text = await response.text()

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
                or not response_data.get("ok")
            ):
                raise RuntimeError(
                    response_data.get("error")
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
    """
    Дублирует сумму в базе бота для отчётов и аудита.
    Если база общая с mini-app, request_id защитит
    историю от двойной записи.
    """

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
                previous_amount or 0
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
                    manual_spend = EXCLUDED.manual_spend,
                    bonus_updated_at = NOW(),
                    bonus_updated_by = EXCLUDED.bonus_updated_by,
                    updated_at = NOW()
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
                    $1,
                    $2,
                    $3,
                    $4,
                    $5,
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

    mini_app_result = await update_bonus_in_mini_app(
        telegram_id,
        amount,
        manager_id,
        request_id,
    )

    previous_local = await save_bonus_in_bot_database(
        telegram_id,
        amount,
        manager_id,
        request_id,
    )

    mini_app_result["previousLocalAmount"] = previous_local

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
                f"Накопленная сумма: {total_spend:,} ฿\n"
                f"Текущая скидка: {discount_percent}%\n\n"
                "Информация уже доступна "
                "в личном кабинете."
            ).replace(",", " "),
        )

        await mark_send_success(
            telegram_id,
            "direct",
        )

        return "sent"

    except Exception as exc:
        blocked = is_blocking_error(exc)

        await mark_send_error(
            telegram_id,
            str(exc),
            blocked,
        )

        return "blocked" if blocked else "failed"


async def process_bonus_amount(
    message: types.Message,
    telegram_id: int,
    amount: int,
) -> None:
    await message.answer(
        (
            "⏳ Сохраняю сумму в личный кабинет...\n\n"
            f"Telegram ID: {telegram_id}\n"
            f"Ручная сумма: {amount:,} ฿"
        ).replace(",", " ")
    )

    try:
        result = await apply_manager_bonus(
            telegram_id,
            amount,
            message.from_user.id,
        )

        manual_spend = int(
            result.get("manualSpend", amount)
            or amount
        )

        order_spend = int(
            result.get("orderSpend", 0)
            or 0
        )

        total_spend = int(
            result.get(
                "totalSpend",
                manual_spend + order_spend,
            )
            or 0
        )

        discount_percent = int(
            result.get(
                "discountPercent",
                discount_by_spend(total_spend),
            )
            or 0
        )

        notification_status = await notify_user_about_bonus(
            telegram_id,
            total_spend,
            discount_percent,
        )

        notification_text = {
            "sent": "Пользователь уведомлён.",
            "blocked": (
                "Пользователь заблокировал бота, "
                "но сумма в ЛК сохранена."
            ),
            "failed": (
                "Сумма сохранена, но уведомление "
                "отправить не удалось."
            ),
        }.get(
            notification_status,
            "Статус уведомления неизвестен.",
        )

        await message.answer(
            (
                "✅ Сумма сохранена в личном кабинете\n\n"
                f"Telegram ID: {telegram_id}\n"
                f"Фактические заказы: {order_spend:,} ฿\n"
                f"Ручная сумма: {manual_spend:,} ฿\n"
                f"Общая накопленная сумма: {total_spend:,} ฿\n"
                f"Уровень скидки: {discount_percent}%\n\n"
                f"{notification_text}"
            ).replace(",", " ")
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
                "Состояние команды сохранено. "
                "Можно повторно отправить сумму "
                "или выполнить /cancel."
            )
        )


# ============================================================================
# ПОЛЬЗОВАТЕЛЬСКИЕ КОМАНДЫ
# ============================================================================

@dp.message(Command("start"))
async def cmd_start(
    message: types.Message,
) -> None:
    await upsert_user(
        message.from_user
    )

    await send_main_keyboard(
        message,
        (
            "Нажмите кнопку ниже, чтобы открыть меню.\n"
            "Если есть вопросы — нажмите "
            "«💬 Задать вопрос менеджеру».\n\n"
            "Отключить рекламу: /stop"
        ),
        force=True,
    )


@dp.message(Command("stop"))
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
        reply_markup=start_keyboard(message.from_user),
    )


@dp.message(Command("ads_on"))
async def cmd_ads_on(
    message: types.Message,
) -> None:
    await set_marketing_allowed(
        message.from_user.id,
        True,
    )

    await message.answer(
        "🔔 Рекламные сообщения включены.",
        reply_markup=start_keyboard(message.from_user),
    )


@dp.callback_query(F.data == "unsubscribe_ads")
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


@dp.message(F.text == MENU_BTN_TEXT)
async def refresh_menu_keyboard(
    message: types.Message,
) -> None:
    await upsert_user(
        message.from_user
    )

    await message.answer(
        "✅ Кнопка меню обновлена. Нажмите её ещё раз.",
        reply_markup=updated_keyboard(message.from_user),
    )

    KEYBOARD_SHOWN_USERS.add(
        message.from_user.id
    )


@dp.message(F.text == ASK_BTN_TEXT)
async def open_manager_chat(
    message: types.Message,
) -> None:
    await upsert_user(
        message.from_user
    )

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


@dp.callback_query(F.data == "back_to_menu")
async def back_to_menu(
    call: types.CallbackQuery,
) -> None:
    await call.message.answer(
        "Ок. Возвращаю кнопки меню 👇",
        reply_markup=start_keyboard(call.from_user),
    )

    KEYBOARD_SHOWN_USERS.add(
        call.from_user.id
    )

    await call.answer()


# ============================================================================
# АДМИНИСТРАТИВНЫЕ КОМАНДЫ
# ============================================================================

@dp.message(Command("adminhelp"))
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


@dp.message(Command("bonus"))
async def cmd_bonus(
    message: types.Message,
) -> None:
    if not is_admin(
        message.from_user.id
    ):
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
            "Или сначала отправить ID, "
            "а следующим сообщением сумму.\n\n"
            "Сумма заменит прежнюю ручную сумму. "
            "Фактические заказы не изменятся.\n\n"
            "Для сброса ручной суммы укажи 0.\n"
            "Отмена: /cancel"
        )
    )


@dp.message(Command("nu4etam"))
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


@dp.message(Command("users"))
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
            last_bot_activity_at DESC NULLS LAST
        LIMIT 15
        """
    )

    lines = [
        "👥 Пользователи бота",
        "",
        f"Всего ID: {int(stats['total'] or 0)}",
        f"Активных: {int(stats['active'] or 0)}",
        f"Для рекламы: {int(stats['advertising'] or 0)}",
        f"Отказались от рекламы: {int(stats['unsubscribed'] or 0)}",
        f"Недоступны: {int(stats['blocked'] or 0)}",
        "",
        "Последние 15:",
    ]

    for user in recent_users:
        full_name = " ".join(
            part
            for part in (
                user["telegram_first_name"],
                user["telegram_last_name"],
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
            if user["marketing_allowed"]
            else "🔕"
        )

        manual_spend = int(
            user["manual_spend"] or 0
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
        "\n".join(lines)[:4096]
    )


@dp.message(Command("checkuser"))
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
    ).split(maxsplit=1)

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
            user["telegram_first_name"],
            user["telegram_last_name"],
        )
        if part
    ).strip()

    await message.answer(
        (
            "✅ Пользователь найден\n\n"
            f"Telegram ID: {user['telegram_id']}\n"
            f"Username: @{user['username'] or '-'}\n"
            f"Имя Telegram: {full_name or '-'}\n"
            f"Имя в заказе: {user['profile_name'] or '-'}\n"
            f"Телефон: {user['phone'] or '-'}\n"
            f"Адрес: {user['address'] or '-'}\n"
            f"Ручная сумма: {int(user['manual_spend'] or 0)} ฿\n"
            f"Обновил сумму: {user['bonus_updated_by'] or '-'}\n"
            f"Дата обновления: {user['bonus_updated_at'] or '-'}\n"
            f"Активен: {'да' if user['is_active'] else 'нет'}\n"
            f"Реклама разрешена: "
            f"{'да' if user['marketing_allowed'] else 'нет'}\n"
            f"Создан: {user['created_at']}\n"
            f"Последняя активность: "
            f"{user['last_bot_activity_at'] or '-'}\n"
            f"Последняя успешная отправка: "
            f"{user['last_successful_send_at'] or '-'}\n"
            f"Заблокирован: {user['blocked_at'] or '-'}\n"
            f"Последняя ошибка: {user['last_send_error'] or '-'}"
        )
    )


@dp.message(Command("export_users"))
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
    writer = csv.writer(output)

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

    writer.writerow(columns)

    for user in users:
        writer.writerow(
            [
                user[column]
                for column in columns
            ]
        )

    filename = (
        "smoke_factory_users_"
        + datetime.now(TIMEZONE).strftime("%Y%m%d_%H%M")
        + ".csv"
    )

    file = BufferedInputFile(
        output.getvalue().encode("utf-8-sig"),
        filename=filename,
    )

    await message.answer_document(
        file,
        caption=f"Пользователей в базе: {len(users)}",
    )


# ============================================================================
# СОЗДАНИЕ РЕКЛАМНОЙ РАССЫЛКИ
# ============================================================================

@dp.message(Command("broadcast"))
async def cmd_broadcast(
    message: types.Message,
) -> None:
    if not is_admin(
        message.from_user.id
    ):
        return

    if broadcast_running or broadcast_lock.locked():
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
            "Можно отправить:\n"
            "• текст;\n"
            "• фотографию с подписью;\n"
            "• видео;\n"
            "• документ.\n\n"
            "После этого бот покажет предпросмотр.\n"
            "Отмена: /cancel"
        )
    )


@dp.callback_query(F.data == "broadcast_confirm")
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
            "Рассылка не найдена. Создай её заново.",
            show_alert=True,
        )
        return

    if broadcast_running or broadcast_lock.locked():
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
            int(prepared["source_chat_id"]),
            int(prepared["source_message_id"]),
        )
    )


@dp.callback_query(F.data == "broadcast_cancel")
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


@dp.message(Command("broadcast_history"))
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
                f"№{row['id']} — {row['broadcast_type']}\n"
                f"Статус: {row['status']}\n"
                f"Всего: {row['total_targets']}, "
                f"доставлено: {row['delivered']}, "
                f"недоступны: {row['blocked']}, "
                f"ошибки: {row['failed']}\n"
                f"Дата: {row['created_at']}\n"
            )
        )

    await message.answer(
        "\n".join(lines)[:4096]
    )


# ============================================================================
# ОБНОВЛЕНИЕ КЛАВИАТУРЫ
# ============================================================================

@dp.message(Command("update_keyboard"))
async def cmd_update_keyboard(
    message: types.Message,
) -> None:
    if not is_admin(
        message.from_user.id
    ):
        return

    if broadcast_running or broadcast_lock.locked():
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
            "Клавиатуру получат все активные пользователи, "
            "включая тех, кто отказался от рекламы."
        ),
        reply_markup=build_keyboard_update_confirm(),
    )


@dp.callback_query(F.data == "keyboard_update_confirm")
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

    if broadcast_running or broadcast_lock.locked():
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
        run_broadcast("keyboard")
    )


@dp.callback_query(F.data == "keyboard_update_cancel")
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
# ОТВЕТ КЛИЕНТУ
# ============================================================================

@dp.callback_query(F.data.startswith("write_client:"))
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
            call.data.split(":", 1)[1]
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
        "✍️ Напишите текст клиенту.\nОтмена: /cancel"
    )

    await call.answer(
        "Жду текст"
    )


@dp.message(Command("cancel"))
async def cmd_cancel(
    message: types.Message,
) -> None:
    if not is_admin(
        message.from_user.id
    ):
        return

    cancelled = False
    admin_id = message.from_user.id

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

    if cancelled:
        await message.answer(
            "✅ Действие отменено."
        )
    else:
        await message.answer(
            "Нет активного действия."
        )


# ============================================================================
# ЗАКАЗЫ ИЗ TELEGRAM WEB APP
# ============================================================================

@dp.message(F.content_type == ContentType.WEB_APP_DATA)
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
        data = json.loads(raw)
    except Exception:
        logger.exception(
            "JSON parse error"
        )

        await message.answer(
            "⚠️ Ошибка данных заказа.",
            reply_markup=start_keyboard(message.from_user),
        )
        return

    user = message.from_user
    client_id = user.id

    await upsert_user(user)

    pay_method = safe_str(
        data.get("payMethod", "не выбран"),
        "не выбран",
    )

    username = (
        f"@{user.username}"
        if user.username
        else (user.full_name or "Без имени")
    )

    phone = safe_str(
        data.get("phone", "не указан"),
        "не указан",
    )

    address = safe_str(
        data.get("address", "не указан"),
        "не указан",
    )

    delivery = safe_int(
        data.get("delivery", 0),
        0,
    )

    total = safe_int(
        data.get("total", 0),
        0,
    )

    items = data.get("items") or {}

    if not isinstance(items, dict):
        items = {}

    comment = (
        data.get("comment")
        or data.get("comments")
        or data.get("comment_text")
        or data.get("note")
        or data.get("notes")
        or ""
    )

    comment = (
        safe_str(comment, "")
        .strip()
        .lstrip(";")
    )

    when_str = ""

    try:
        if data.get("orderWhen") in (
            "soonest",
            "asap",
        ):
            raw_date = data.get("orderDate")

            dt = (
                datetime.strptime(
                    str(raw_date),
                    "%Y-%m-%d",
                )
                if raw_date
                else datetime.now(TIMEZONE)
            )

            when_str = (
                f"{dt.strftime('%d.%m')}, ближайшее"
            )

        elif (
            data.get("orderDate")
            and data.get("orderTime")
        ):
            dt = datetime.strptime(
                str(data["orderDate"]),
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
    order_items: list[dict] = []

    for name, info in items.items():
        if not isinstance(info, dict):
            continue

        qty = safe_int(
            info.get("qty", 0),
            0,
        )

        price = safe_int(
            info.get("price", 0),
            0,
        )

        lines.append(
            f"- {name} ×{qty} = {qty * price} ฿"
        )

        order_items.append(
            {
                "name": safe_str(name, ""),
                "qty": qty,
                "price": price,
                "img": safe_str(
                    info.get("img"),
                    "",
                ),
            }
        )

    items_text = (
        "\n".join(lines)
        if lines
        else "—"
    )

    try:
        saved_order_id = await save_order_to_database(
            user,
            data,
            order_items,
        )

        logger.info(
            "Заказ сохранён в БД, id=%s",
            saved_order_id,
        )

    except Exception:
        logger.exception(
            "Не удалось сохранить заказ в БД"
        )

    client_text = (
        "📦 Ваш заказ принят!\n\n"
        f"Имя: {data.get('name') or username}\n"
        f"Телефон: {phone}\n"
        f"Адрес: {address}\n"
        f"Оплата: {pay_method}\n"
        f"Доставка: {delivery} ฿\n"
    )

    if when_str:
        client_text += (
            f"Время: {when_str}\n"
        )

    if comment:
        client_text += (
            f"Комментарий: {comment}\n"
        )

    client_text += (
        f"\n🧾 Состав заказа:\n{items_text}"
        f"\n\n💰 Итого: {total} ฿"
    )

    await message.answer(
        client_text,
        reply_markup=start_keyboard(user),
    )

    KEYBOARD_SHOWN_USERS.add(
        client_id
    )

    admin_text = (
        "✅ <b>Новый заказ</b>\n"
        f"• <i>Пользователь:</i> {html.escape(username)}\n"
        f"• <i>User ID:</i> <code>{client_id}</code>\n"
        f"• <i>Имя:</i> "
        f"{html.escape(safe_str(data.get('name') or username))}\n"
        f"• <i>Телефон:</i> {html.escape(phone)}\n"
        f"• <i>Адрес:</i> {html.escape(address)}\n"
        f"• <i>Доставка:</i> {delivery} ฿\n"
        f"• <i>Оплата:</i> {html.escape(pay_method)}\n"
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
        f"\n🍽 <b>Состав заказа:</b>\n"
        f"{html.escape(items_text)}"
        f"\n\n💰 <b>Итого:</b> {total} ฿"
    )

    try:
        await send_order_to_admin(
            admin_text,
            client_id,
        )
    except Exception:
        logger.exception(
            "ADMIN send failed окончательно, "
            "даже без profile кнопки"
        )

    print_payload = {
        "name": data.get("name") or username,
        "phone": phone,
        "address": address,
        "delivery": delivery,
        "payment": pay_method,
        "items": order_items,
        "total": total,
        "date": datetime.now(
            TIMEZONE
        ).strftime("%Y-%m-%d %H:%M:%S"),
        "order_time": when_str,
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
                        "Печать отправлена"
                    )
                else:
                    logger.error(
                        "Ошибка печати: HTTP %s",
                        response.status,
                    )

    except Exception:
        logger.exception(
            "Print send error"
        )


# ============================================================================
# СООБЩЕНИЯ АДМИНИСТРАТОРА
# ============================================================================

@dp.message(F.from_user.id == ADMIN_CHAT_ID)
async def admin_message_router(
    message: types.Message,
) -> None:
    admin_id = message.from_user.id

    if (
        message.text
        and message.text.startswith("/")
    ):
        return

    # --------------------------------------------------------
    # Команда /bonus
    # --------------------------------------------------------

    if admin_id in waiting_bonus:
        state = waiting_bonus[admin_id]

        if not message.text:
            await message.answer(
                "Отправь ID и сумму обычным текстом."
            )
            return

        text = message.text.strip()
        parts = text.split()

        # Можно прислать ID и сумму одним сообщением.
        if (
            state.get("stage") == "telegram_id"
            and len(parts) >= 2
        ):
            try:
                telegram_id = int(parts[0])
            except ValueError:
                await message.answer(
                    "Telegram ID должен состоять из цифр."
                )
                return

            amount_text = "".join(parts[1:])
            amount = parse_money_amount(
                amount_text
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

        # Сначала принимаем только ID.
        if state.get("stage") == "telegram_id":
            try:
                telegram_id = int(text)
            except ValueError:
                await message.answer(
                    (
                        "Telegram ID должен состоять из цифр.\n"
                        "Пример: 123456789"
                    )
                )
                return

            state["telegram_id"] = telegram_id
            state["stage"] = "amount"

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
                        known_user["telegram_first_name"],
                        known_user["telegram_last_name"],
                    )
                    if part
                ).strip()

                username = (
                    f"@{known_user['username']}"
                    if known_user["username"]
                    else "без username"
                )

                current_manual = int(
                    known_user["manual_spend"] or 0
                )

                await message.answer(
                    (
                        "✅ Пользователь найден\n\n"
                        f"ID: {telegram_id}\n"
                        f"Имя: {full_name or '-'}\n"
                        f"Username: {username}\n"
                        f"Текущая ручная сумма: "
                        f"{current_manual:,} ฿\n\n"
                        "Теперь отправь новую сумму.\n"
                        "Пример: 5000"
                    ).replace(",", " ")
                )
            else:
                await message.answer(
                    (
                        f"ID принят: {telegram_id}\n\n"
                        "В базе бота пользователь пока не найден, "
                        "но сумма всё равно будет записана "
                        "в его личный кабинет.\n\n"
                        "Теперь отправь сумму.\n"
                        "Пример: 5000"
                    )
                )

            return

        # Принимаем сумму после ID.
        if state.get("stage") == "amount":
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
                state["telegram_id"]
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
        )["client_id"]

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
                "Не удалось отправить клиенту %s: %s",
                client_id,
                exc,
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
            "source_chat_id": message.chat.id,
            "source_message_id": message.message_id,
        }

        target_count = await get_broadcast_target_count(
            "advertising"
        )

        try:
            await bot.copy_message(
                chat_id=ADMIN_CHAT_ID,
                from_chat_id=message.chat.id,
                message_id=message.message_id,
                reply_markup=build_broadcast_confirm_keyboard(),
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

    if message.text in [
        ASK_BTN_TEXT,
        MENU_BTN_TEXT,
    ]:
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

    run_fake_server(PORT)

    schedule_restart()

    logger.info(
        "Бот запущен и готов сохранять пользователей"
    )

    try:
        await dp.start_polling(bot)
    finally:
        if db_pool:
            await db_pool.close()

        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
