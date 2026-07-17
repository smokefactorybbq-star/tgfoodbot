import os
import sys
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
from aiogram.enums import ContentType
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

# === Логирование ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# === Настройки ===
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

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

KEYBOARD_SHOWN_USERS: set[int] = set()
waiting_reply: dict[int, dict[str, int]] = {}
db_pool: asyncpg.Pool | None = None


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

            CREATE TABLE IF NOT EXISTS visits (
                id BIGSERIAL PRIMARY KEY,
                telegram_id BIGINT REFERENCES users(telegram_id) ON DELETE SET NULL,
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
            """
        )

    logger.info("База данных подключена, таблицы готовы")


async def upsert_user(user: types.User | None) -> None:
    if not db_pool or not user:
        return

    await db_pool.execute(
        """
        INSERT INTO users (
            telegram_id,
            username,
            telegram_first_name,
            telegram_last_name,
            created_at,
            updated_at,
            last_bot_activity_at
        )
        VALUES ($1, $2, $3, $4, NOW(), NOW(), NOW())
        ON CONFLICT (telegram_id)
        DO UPDATE SET
            username = EXCLUDED.username,
            telegram_first_name = EXCLUDED.telegram_first_name,
            telegram_last_name = EXCLUDED.telegram_last_name,
            updated_at = NOW(),
            last_bot_activity_at = NOW()
        """,
        user.id,
        user.username,
        user.first_name,
        user.last_name,
    )


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

    delivery = max(0, safe_int(data.get("delivery", 0)))
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
        safe_int(data.get("discount", data.get("discountAmount", 0))),
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
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15
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
                    FROM visits v, day_bounds d
                    WHERE v.visited_at >= d.start_utc
                      AND v.visited_at < d.end_utc
                ) AS visits,
                (
                    SELECT COUNT(DISTINCT telegram_id)
                    FROM visits v, day_bounds d
                    WHERE v.visited_at >= d.start_utc
                      AND v.visited_at < d.end_utc
                      AND telegram_id IS NOT NULL
                ) AS unique_visitors,
                (
                    SELECT COUNT(*)
                    FROM users u, day_bounds d
                    WHERE u.created_at >= d.start_utc
                      AND u.created_at < d.end_utc
                ) AS new_users,
                (
                    SELECT COUNT(*)
                    FROM orders o, day_bounds d
                    WHERE o.created_at >= d.start_utc
                      AND o.created_at < d.end_utc
                ) AS orders_count,
                (
                    SELECT COUNT(DISTINCT telegram_id)
                    FROM orders o, day_bounds d
                    WHERE o.created_at >= d.start_utc
                      AND o.created_at < d.end_utc
                ) AS buyers,
                (
                    SELECT COALESCE(SUM(total), 0)
                    FROM orders o, day_bounds d
                    WHERE o.created_at >= d.start_utc
                      AND o.created_at < d.end_utc
                ) AS revenue,
                (
                    SELECT COALESCE(AVG(total), 0)
                    FROM orders o, day_bounds d
                    WHERE o.created_at >= d.start_utc
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
    today = datetime.now(ZoneInfo("Asia/Bangkok")).strftime("%d.%m.%Y")

    return (
        f"📊 Статистика за {today}\n\n"
        f"Открытий сайта: {visits}\n"
        f"Уникальных посетителей: {unique_visitors}\n"
        f"Новых пользователей: {int(row['new_users'] or 0)}\n\n"
        f"Заказов: {orders_count}\n"
        f"Покупателей: {int(row['buyers'] or 0)}\n"
        f"Конверсия: {conversion:.1f}%\n\n"
        f"Выручка: {int(row['revenue'] or 0)} ฿\n"
        f"Средний чек: {round(float(row['avg_check'] or 0))} ฿"
    )


# ============================================================================
# HEALTHCHECK И ПЛАНОВЫЙ ПЕРЕЗАПУСК
# ============================================================================
def run_fake_server(port: int = PORT) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")

        def log_message(self, format: str, *args) -> None:
            return

    server = HTTPServer(("", port), Handler)
    threading.Thread(
        target=server.serve_forever,
        daemon=True,
    ).start()


def schedule_restart() -> None:
    def _restart() -> None:
        os.execv(sys.executable, [sys.executable] + sys.argv)

    timer = threading.Timer(RESTART_MINUTES * 60, _restart)
    timer.daemon = True
    timer.start()


# ============================================================================
# ПОДПИСАННАЯ ССЫЛКА MINI APP
#
# KeyboardButton Mini App сохраняется, поэтому Telegram.WebApp.sendData
# продолжает отправлять заказ в бот. Для личного кабинета пользователь
# передаётся через подписанный токен в URL. Сервер mini-app проверяет подпись
# тем же TELEGRAM_BOT_TOKEN.
# ============================================================================
def build_signed_webapp_url(user: types.User) -> str:
    # Короткие ключи уменьшают длину WebApp URL.
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

    token = base64.urlsafe_b64encode(payload_json).decode("ascii").rstrip("=")
    signature = hmac.new(
        API_TOKEN.encode("utf-8"),
        token.encode("ascii"),
        hashlib.sha256,
    ).hexdigest()

    parts = urlsplit(WEBAPP_URL)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.update({"u": token, "s": signature})

    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path or "/",
            urlencode(query),
            parts.fragment,
        )
    )


def start_keyboard(user: types.User) -> types.ReplyKeyboardMarkup:
    web_app_btn = types.KeyboardButton(
        text=MENU_BTN_TEXT,
        web_app=types.WebAppInfo(
            url=build_signed_webapp_url(user)
        ),
    )

    ask_btn = types.KeyboardButton(text=ASK_BTN_TEXT)

    return types.ReplyKeyboardMarkup(
        keyboard=[
            [web_app_btn],
            [ask_btn],
        ],
        resize_keyboard=True,
    )


def updated_keyboard(user: types.User) -> types.ReplyKeyboardMarkup:
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


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================
def safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def safe_str(value, default: str = "") -> str:
    try:
        if value is None:
            return default
        return str(value)
    except Exception:
        return default


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
        logger.info("ADMIN: sent with full kb (profile+reply)")
    except Exception as exc:
        error_text = str(exc)
        logger.error("ADMIN send failed (full kb): %s", error_text)

        if "BUTTON_USER_PRIVACY_RESTRICTED" in error_text:
            logger.warning(
                "Privacy restricted: resend without profile button"
            )
            await bot.send_message(
                ADMIN_CHAT_ID,
                admin_text_html,
                parse_mode="HTML",
                reply_markup=build_admin_kb_safe(client_id),
            )
            logger.info("ADMIN: sent with SAFE kb (reply only)")
            return

        raise


# ============================================================================
# КОМАНДЫ И КНОПКИ
# ============================================================================
@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    await upsert_user(message.from_user)
    await send_main_keyboard(
        message,
        "Нажмите кнопку ниже, чтобы открыть меню.\n"
        "Если есть вопросы — нажмите «💬 Задать вопрос менеджеру».",
        force=True,
    )


@dp.message(F.text == MENU_BTN_TEXT)
async def refresh_menu_keyboard(message: types.Message) -> None:
    """
    Обрабатывает старую текстовую кнопку, которая могла остаться
    у пользователя от предыдущей версии бота.
    """
    await upsert_user(message.from_user)
    await message.answer(
        "✅ Кнопка меню обновлена. Нажмите её ещё раз.",
        reply_markup=updated_keyboard(message.from_user),
    )
    KEYBOARD_SHOWN_USERS.add(message.from_user.id)


@dp.message(Command("nu4etam"))
async def cmd_daily_report(message: types.Message) -> None:
    await upsert_user(message.from_user)

    if message.from_user.id != ADMIN_CHAT_ID:
        return

    try:
        await message.answer(await build_daily_report())
    except Exception:
        logger.exception("Ошибка формирования отчёта")
        await message.answer("⚠️ Не удалось сформировать отчёт.")


@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message) -> None:
    if message.from_user.id != ADMIN_CHAT_ID:
        return

    if message.from_user.id in waiting_reply:
        waiting_reply.pop(message.from_user.id, None)
        await message.answer("✅ Отменено.")
    else:
        await message.answer("Нет активного режима ответа.")


@dp.message(F.text == ASK_BTN_TEXT)
async def open_manager_chat(message: types.Message) -> None:
    await upsert_user(message.from_user)

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
async def back_to_menu(call: types.CallbackQuery) -> None:
    await upsert_user(call.from_user)
    await call.message.answer(
        "Ок. Возвращаю кнопки меню 👇",
        reply_markup=start_keyboard(call.from_user),
    )
    KEYBOARD_SHOWN_USERS.add(call.from_user.id)
    await call.answer()


@dp.callback_query(F.data.startswith("write_client:"))
async def cb_write_client(call: types.CallbackQuery) -> None:
    await upsert_user(call.from_user)

    if call.from_user.id != ADMIN_CHAT_ID:
        await call.answer("Недостаточно прав", show_alert=True)
        return

    try:
        client_id = int(call.data.split(":", 1)[1])
    except Exception:
        await call.answer("Ошибка данных", show_alert=True)
        return

    waiting_reply[call.from_user.id] = {"client_id": client_id}
    await call.message.answer(
        "✍️ Напишите текст клиенту.\nОтмена: /cancel"
    )
    await call.answer("Жду текст")


@dp.message(F.from_user.id == ADMIN_CHAT_ID, F.text)
async def admin_text_router(message: types.Message) -> None:
    is_waiting = message.from_user.id in waiting_reply
    is_text = bool(message.text)
    is_command = bool(message.text and message.text.startswith("/"))

    if not is_waiting or not is_text or is_command:
        return

    client_id = waiting_reply.pop(message.from_user.id)["client_id"]

    try:
        await bot.send_message(
            client_id,
            f"💬 Сообщение от менеджера:\n\n{message.text}",
        )
        await message.answer("✅ Отправлено клиенту.")
    except Exception as exc:
        logger.exception(
            "Не удалось отправить клиенту %s: %s",
            client_id,
            exc,
        )
        await message.answer(
            "⚠️ Не получилось отправить. Клиент мог заблокировать бота."
        )


# ============================================================================
# ЗАКАЗЫ ИЗ TELEGRAM WEB APP
# ============================================================================
@dp.message(F.content_type == ContentType.WEB_APP_DATA)
async def handle_order(message: types.Message) -> None:
    logger.info("===== ПОЛУЧЕН ЗАКАЗ ОТ WEB APP =====")

    raw = message.web_app_data.data
    logger.info("RAW: %s", raw)

    try:
        data = json.loads(raw)
    except Exception:
        logger.exception("JSON parse error")
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

    phone = safe_str(data.get("phone", "не указан"), "не указан")
    address = safe_str(
        data.get("address", "не указан"),
        "не указан",
    )
    delivery = safe_int(data.get("delivery", 0), 0)
    total = safe_int(data.get("total", 0), 0)

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
    comment = safe_str(comment, "").strip().lstrip(";")

    when_str = ""
    try:
        if data.get("orderWhen") == "soonest":
            raw_date = data.get("orderDate")
            dt = (
                datetime.strptime(str(raw_date), "%Y-%m-%d")
                if raw_date
                else datetime.now(ZoneInfo("Asia/Bangkok"))
            )
            when_str = f"{dt.strftime('%d.%m')}, ближайшее"
        elif data.get("orderDate") and data.get("orderTime"):
            dt = datetime.strptime(
                str(data["orderDate"]),
                "%Y-%m-%d",
            )
            when_str = (
                f"{dt.strftime('%d.%m')} в {data['orderTime']}"
            )
    except Exception:
        logger.exception("when_str parse error")
        when_str = ""

    lines: list[str] = []
    order_items: list[dict] = []

    for name, info in items.items():
        if not isinstance(info, dict):
            continue

        qty = safe_int(info.get("qty", 0), 0)
        price = safe_int(info.get("price", 0), 0)

        lines.append(f"- {name} ×{qty} = {qty * price} ฿")
        order_items.append(
            {
                "name": safe_str(name, ""),
                "qty": qty,
                "price": price,
                "img": safe_str(info.get("img"), ""),
            }
        )

    items_text = "\n".join(lines) if lines else "—"

    try:
        saved_order_id = await save_order_to_database(
            user,
            data,
            order_items,
        )
        logger.info("Заказ сохранён в БД, id=%s", saved_order_id)
    except Exception:
        logger.exception("Не удалось сохранить заказ в БД")

    client_text = (
        "📦 Ваш заказ принят!\n\n"
        f"Имя: {username}\n"
        f"Телефон: {phone}\n"
        f"Адрес: {address}\n"
        f"Оплата: {pay_method}\n"
        f"Доставка: {delivery} ฿\n"
    )

    if when_str:
        client_text += f"Время: {when_str}\n"

    if comment:
        client_text += f"Комментарий: {comment}\n"

    client_text += (
        f"\n🧾 Состав заказа:\n{items_text}"
        f"\n\n💰 Итого: {total} ฿"
    )

    await message.answer(
        client_text,
        reply_markup=start_keyboard(user),
    )
    KEYBOARD_SHOWN_USERS.add(client_id)

    admin_text = (
        "✅ <b>Новый заказ</b>\n"
        f"• <i>Пользователь:</i> {html.escape(username)}\n"
        f"• <i>User ID:</i> <code>{client_id}</code>\n"
        f"• <i>Телефон:</i> {html.escape(phone)}\n"
        f"• <i>Адрес:</i> {html.escape(address)}\n"
        f"• <i>Доставка:</i> {delivery} ฿\n"
        f"• <i>Оплата:</i> {html.escape(pay_method)}\n"
    )

    if when_str:
        admin_text += (
            f"• <i>Время заказа:</i> {html.escape(when_str)}\n"
        )

    if comment:
        admin_text += (
            f"• <i>Комментарий:</i> {html.escape(comment)}\n"
        )

    admin_text += (
        f"\n🍽 <b>Состав заказа:</b>\n{html.escape(items_text)}"
        f"\n\n💰 <b>Итого:</b> {total} ฿"
    )

    try:
        await send_order_to_admin(admin_text, client_id)
    except Exception:
        logger.exception(
            "ADMIN send failed окончательно, даже без profile кнопки"
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
            ZoneInfo("Asia/Bangkok")
        ).strftime("%Y-%m-%d %H:%M:%S"),
        "order_time": when_str,
        "comment": comment,
        "comments": comment,
        "comment_text": comment,
        "note": comment,
        "notes": comment,
    }

    try:
        timeout = aiohttp.ClientTimeout(total=7)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                PRINT_URL,
                json=print_payload,
            ) as response:
                await response.text()

                if response.status == 200:
                    logger.info("Печать отправлена")
                else:
                    logger.error(
                        "Ошибка печати: HTTP %s",
                        response.status,
                    )
    except Exception:
        logger.exception("Print send error")


@dp.message()
async def ensure_keyboard_if_missing(message: types.Message) -> None:
    await upsert_user(message.from_user)

    if message.content_type == ContentType.WEB_APP_DATA:
        return

    if message.text in [ASK_BTN_TEXT, MENU_BTN_TEXT]:
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
    logger.info("=== Запуск бота Smoke Factory BBQ ===")
    logger.info("WEBAPP_URL=%s", WEBAPP_URL)

    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception as exc:
        logger.error("delete_webhook error: %s", exc)

    await init_database()
    run_fake_server(PORT)
    schedule_restart()

    try:
        await dp.start_polling(bot)
    finally:
        if db_pool:
            await db_pool.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())

