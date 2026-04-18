import os
import sys
import json
import logging
import asyncio
import threading
import html
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from zoneinfo import ZoneInfo

import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ContentType
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

# === Логирование ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
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

ADMIN_CHAT_ID    = int(os.getenv("ADMIN_CHAT_ID", "7309681026"))
RESTART_MINUTES  = int(os.getenv("RESTART_MINUTES", "420"))

MANAGER_URL      = os.getenv("MANAGER_URL", "https://t.me/SmokefactoryBBQ")
WEBAPP_URL       = os.getenv("WEBAPP_URL", "https://v0-index-sepia.vercel.app")
ASK_BTN_TEXT     = "💬 Задать вопрос менеджеру"
PRINT_URL        = os.getenv("PRINT_URL", "https://bd37-171-6-238-195.ngrok-free.app/order")

bot = Bot(token=API_TOKEN)
dp  = Dispatcher()

KEYBOARD_SHOWN_USERS = set()
waiting_reply = {}  # waiting_reply[admin_id] = {"client_id": int}


def run_fake_server(port: int = 8080):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")

    threading.Thread(target=HTTPServer(("", port), Handler).serve_forever, daemon=True).start()


def schedule_restart():
    def _restart():
        os.execv(sys.executable, [sys.executable] + sys.argv)

    timer = threading.Timer(RESTART_MINUTES * 60, _restart)
    timer.daemon = True
    timer.start()


def start_keyboard() -> types.ReplyKeyboardMarkup:
    web_app_btn = types.KeyboardButton(text="📋 Открыть меню", web_app=types.WebAppInfo(url=WEBAPP_URL))
    ask_btn = types.KeyboardButton(text=ASK_BTN_TEXT)
    return types.ReplyKeyboardMarkup(keyboard=[[web_app_btn], [ask_btn]], resize_keyboard=True)


async def send_main_keyboard(message: types.Message, text: str, force: bool = False):
    uid = message.from_user.id
    if (uid not in KEYBOARD_SHOWN_USERS) or force:
        await message.answer(text, reply_markup=start_keyboard())
        KEYBOARD_SHOWN_USERS.add(uid)
        return True
    return False


def safe_int(x, default=0):
    try:
        return int(x)
    except Exception:
        return default


def safe_str(x, default=""):
    try:
        if x is None:
            return default
        return str(x)
    except Exception:
        return default


def build_admin_kb_full(client_id: int) -> types.InlineKeyboardMarkup:
    """Пробуем 2 кнопки: профиль + написать"""
    kb = InlineKeyboardBuilder()
    kb.button(text="👤 Открыть профиль клиента", url=f"tg://user?id={client_id}")
    kb.button(text="✍️ Написать клиенту", callback_data=f"write_client:{client_id}")
    kb.adjust(1)
    return kb.as_markup()


def build_admin_kb_safe(client_id: int) -> types.InlineKeyboardMarkup:
    """Безопасный вариант: только написать"""
    kb = InlineKeyboardBuilder()
    kb.button(text="✍️ Написать клиенту", callback_data=f"write_client:{client_id}")
    kb.adjust(1)
    return kb.as_markup()


async def send_order_to_admin(admin_text_html: str, client_id: int):
    """
    1) Пытаемся отправить с кнопкой "профиль"
    2) Если Telegram ругается BUTTON_USER_PRIVACY_RESTRICTED -> отправляем без неё
    """
    try:
        await bot.send_message(
            ADMIN_CHAT_ID,
            admin_text_html,
            parse_mode="HTML",
            reply_markup=build_admin_kb_full(client_id)
        )
        logger.info("ADMIN: sent with full kb (profile+reply)")
    except Exception as e:
        err = str(e)
        logger.error(f"ADMIN send failed (full kb): {err}")

        # Ключевой фикс
        if "BUTTON_USER_PRIVACY_RESTRICTED" in err:
            logger.warning("Privacy restricted: resend without profile button")
            await bot.send_message(
                ADMIN_CHAT_ID,
                admin_text_html,
                parse_mode="HTML",
                reply_markup=build_admin_kb_safe(client_id)
            )
            logger.info("ADMIN: sent with SAFE kb (reply only)")
        else:
            # если ошибка другая — просто пробрасываем, чтобы лог было видно
            raise


# === Команды ===
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await send_main_keyboard(
        message,
        "Нажмите кнопку ниже, чтобы открыть меню.\n"
        "Если есть вопросы — нажмите «💬 Задать вопрос менеджеру».",
        force=True
    )


@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message):
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    if message.from_user.id in waiting_reply:
        waiting_reply.pop(message.from_user.id, None)
        await message.answer("✅ Отменено.")
    else:
        await message.answer("Нет активного режима ответа.")


@dp.message(F.text == ASK_BTN_TEXT)
async def open_manager_chat(message: types.Message):
    kb = InlineKeyboardBuilder()
    kb.button(text="👉 Открыть чат менеджера", url=MANAGER_URL)
    kb.button(text="⬅️ Назад в меню", callback_data="back_to_menu")
    kb.adjust(1)
    await message.answer("Открой чат менеджера по кнопке ниже 👇", reply_markup=kb.as_markup())


@dp.callback_query(F.data == "back_to_menu")
async def back_to_menu(call: types.CallbackQuery):
    await call.message.answer("Ок. Возвращаю кнопки меню 👇", reply_markup=start_keyboard())
    KEYBOARD_SHOWN_USERS.add(call.from_user.id)
    await call.answer()


# === Кнопка "Написать клиенту" ===
@dp.callback_query(F.data.startswith("write_client:"))
async def cb_write_client(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_CHAT_ID:
        await call.answer("Недостаточно прав", show_alert=True)
        return

    try:
        client_id = int(call.data.split(":", 1)[1])
    except Exception:
        await call.answer("Ошибка данных", show_alert=True)
        return

    waiting_reply[call.from_user.id] = {"client_id": client_id}
    await call.message.answer("✍️ Напишите текст клиенту.\nОтмена: /cancel")
    await call.answer("Жду текст")


# === Менеджер вводит текст -> отправка клиенту ===
@dp.message(F.from_user.id == ADMIN_CHAT_ID)
async def admin_text_router(message: types.Message):
    if message.from_user.id in waiting_reply and message.text and not message.text.startswith("/"):
        client_id = waiting_reply.pop(message.from_user.id)["client_id"]
        try:
            await bot.send_message(client_id, f"💬 Сообщение от менеджера:\n\n{message.text}")
            await message.answer("✅ Отправлено клиенту.")
        except Exception as e:
            logger.exception(f"Не удалось отправить клиенту {client_id}: {e}")
            await message.answer("⚠️ Не получилось отправить (клиент мог заблокировать бота).")
        return


# === WebApp Data (ЗАКАЗЫ) ===
@dp.message(F.content_type == ContentType.WEB_APP_DATA)
async def handle_order(message: types.Message):
    logger.info("===== ПОЛУЧЕН ЗАКАЗ ОТ WEB APP =====")
    raw = message.web_app_data.data
    logger.info(f"RAW: {raw}")

    # JSON parse
    try:
        data = json.loads(raw)
    except Exception:
        logger.exception("JSON parse error")
        await message.answer("⚠️ Ошибка данных заказа.", reply_markup=start_keyboard())
        return

    user = message.from_user
    client_id = user.id

    pay_method = safe_str(data.get("payMethod", "не выбран"), "не выбран")
    username = f"@{user.username}" if user.username else (user.full_name or "Без имени")

    phone = safe_str(data.get("phone", "не указан"), "не указан")
    address = safe_str(data.get("address", "не указан"), "не указан")
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
            dt = datetime.strptime(str(raw_date), "%Y-%m-%d") if raw_date else datetime.now(ZoneInfo("Asia/Bangkok"))
            when_str = f"{dt.strftime('%d.%m')}, ближайшее"
        elif data.get("orderDate") and data.get("orderTime"):
            dt = datetime.strptime(str(data["orderDate"]), "%Y-%m-%d")
            when_str = f"{dt.strftime('%d.%m')} в {data['orderTime']}"
    except Exception:
        logger.exception("when_str parse error")
        when_str = ""

    lines = []
    order_items = []
    for name, info in items.items():
        if not isinstance(info, dict):
            continue
        qty = safe_int(info.get("qty", 0), 0)
        price = safe_int(info.get("price", 0), 0)
        lines.append(f"- {name} ×{qty} = {qty * price} ฿")
        order_items.append({"name": safe_str(name, ""), "qty": qty, "price": price})
    items_text = "\n".join(lines) if lines else "—"

    # Клиенту подтверждение
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
    client_text += f"\n🧾 Состав заказа:\n{items_text}\n\n💰 Итого: {total} ฿"

    await message.answer(client_text, reply_markup=start_keyboard())
    KEYBOARD_SHOWN_USERS.add(client_id)

    # Админу (экранируем HTML)
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
        admin_text += f"• <i>Время заказа:</i> {html.escape(when_str)}\n"
    if comment:
        admin_text += f"• <i>Комментарий:</i> {html.escape(comment)}\n"
    admin_text += f"\n🍽 <b>Состав заказа:</b>\n{html.escape(items_text)}\n\n💰 <b>Итого:</b> {total} ฿"

    # КЛЮЧЕВОЕ: отправка админу с фолбэком по privacy
    try:
        await send_order_to_admin(admin_text, client_id)
    except Exception:
        logger.exception("ADMIN send failed окончательно (даже без profile кнопки)")

    # Печать — отдельно
    payload = {
        "name": username,
        "phone": phone,
        "address": address,
        "delivery": delivery,
        "payment": pay_method,
        "items": order_items,
        "total": total,
        "date": datetime.now(ZoneInfo("Asia/Bangkok")).strftime("%Y-%m-%d %H:%M:%S"),
        "order_time": when_str,
        "comment": comment,
        "comments": comment,
        "comment_text": comment,
        "note": comment,
        "notes": comment,
    }

    try:
        timeout = aiohttp.ClientTimeout(total=7)
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            async with sess.post(PRINT_URL, json=payload) as resp:
                _ = await resp.text()
                if resp.status == 200:
                    logger.info("Печать отправлена")
                else:
                    logger.error(f"Ошибка печати: HTTP {resp.status}")
    except Exception:
        logger.exception("Print send error")


@dp.message()
async def ensure_keyboard_if_missing(message: types.Message):
    if message.content_type == ContentType.WEB_APP_DATA:
        return
    if message.text == ASK_BTN_TEXT:
        return
    await send_main_keyboard(message, "Выберите действие 👇", force=False)


async def main():
    logger.info("=== Запуск бота Smoke Factory BBQ ===")
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        logger.error(f"delete_webhook error: {e}")

    run_fake_server(8080)
    schedule_restart()
    await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    asyncio.run(main())
