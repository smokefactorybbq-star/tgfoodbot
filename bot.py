import os
import sys
import json
import logging
import asyncio
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ContentType
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder
from zoneinfo import ZoneInfo
import aiohttp

# === Логирование (сначала!) ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# === Настройки ===
try:
    from dotenv import load_dotenv  # pip install python-dotenv (по желанию)
    load_dotenv()
except Exception:
    pass

API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not API_TOKEN:
    logger.critical("ERROR: переменная окружения TELEGRAM_BOT_TOKEN не установлена")
    sys.exit(1)

ADMIN_CHAT_ID    = int(os.getenv("ADMIN_CHAT_ID", "7309681026"))
RESTART_MINUTES  = int(os.getenv("RESTART_MINUTES", "420"))

# ссылка на чат менеджера (личка: https://t.me/username, группа: invite link)
MANAGER_URL      = os.getenv("MANAGER_URL", "https://t.me/SmokefactoryBBQ")

WEBAPP_URL       = os.getenv("WEBAPP_URL", "https://v0-index-sepia.vercel.app")

ASK_BTN_TEXT     = "💬 Задать вопрос менеджеру"

# === Инициализация бота и диспетчера ===
bot = Bot(token=API_TOKEN)
dp  = Dispatcher()

# === Память: кому мы уже показывали клавиатуру (в рамках текущего запуска процесса) ===
KEYBOARD_SHOWN_USERS = set()


def run_fake_server(port: int = 8080):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")

    threading.Thread(
        target=HTTPServer(("", port), Handler).serve_forever,
        daemon=True
    ).start()


def schedule_restart():
    def _restart():
        os.execv(sys.executable, [sys.executable] + sys.argv)

    timer = threading.Timer(RESTART_MINUTES * 60, _restart)
    timer.daemon = True
    timer.start()


def start_keyboard() -> types.ReplyKeyboardMarkup:
    web_app_btn = types.KeyboardButton(
        text="📋 Открыть меню",
        web_app=types.WebAppInfo(url=WEBAPP_URL)
    )
    ask_btn = types.KeyboardButton(text=ASK_BTN_TEXT)

    return types.ReplyKeyboardMarkup(
        keyboard=[[web_app_btn], [ask_btn]],  # "задать вопрос" ниже
        resize_keyboard=True
    )


async def send_main_keyboard(message: types.Message, text: str, force: bool = False):
    """
    Показываем клавиатуру только если:
    - force=True (принудительно)
    - или пользователю ещё не показывали клавиатуру в этом запуске.
    """
    uid = message.from_user.id
    if (uid not in KEYBOARD_SHOWN_USERS) or force:
        await message.answer(text, reply_markup=start_keyboard())
        KEYBOARD_SHOWN_USERS.add(uid)
        return True
    return False


# === /start ===
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await send_main_keyboard(
        message,
        "Нажмите кнопку ниже, чтобы открыть меню.\n"
        "Если есть вопросы — нажмите «💬 Задать вопрос менеджеру».",
        force=True
    )
    logger.info(f"Пользователь {message.from_user.id} нажал /start")


# === Кнопка: Задать вопрос менеджеру -> ссылка в чат менеджера ===
@dp.message(F.text == ASK_BTN_TEXT)
async def open_manager_chat(message: types.Message):
    kb = InlineKeyboardBuilder()
    kb.button(text="👉 Открыть чат менеджера", url=MANAGER_URL)
    kb.button(text="⬅️ Назад в меню", callback_data="back_to_menu")
    kb.adjust(1)

    await message.answer(
        "Открой чат менеджера по кнопке ниже 👇",
        reply_markup=kb.as_markup()
    )


# === Назад в меню (inline) ===
@dp.callback_query(F.data == "back_to_menu")
async def back_to_menu(call: types.CallbackQuery):
    # Показываем клавиатуру принудительно, чтобы точно появилась
    msg = call.message
    await msg.answer("Ок. Возвращаю кнопки меню 👇", reply_markup=start_keyboard())
    KEYBOARD_SHOWN_USERS.add(call.from_user.id)
    await call.answer()


# === Web App Data (ЗАКАЗЫ) ===
@dp.message(F.content_type == ContentType.WEB_APP_DATA)
async def handle_order(message: types.Message):
    logger.info("===== ПОЛУЧЕН ЗАКАЗ ОТ WEB APP =====")
    raw = message.web_app_data.data
    logger.info(f"Сырой data: {raw}")

    try:
        data = json.loads(raw)
        pay_method = data.get("payMethod", "не выбран")
        user       = message.from_user
        username   = f"@{user.username}" if user.username else (user.full_name or "Без имени")
        phone      = data.get("phone", "не указан")
        address    = data.get("address", "не указан")
        delivery   = data.get("delivery", 0)
        total      = data.get("total", 0)
        items      = data.get("items", {})

        comment = (
            data.get("comment")
            or data.get("comments")
            or data.get("comment_text")
            or data.get("note")
            or data.get("notes")
            or ""
        )
        comment = str(comment).strip().lstrip(";")

        when_str = ""
        if data.get("orderWhen") == "soonest":
            raw_date = data.get("orderDate")
            dt = datetime.strptime(raw_date, "%Y-%m-%d") if raw_date else datetime.now(ZoneInfo("Asia/Bangkok"))
            when_str = f"{dt.strftime('%d.%m')}, ближайшее"
        elif data.get("orderDate") and data.get("orderTime"):
            try:
                dt = datetime.strptime(data["orderDate"], "%Y-%m-%d")
                when_str = f"{dt.strftime('%d.%m')} в {data['orderTime']}"
            except Exception:
                when_str = f"{data.get('orderDate')} {data.get('orderTime')}"

        lines = []
        order_items = []
        for name, info in items.items():
            qty   = int(info.get("qty", 0) or 0)
            price = int(info.get("price", 0) or 0)
            lines.append(f"- {name} ×{qty} = {qty * price} ฿")
            order_items.append({"name": name, "qty": qty, "price": price})
        items_text = "\n".join(lines) if lines else "—"

        admin_text = (
            "✅ <b>Новый заказ</b>\n"
            f"• <i>Пользователь:</i> {username}\n"
            f"• <i>Телефон:</i> {phone}\n"
            f"• <i>Адрес:</i> {address}\n"
            f"• <i>Доставка:</i> {delivery} ฿\n"
            f"• <i>Оплата:</i> {pay_method}\n"
        )
        if when_str:
            admin_text += f"• <i>Время заказа:</i> {when_str}\n"
        if comment:
            admin_text += f"• <i>Комментарий:</i> {comment}\n"

        admin_text += f"\n🍽 <b>Состав заказа:</b>\n{items_text}\n\n💰 <b>Итого:</b> {total} ฿"

        await bot.send_message(ADMIN_CHAT_ID, admin_text, parse_mode="HTML")
        logger.info("Заказ отправлен админу")

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
            f"\n🧾 Состав заказа:\n{items_text}\n\n"
            f"💰 Итого: {total} ฿\n\n"
            "Мы скоро свяжемся с вами для подтверждения заказа!"
        )

        # ✅ После заказа показываем клавиатуру принудительно
        await message.answer(client_text, reply_markup=start_keyboard())
        KEYBOARD_SHOWN_USERS.add(message.from_user.id)

        payload = {
            "name":       username,
            "phone":      phone,
            "address":    address,
            "delivery":   delivery,
            "payment":    pay_method,
            "items":      order_items,
            "total":      total,
            "date":       datetime.now(ZoneInfo("Asia/Bangkok")).strftime("%Y-%m-%d %H:%M:%S"),
            "order_time": when_str,
            "comment":      comment,
            "comments":     comment,
            "comment_text": comment,
            "note":         comment,
            "notes":        comment,
        }

        async with aiohttp.ClientSession() as sess:
            async with sess.post("https://34c3-171-6-238-175.ngrok-free.app/order", json=payload) as resp:
                _ = await resp.text()
                if resp.status == 200:
                    logger.info("Печать отправлена")
                else:
                    logger.error(f"Ошибка печати: HTTP {resp.status}")

    except Exception:
        logger.exception("Ошибка обработки заказа")
        # если ошибка — тоже покажем клавиатуру
        await message.answer("⚠️ Произошла ошибка при оформлении заказа.", reply_markup=start_keyboard())
        KEYBOARD_SHOWN_USERS.add(message.from_user.id)


# === Показывать клавиатуру только если мы ещё не показывали её пользователю ===
@dp.message()
async def ensure_keyboard_if_missing(message: types.Message):
    # Заказы уже обрабатывает handle_order
    if message.content_type == ContentType.WEB_APP_DATA:
        return

    # Нажатие на ASK_BTN_TEXT уже обрабатывает open_manager_chat
    if message.text == ASK_BTN_TEXT:
        return

    # Если пользователю ещё не показывали клавиатуру — покажем.
    shown = await send_main_keyboard(
        message,
        "Выберите действие 👇",
        force=False
    )

    # Если клавиатура уже была показана — НЕ отвечаем (чтобы не спамить)
    if not shown:
        return


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




