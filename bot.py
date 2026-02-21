import os
import sys
import json
import logging
import asyncio
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from zoneinfo import ZoneInfo

import aiohttp
from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ContentType
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

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

PRINT_URL        = os.getenv("PRINT_URL", "https://1ea2-171-6-239-140.ngrok-free.app/order")

# === Инициализация бота и диспетчера ===
bot = Bot(token=API_TOKEN)
dp  = Dispatcher()

# === Память: кому мы уже показывали клавиатуру (в рамках текущего запуска процесса) ===
KEYBOARD_SHOWN_USERS = set()

# === Память: режим "жду текст от менеджера для клиента" ===
# waiting_reply[admin_id] = {"client_id": int}
waiting_reply = {}


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
        keyboard=[[web_app_btn], [ask_btn]],
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


# === Сервисные команды ===
@dp.message(Command("myid"))
async def cmd_myid(message: types.Message):
    await message.answer(
        f"chat.id = <code>{message.chat.id}</code>\nfrom_user.id = <code>{message.from_user.id}</code>",
        parse_mode="HTML"
    )

@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message):
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    if message.from_user.id in waiting_reply:
        waiting_reply.pop(message.from_user.id, None)
        await message.answer("✅ Отменено. Больше не жду текст.")
    else:
        await message.answer("Нет активного режима ответа.")


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
    msg = call.message
    await msg.answer("Ок. Возвращаю кнопки меню 👇", reply_markup=start_keyboard())
    KEYBOARD_SHOWN_USERS.add(call.from_user.id)
    await call.answer()


# === Кнопка под заказом у менеджера: "Написать клиенту" ===
@dp.callback_query(F.data.startswith("write_client:"))
async def cb_write_client(call: types.CallbackQuery):
    # чтобы никто кроме админа не мог нажать
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
        "✍️ Напишите текст, который нужно отправить клиенту.\n"
        "Отмена: /cancel"
    )
    await call.answer("Ок, жду текст")


# === Менеджер вводит текст — бот пересылает клиенту ===
@dp.message(F.from_user.id == ADMIN_CHAT_ID)
async def admin_text_router(message: types.Message):
    if message.from_user.id in waiting_reply and message.text and not message.text.startswith("/"):
        info = waiting_reply.pop(message.from_user.id)
        client_id = info["client_id"]

        try:
            await bot.send_message(
                chat_id=client_id,
                text=f"💬 Сообщение от менеджера:\n\n{message.text}"
            )
            await message.answer("✅ Сообщение отправлено клиенту.")
        except Exception as e:
            logger.exception(f"Не удалось отправить клиенту {client_id}: {e}")
            await message.answer(
                "⚠️ Не получилось отправить клиенту.\n"
                "Обычно причины: клиент заблокировал бота или не нажимал /start."
            )
        return


def build_admin_order_kb(client_id: int) -> types.InlineKeyboardMarkup:
    """
    Две кнопки:
    1) Открыть профиль/чат клиента (tg://user?id=...)
    2) Написать клиенту через бота (ввод текста -> отправка клиенту)
    """
    kb = InlineKeyboardBuilder()

    # Открыть профиль/чат клиента (может работать не везде, но часто работает)
    kb.button(text="👤 Открыть профиль клиента", url=f"tg://user?id={client_id}")

    # Написать через бота
    kb.button(text="✍️ Написать клиенту", callback_data=f"write_client:{client_id}")

    kb.adjust(1)
    return kb.as_markup()


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
        client_id  = user.id

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
            f"• <i>User ID:</i> <code>{client_id}</code>\n"
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

        # Отправляем админу + две кнопки
        await bot.send_message(
            ADMIN_CHAT_ID,
            admin_text,
            parse_mode="HTML",
            reply_markup=build_admin_order_kb(client_id)
        )
        logger.info("Заказ отправлен админу + кнопки профиль/написать")

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

        await message.answer(client_text, reply_markup=start_keyboard())
        KEYBOARD_SHOWN_USERS.add(client_id)

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
            async with sess.post(PRINT_URL, json=payload) as resp:
                _ = await resp.text()
                if resp.status == 200:
                    logger.info("Печать отправлена")
                else:
                    logger.error(f"Ошибка печати: HTTP {resp.status}")

    except Exception:
        logger.exception("Ошибка обработки заказа")
        await message.answer("⚠️ Произошла ошибка при оформлении заказа.", reply_markup=start_keyboard())
        KEYBOARD_SHOWN_USERS.add(message.from_user.id)


# === Показывать клавиатуру только если мы ещё не показывали её пользователю ===
@dp.message()
async def ensure_keyboard_if_missing(message: types.Message):
    if message.content_type == ContentType.WEB_APP_DATA:
        return
    if message.text == ASK_BTN_TEXT:
        return

    shown = await send_main_keyboard(
        message,
        "Выберите действие 👇",
        force=False
    )
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
