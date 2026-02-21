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

# === Настройки окружения ===
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

# (опционально) куда сохранять связку order->user, чтобы переживать рестарты
STATE_FILE       = os.getenv("STATE_FILE", "orders_state.json")

# === Инициализация бота и диспетчера ===
bot = Bot(token=API_TOKEN)
dp  = Dispatcher()

# === Память: кому уже показывали клавиатуру ===
KEYBOARD_SHOWN_USERS = set()

# === Память: ожидаем текст ответа менеджера клиенту ===
# waiting_admin_reply[admin_id] = {"user_id": int, "order_no": str|None}
waiting_admin_reply = {}

# === Память: связка admin_message_id -> user_id (чтобы можно было отвечать "reply" на сообщение заказа)
admin_msg_to_user = {}


def load_state():
    global admin_msg_to_user
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            # ключи json — строки, приведем к int
            admin_msg_to_user = {int(k): int(v) for k, v in data.get("admin_msg_to_user", {}).items()}
            logger.info(f"STATE loaded: {len(admin_msg_to_user)} links")
    except Exception:
        logger.exception("Failed to load state")


def save_state():
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(
                {"admin_msg_to_user": {str(k): v for k, v in admin_msg_to_user.items()}},
                f,
                ensure_ascii=False,
                indent=2
            )
    except Exception:
        logger.exception("Failed to save state")


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


# === Админ: нажал "Ответить клиенту" (через бота) ===
@dp.callback_query(F.data.startswith("reply_to_user:"))
async def cb_reply_to_user(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_CHAT_ID:
        await call.answer("Недостаточно прав", show_alert=True)
        return

    try:
        user_id_str = call.data.split(":", 1)[1]
        user_id = int(user_id_str)
    except Exception:
        await call.answer("Ошибка данных", show_alert=True)
        return

    # попытаемся вытащить номер заказа из текста админ-сообщения (если есть)
    order_no = None
    try:
        if call.message and call.message.text:
            txt = call.message.text
            # если у вас есть формат orderNo, можно улучшить парсер
            # сейчас просто не обязательная штука
            order_no = None
    except Exception:
        pass

    waiting_admin_reply[call.from_user.id] = {"user_id": user_id, "order_no": order_no}

    await call.message.answer(
        "📝 Напишите сообщение клиенту одним текстом.\n"
        "Чтобы отменить — отправьте /cancel"
    )
    await call.answer("Ок, жду текст")


# === Админ: /cancel ===
@dp.message(Command("cancel"))
async def cancel_waiting(message: types.Message):
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    if message.from_user.id in waiting_admin_reply:
        waiting_admin_reply.pop(message.from_user.id, None)
        await message.answer("✅ Отменено.")
    else:
        await message.answer("Нет активного ответа.")


# === Админ: отправил текст, когда бот ждёт сообщение для клиента ===
@dp.message(F.from_user.id == ADMIN_CHAT_ID)
async def admin_text_router(message: types.Message):
    """
    ДВА режима:
    1) Если менеджер нажал кнопку "Ответить клиенту" — ждём обычный текст и шлём клиенту.
    2) Если менеджер сделал reply (ответом) на сообщение заказа — попробуем по message_id найти user_id и переслать текст.
    """
    # (1) режим ожидания после кнопки
    if message.from_user.id in waiting_admin_reply and message.text and not message.text.startswith("/"):
        info = waiting_admin_reply.pop(message.from_user.id)
        user_id = info["user_id"]

        try:
            await bot.send_message(
                chat_id=user_id,
                text=f"💬 Сообщение от менеджера:\n\n{message.text}"
            )
            await message.answer("✅ Отправлено клиенту.")
        except Exception as e:
            logger.error(f"Cannot send to user {user_id}: {e}")
            await message.answer(
                "⚠️ Не получилось отправить.\n"
                "Причины обычно такие: пользователь заблокировал бота или не нажимал /start."
            )
        return

    # (2) режим: менеджер ответил (reply) на админ-сообщение о заказе
    if message.reply_to_message and message.text and not message.text.startswith("/"):
        replied_id = message.reply_to_message.message_id
        user_id = admin_msg_to_user.get(replied_id)
        if user_id:
            try:
                await bot.send_message(
                    chat_id=user_id,
                    text=f"💬 Сообщение от менеджера:\n\n{message.text}"
                )
                await message.answer("✅ Отправлено клиенту (через reply).")
            except Exception as e:
                logger.error(f"Cannot send to user {user_id}: {e}")
                await message.answer(
                    "⚠️ Не получилось отправить.\n"
                    "Причины обычно такие: пользователь заблокировал бота или не нажимал /start."
                )
            return

    # иначе: ничего не делаем (чтобы не мешать)
    return


def build_admin_buttons(user_id: int) -> types.InlineKeyboardMarkup:
    """
    1) tg://user?id=... — открывает профиль/чат на телефоне/десктопе (работает не везде, но часто работает)
    2) Ответить через бота — гарантированно работает, если бот может писать пользователю
    """
    kb = InlineKeyboardBuilder()

    # Открыть чат с пользователем (скрытый аккаунт ок, главное знать user_id)
    kb.button(text="✉️ Открыть чат с клиентом", url=f"tg://user?id={user_id}")

    # Ответить через бота
    kb.button(text="📝 Ответить клиенту через бота", callback_data=f"reply_to_user:{user_id}")

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
        user_id    = user.id

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
            f"• <i>User ID:</i> <code>{user_id}</code>\n"
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

        # отправляем админу + кнопки для связи с клиентом
        admin_msg = await bot.send_message(
            ADMIN_CHAT_ID,
            admin_text,
            parse_mode="HTML",
            reply_markup=build_admin_buttons(user_id)
        )

        # сохраняем связку message_id админского сообщения -> user_id клиента
        admin_msg_to_user[admin_msg.message_id] = user_id
        save_state()

        logger.info("Заказ отправлен админу (с кнопками для ответа)")

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
            "Если нужно уточнение — менеджер напишет вам здесь в Telegram."
        )

        # ✅ После заказа показываем клавиатуру принудительно
        await message.answer(client_text, reply_markup=start_keyboard())
        KEYBOARD_SHOWN_USERS.add(user_id)

        # отправка на печать
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
            async with sess.post("https://1ea2-171-6-239-140.ngrok-free.app/order", json=payload) as resp:
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
    # Заказы уже обрабатывает handle_order
    if message.content_type == ContentType.WEB_APP_DATA:
        return

    # Нажатие на ASK_BTN_TEXT уже обрабатывает open_manager_chat
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
    load_state()

    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        logger.error(f"delete_webhook error: {e}")

    run_fake_server(8080)
    schedule_restart()

    await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    asyncio.run(main())
