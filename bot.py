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
from zoneinfo import ZoneInfo
import aiohttp

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

ADMIN_CHAT_ID   = int(os.getenv("ADMIN_CHAT_ID", "7309681026"))
RESTART_MINUTES = int(os.getenv("RESTART_MINUTES", "420"))

# === Логирование ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# === Инициализация бота и диспетчера ===
bot = Bot(token=API_TOKEN)
dp  = Dispatcher()

def run_fake_server(port: int = 8080):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
    threading.Thread(target=HTTPServer(('', port), Handler).serve_forever, daemon=True).start()

def schedule_restart():
    def _restart():
        os.execv(sys.executable, [sys.executable] + sys.argv)
    timer = threading.Timer(RESTART_MINUTES * 60, _restart)
    timer.daemon = True
    timer.start()

# === /start ===
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    web_app_btn = types.KeyboardButton(
        text="📋 Открыть меню",
        web_app=types.WebAppInfo(url="https://v0-index-sepia.vercel.app")
    )
    keyboard = types.ReplyKeyboardMarkup(
        keyboard=[[web_app_btn]],
        resize_keyboard=True
    )
    await message.answer(
        "Добро пожаловать в Smoke Factory BBQ!\nНажмите кнопку ниже, чтобы открыть меню.",
        reply_markup=keyboard
    )
    logger.info(f"Пользователь {message.from_user.id} нажал /start")

# === Web App Data ===
@dp.message(F.content_type == ContentType.WEB_APP_DATA)
async def handle_order(message: types.Message):
    logger.info("===== ПОЛУЧЕН ЗАКАЗ ОТ WEB APP =====")
    raw = message.web_app_data.data
    logger.info(f"Сырой data: {raw}")

    try:
        data = json.loads(raw)
        pay_method = data.get('payMethod', 'не выбран')
        user       = message.from_user
        username   = f"@{user.username}" if user.username else user.full_name or "Без имени"
        phone      = data.get('phone', 'не указан')
        address    = data.get('address', 'не указан')
        delivery   = data.get('delivery', 0)
        total      = data.get('total', 0)
        items      = data.get('items', {})

        # === КОММЕНТАРИЙ: берём из любых полей и чистим ведущие ';' ===
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
            except:
                when_str = f"{data['orderDate']} {data['orderTime']}"

        lines = []
        order_items = []
        for name, info in items.items():
            qty   = info.get("qty", 0)
            price = info.get("price", 0)
            lines.append(f"- {name} ×{qty} = {qty*price} ฿")
            order_items.append({"name": name, "qty": qty, "price": price})
        items_text = "\n".join(lines)

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
        # === КОММЕНТАРИЙ менеджеру отдельной строкой ===
        if comment:
            admin_text += f"• <i>Комментарий:</i> {comment}\n"

        admin_text += f"\n🍽 <b>Состав заказа:</b>\n{items_text}\n\n💰 <b>Итого:</b> {total} ฿"
        await bot.send_message(ADMIN_CHAT_ID, admin_text, parse_mode="HTML")
        logger.info("Заказ отправлен админу")

        client_text = (
            "📦 Ваш заказ принят!\n\n"
            f"Имя: {username}\nТелефон: {phone}\nАдрес: {address}\n"
            f"Оплата: {pay_method}\nДоставка: {delivery} ฿\n"
        )
        if when_str:
            client_text += f"Время: {when_str}\n"
        if comment:
            client_text += f"Комментарий: {comment}\n"
        client_text += f"\n🧾 Состав заказа:\n{items_text}\n\n💰 Итого: {total} ฿\n\nМы скоро свяжемся с вами для подтверждения заказа!"
        await message.answer(client_text)

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
            # дублируем комментарий — на случай, если печать/сервер ждёт другое поле
            "comment":       comment,
            "comments":      comment,
            "comment_text":  comment,
            "note":          comment,
            "notes":         comment,
        }
        async with aiohttp.ClientSession() as sess:
            async with sess.post("https://290c42f7da4e.ngrok-free.app/order", json=payload) as resp:
                _ = await resp.text()  # читаем тело, чтобы аккуратно закрыть соединение
                if resp.status == 200:
                    logger.info("Печать отправлена")
                else:
                    logger.error(f"Ошибка печати: HTTP {resp.status}")

    except Exception:
        logger.exception("Ошибка обработки заказа")
        await message.answer("⚠️ Произошла ошибка при оформлении заказа.")

async def main():
    logger.info("=== Запуск бота Smoke Factory BBQ ===")
    # Если токен неправильный — delete_webhook может бросить NotFound.
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        logger.error(f"delete_webhook error: {e}")
    run_fake_server(8080)
    schedule_restart()
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(main())


