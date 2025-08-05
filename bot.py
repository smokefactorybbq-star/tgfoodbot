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
API_TOKEN      = os.getenv("TELEGRAM_BOT_TOKEN", "7557856598:AAFcJkyfj21_dYN_C9-_978G7rGhVZOfo6M")
ADMIN_CHAT_ID  = int(os.getenv("ADMIN_CHAT_ID", "7309681026"))
RESTART_MINUTES = 120

# === Логирование ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# === Инициализация бота и диспетчера ===
bot = Bot(token=API_TOKEN)
dp  = Dispatcher()  # без аргументов!

def run_fake_server(port: int = 8080):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
    server = HTTPServer(('', port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()

def schedule_restart():
    def _restart():
        os.execv(sys.executable, [sys.executable] + sys.argv)
    threading.Timer(RESTART_MINUTES * 60, _restart, daemon=True).start()

# === Хендлер на /start ===
@dp.message.register(Command("start"))
async def cmd_start(message: types.Message):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    web_app_btn = types.KeyboardButton(
        text="📋 Открыть меню",
        web_app=types.WebAppInfo(url="https://v0-index-sepia.vercel.app")
    )
    keyboard.add(web_app_btn)
    await message.answer(
        "Добро пожаловать в Smoke Factory BBQ!\nНажмите кнопку ниже, чтобы открыть меню.",
        reply_markup=keyboard
    )
    logger.info(f"Пользователь {message.from_user.id} нажал /start")

# === Хендлер WebApp данных ===
@dp.message.register(F.content_type == ContentType.WEB_APP_DATA)
async def handle_order(message: types.Message):
    logger.info("===== ПОЛУЧЕН ЗАКАЗ ОТ WEB APP =====")
    raw = message.web_app_data.data
    logger.info(f"Сырой data: {raw}")

    try:
        data = json.loads(raw)
        # Собираем поля заказа
        pay_method = data.get('payMethod', 'не выбран')
        user       = message.from_user
        username   = f"@{user.username}" if user.username else user.full_name or "Без имени"
        phone      = data.get('phone', 'не указан')
        address    = data.get('address', 'не указан')
        delivery   = data.get('delivery', 0)
        total      = data.get('total', 0)
        items      = data.get('items', {})

        # Время заказа
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

        # Состав заказа
        lines = []
        order_items = []
        for name, info in items.items():
            qty   = info.get("qty", 0)
            price = info.get("price", 0)
            lines.append(f"- {name} ×{qty} = {qty*price} ฿")
            order_items.append({"name": name, "qty": qty, "price": price})
        items_text = "\n".join(lines)

        # Сообщение админу
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
        admin_text += f"\n🍽 <b>Состав заказа:</b>\n{items_text}\n\n💰 <b>Итого:</b> {total} ฿"
        await bot.send_message(chat_id=ADMIN_CHAT_ID, text=admin_text, parse_mode="HTML")
        logger.info("Заказ отправлен админу")

        # Ответ клиенту
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
        client_text += f"\n🧾 Состав заказа:\n{items_text}\n\n💰 Итого: {total} ฿\n\nМы скоро свяжемся!"
        await bot.send_message(chat_id=message.chat.id, text=client_text)

        # Отправка на печать
        payload = {
            "name":       username,
            "phone":      phone,
            "address":    address,
            "delivery":   delivery,
            "payment":    pay_method,
            "items":      order_items,
            "total":      total,
            "date":       datetime.now(ZoneInfo("Asia/Bangkok")).strftime("%Y-%m-%d %H:%M:%S"),
            "order_time": when_str
        }
        async with aiohttp.ClientSession() as sess:
            resp = await sess.post("https://9c7ad82f72b9.ngrok-free.app/order", json=payload)
            if resp.status == 200:
                logger.info("Заказ отправлен в чековую программу")
            else:
                logger.error(f"Ошибка печати: HTTP {resp.status}")

    except Exception:
        logger.exception("Ошибка при обработке заказа")
        await message.answer("⚠️ Произошла ошибка при оформлении заказа.")

# === Главная функция ===
async def main():
    logger.info("=== Запуск бота Smoke Factory BBQ ===")
    run_fake_server(8080)
    schedule_restart()
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
