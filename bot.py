import os
import sys
import json
import logging
import asyncio
from aiogram import Bot, Dispatcher, types, executor
from datetime import datetime, timedelta
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

def run_fake_server(port=8080):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
    server = HTTPServer(('', port), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
# === Настройки ===
API_TOKEN = os.getenv("API_TOKEN") or "7557856598:AAFcJkyfj21_dYN_C9-_978G7rGhVZOfo6M"
ADMIN_CHAT_ID = 7309681026
RESTART_MINUTES = 120

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)
run_fake_server(port=8080)
def schedule_restart():
    def _restart():
        python = sys.executable
        os.execv(python, [python] + sys.argv)
    import threading
    timer = threading.Timer(RESTART_MINUTES * 60, _restart)
    timer.daemon = True
    timer.start()

@dp.message_handler(commands=["start"])
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
    logger.info(f"Пользователь {message.from_user.id} начал работу с ботом.")

@dp.message_handler(content_types=types.ContentType.WEB_APP_DATA)
async def handle_order(message: types.Message):
    logger.info("===== ПОЛУЧЕН ЗАКАЗ ОТ WEB APP =====")
    logger.info(f"Сырой data из WebApp: {message.web_app_data.data}")

    try:
        data = json.loads(message.web_app_data.data)
        logger.info(f"Декодированные данные заказа: {data}")

        pay_method = data.get('payMethod', 'не выбран')
        user = message.from_user
        username = f"@{user.username}" if user.username else (user.full_name or "Без имени")
        phone = data.get('phone', 'не указан')
        address = data.get('address', 'не указан')
        delivery = data.get('delivery', 0)
        total = data.get('total', 0)
        items = data.get('items', {})

        # === Время заказа из корзины ===
        when_str = ""
        if data.get("orderWhen") == "soonest":
            try:
                order_date = data.get('orderDate')
                if order_date:
                    d = datetime.strptime(order_date, "%Y-%m-%d")
                    when_str = f"{d.strftime('%d.%m')}, ближайшее"
                else:
                    d = datetime.now() + timedelta(hours=7)
                    when_str = f"{d.strftime('%d.%m')}, ближайшее"
            except Exception:
                d = datetime.now() + timedelta(hours=7)
                when_str = f"{d.strftime('%d.%m')}, ближайшее"
        elif data.get('orderDate') and data.get('orderTime'):
            try:
                d = datetime.strptime(data['orderDate'], "%Y-%m-%d")
                when_str = f"{d.strftime('%d.%m')} в {data['orderTime']}"
            except Exception:
                when_str = f"{data['orderDate']} {data['orderTime']}"

        # Формирование текста заказа
        item_lines = []
        order_items = []
        for name, info in items.items():
            qty = info.get('qty', 0)
            price = info.get('price', 0)
            line_sum = qty * price
            item_lines.append(f"- {name} ×{qty} = {line_sum} ฿")
            order_items.append({"name": name, "qty": qty, "price": price})

        items_text = "\n".join(item_lines)

        # Сообщение для менеджера
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
        admin_text += (
            "\n🍽 <b>Состав заказа:</b>\n" +
            items_text +
            f"\n\n💰 <b>Итого (с доставкой):</b> {total} ฿"
        )
        await bot.send_message(chat_id=ADMIN_CHAT_ID, text=admin_text, parse_mode="HTML")
        logger.info("Заказ успешно отправлен админу.")

        # Сообщение для клиента
        client_text = (
            "📦 Ваш заказ успешно принят!\n\n"
            f"Имя: {username}\n"
            f"Телефон: {phone}\n"
            f"Адрес: {address}\n"
            f"Оплата: {pay_method}\n"
            f"Доставка: {delivery} ฿\n"
        )
        if when_str:
            client_text += f"Время заказа: {when_str}\n"
        client_text += (
            f"\n🧾 Состав заказа:\n{items_text}\n\n"
            f"💰 Итого: {total} ฿\n\n"
            "Мы скоро свяжемся с вами для подтверждения!"
        )
        await bot.send_message(chat_id=message.chat.id, text=client_text)

        # === Время для печати чека: используем локальное время Таиланда ===
        from zoneinfo import ZoneInfo
        dt_bkk = datetime.now(ZoneInfo("Asia/Bangkok"))
        order_payload = {
            "name": username,
            "phone": phone,
            "address": address,
            "delivery": delivery,
            "payment": pay_method,
            "items": order_items,
            "total": total,
            "date": dt_bkk.strftime("%Y-%m-%d %H:%M:%S"),
            "order_time": when_str  # Время заказа для чека
        }
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.post("https://9c7ad82f72b9.ngrok-free.app/order", json=order_payload) as resp:
                    if resp.status == 200:
                        logger.info("✅ Заказ успешно отправлен в чековую программу.")
                    else:
                        logger.error(f"❌ Ошибка при отправке заказа в чековую программу: HTTP {resp.status}")
        except Exception as e:
            logger.exception("❌ Ошибка при подключении к чековой программе")

    except Exception as e:
        logger.error(f"Ошибка при обработке заказа: {e}")
        await message.answer("⚠️ Произошла ошибка при оформлении заказа.")

async def on_startup(_):
    logger.info("=== Запуск бота Smoke Factory BBQ ===")
    schedule_restart()

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)
