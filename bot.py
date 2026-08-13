# -*- coding: utf-8 -*-
import os
import json
import threading
import asyncio
import queue
import traceback
import winsound
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse
import tkinter as tk
from tkinter import messagebox, simpledialog
import win32print
import win32ui
from PIL import Image, ImageWin, ImageDraw, ImageFont, ImageOps
from datetime import datetime, date
import calendar
import re
from pathlib import Path

# NIIMBOT загружается мягко: даже если библиотека bleak ещё не установлена,
# вся прежняя чековая программа продолжит запускаться и работать как раньше.
try:
    from niimbot_b1_ble import print_file as print_niimbot_file
    NIIMBOT_IMPORT_ERROR = None
except Exception as exc:
    print_niimbot_file = None
    NIIMBOT_IMPORT_ERROR = exc

# ===================== ПУТИ / НАСТРОЙКИ =====================
BASE_DIR = Path(__file__).resolve().parent
os.chdir(BASE_DIR)                            # фиксируем рабочую папку
ORDERS_DIR = BASE_DIR / "orders"              # абсолютный путь к orders
ORDERS_DIR.mkdir(exist_ok=True)
SOUNDS_DIR = BASE_DIR / "sounds"
ALARM_SOUND = SOUNDS_DIR / "new_order.wav"

# ===================== СКРЫТАЯ GRAB-ПЕЧАТЬ =====================
# Интерфейс программы не меняется. Второй бот отправляет только номер заказа
# на /grab (или /grab-label), программа сохраняет его в папке GRAB и ставит
# этикетку в очередь на автоматическую печать.
GRAB_DIR = BASE_DIR / "GRAB"
GRAB_DIR.mkdir(exist_ok=True)
GRAB_TEMPLATE_PATH = BASE_DIR / "grab_template.png"
GRAB_BLE_ADDRESS = "23:0a:05:7b:99:bc"
GRAB_PRINT_DENSITY = 3
GRAB_IMAGE_THRESHOLD = 145
GRAB_LABEL_WIDTH = 384
GRAB_LABEL_HEIGHT = 240
GRAB_PRINT_QUEUE = queue.Queue()


MAX_LINE_WIDTH = 48
RECEIVER_HOST = "127.0.0.1"
RECEIVER_PORT = 8000     # ngrok: https://... -> http://localhost:8000

# ===================== СЛОВАРИ =====================
TRANSLATIONS = {
    "Борщ": "Borscht",
    "Солянка": "Solyanka",
    "Гороховый суп": "Peas soup",
    "Котлеты куриные": "Chicken cutlets",
    "Вареники с картошкой и беконом": "Vareniki with potato and bacon",
    "Пельмени": "Pelmeni",
    "Котлеты из домашнего фарша": "Meat cutlets",
    "Перец фаршированный": "Stuffed pepper",
    "Тефтели": "Meatballs",
    "Лепешка с сыром": "Cheese flatbread",
    "Паста карбонара": "Pasta carbonara",
    "Паста болоньезе": "Pasta Bolognese",
    "Ребра BBQ": "BBQ ribs",
    "Кебаб свинина-говядина": "Pork-beef kebab",
    "Кебаб из курицы": "Chicken kebab",
    "Шашлык из свинины": "Pork shashlik",
    "Пельмени 1кг": "Pelmeni 1kg",
    "Вареники 1кг": "Vareniki 1kg",
    "Копченая шейка": "Smoked pork collar",
    "Копченый бекон": "Smoked pork belly",
    "Колбаса краковская": "Krakow sausage",
    "Копченая курица": "Smoked chicken",
    "Копченая корейка": "Smoked pork loin",
    "Котлета по-киевски": "Cutlet Kiev",
    "Лепешка с картошкой": "Potato flatbread",
    "Салат Цезарь с копченой курицей": "Caesar salad",
    "Салат с тунцом": "Tuna salad",
    "Бефстроганов": "Beefstroganoff",
    "Лепешка с мясом": "Meat flatbread",
    "Картошка фри": "French fries",
    "Картошка дольками": "Potato wedges",
    "Мини чебуреки": "Mini Chebureki",
    "Салат баклажаны в кляре": "Eggplant salad",
    "Шашлык из курицы": "Chicken grill"
}

PAYMENT_MAP = {
    "Банк РФ": "Cash",
    "Thai bank": "PromptPay",
}

COMPANY_ADDRESS = "Address: 100/531 Village №5, Ratsada Subdistict, Mueang Phuket District, Phuket Province 83000"
COMPANY_TAX_ID  = "TAX ID: 0835567035228"

# ===================== УТИЛИТЫ =====================
def wrap_text(text: str, width: int):
    lines = []
    for line in str(text or "").split("\n"):
        while len(line) > width:
            lines.append(line[:width])
            line = line[width:]
        lines.append(line)
    return lines

def _parse_dt_safe(s: str):
    """Понимает даты из разных JSON и из разных имён файлов."""
    s = str(s or "").strip()
    if not s:
        return None

    # ISO-строки: 2025-06-30T20:13:55 или 2025-06-30 20:13:55
    iso_s = s.replace("T", " ").split("+")[0].split("Z")[0].strip()

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y-%m-%d_%H-%M-%S",
        "%Y-%m-%d_%H-%M",
        "%Y%m%d-%H%M%S",
        "%Y%m%d-%H%M",
        "%Y%m%d",
        "%d.%m.%Y %H:%M:%S",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y",
    ):
        try:
            return datetime.strptime(iso_s, fmt)
        except Exception:
            pass
    return None

def parse_dt_from_filename(filename: str):
    """
    Достаёт дату из имён файлов заказов:
      20260629-202410.json
      2025-06-30_20-13-55.json
    """
    stem = Path(filename).stem

    patterns = [
        r"(\d{8}-\d{6})",
        r"(\d{8}-\d{4})",
        r"(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})",
        r"(\d{4}-\d{2}-\d{2}_\d{2}-\d{2})",
        r"(\d{4}-\d{2}-\d{2})",
        r"(\d{8})",
    ]

    for pattern in patterns:
        m = re.search(pattern, stem)
        if m:
            dt = _parse_dt_safe(m.group(1))
            if dt:
                return dt
    return None

def iter_order_json_files():
    """
    Возвращает ВСЕ json-файлы из папки orders.
    Важно: Path.glob("*.json") на Windows может пропускать файлы с .JSON/.Json
    и не показывает, из какой именно папки программа читает orders.
    Поэтому используем iterdir() и проверку расширения без учета регистра.
    """
    files = []
    try:
        for p in ORDERS_DIR.iterdir():
            if p.is_file() and p.suffix.lower() == ".json":
                files.append(p)
    except Exception as e:
        print(f"Ошибка чтения папки orders: {ORDERS_DIR} — {e}")
    return files

def get_order_sort_dt(path: Path):
    """Дата сортировки: сначала date/orderDate внутри JSON, потом дата из имени, потом дата изменения файла."""
    try:
        with path.open(encoding="utf-8-sig") as h:
            data = json.load(h)
        dt = _parse_dt_safe(data.get("date", ""))
        if not dt:
            date_str = data.get("orderDate") or data.get("created_at") or data.get("createdAt") or data.get("time")
            dt = _parse_dt_safe(date_str)
        if dt:
            return dt
    except Exception:
        pass

    dt = parse_dt_from_filename(path.name)
    if dt:
        return dt

    try:
        return datetime.fromtimestamp(path.stat().st_mtime)
    except Exception:
        return datetime.min

def get_order_number(order: dict) -> str:
    """
    Чековая программа не создаёт и не вычисляет номер.
    Она использует только номер, который прислал бот.
    """
    return str(
        order.get("order_number")
        or order.get("orderNumber")
        or order.get("order_no")
        or order.get("orderNo")
        or ""
    ).strip()


def get_receipt_number(filename: str) -> int:
    """
    Номер чека считается только по JSON-файлам в папке orders:
    файлы сортируются по дате и получают порядковые номера с 1.

    Для нового заказа номер чека равен количеству JSON-файлов.
    Дополнительный SM-* от бота на расчёт не влияет.
    """
    records = []

    for path in iter_order_json_files():
        try:
            with path.open(encoding="utf-8-sig") as handle:
                data = json.load(handle)

            dt = (
                _parse_dt_safe(data.get("date", ""))
                or _parse_dt_safe(data.get("orderDate", ""))
                or parse_dt_from_filename(path.name)
                or datetime.fromtimestamp(path.stat().st_mtime)
            )
        except Exception:
            dt = datetime.min

        records.append(
            (
                dt,
                path.name.lower(),
                path.name,
            )
        )

    records.sort(
        key=lambda row: (
            row[0],
            row[1],
        )
    )

    target_name = Path(filename).name.lower()

    for receipt_number, (_, _, stored_name) in enumerate(
        records,
        start=1,
    ):
        if stored_name.lower() == target_name:
            return receipt_number

    return 0


def validate_order_number(order_number: str) -> bool:
    return bool(
        re.fullmatch(
            r"SM-[0-9]+",
            str(order_number or "").strip(),
        )
    )

def print_to_windows_printer(text: str):
    printer_name = win32print.GetDefaultPrinter()
    if not printer_name:
        raise RuntimeError("Не найден принтер по умолчанию")

    hdc = win32ui.CreateDC()
    hdc.CreatePrinterDC(printer_name)
    hdc.StartDoc("Smoke Factory BBQ — Receipt")
    hdc.StartPage()

    y = 10
    try:
        logo_path = BASE_DIR / "2.bmp"
        if logo_path.exists():
            img = Image.open(logo_path)
            dib = ImageWin.Dib(img)
            dib.draw(hdc.GetHandleOutput(), (80, y, 500, y + 350))
            y += 360
        else:
            y += 30
    except Exception as e:
        print(f"Ошибка логотипа: {e}")
        y += 30

    for line in wrap_text(text, MAX_LINE_WIDTH):
        try:
            hdc.TextOut(10, y, line)
        except Exception:
            hdc.TextOut(10, y, line.encode("ascii", "ignore").decode("ascii"))
        y += 50

    hdc.EndPage()
    hdc.EndDoc()
    hdc.DeleteDC()


def print_report_to_windows_printer(text: str):
    """
    Отдельная печать только для Daily/Montly reports.
    Обычные чеки НЕ меняет: логотип, шрифт и формат чеков остаются в print_to_windows_printer().
    Для отчетов используется компактная печать без логотипа и с переходом на новую страницу,
    чтобы длинный месячный отчет не обрезался.
    """
    printer_name = win32print.GetDefaultPrinter()
    if not printer_name:
        raise RuntimeError("Не найден принтер по умолчанию")

    hdc = win32ui.CreateDC()
    hdc.CreatePrinterDC(printer_name)
    hdc.StartDoc("Smoke Factory BBQ — Report")
    hdc.StartPage()

    # Компактный шрифт только для отчетов.
    try:
        font = win32ui.CreateFont({
            "name": "Courier New",
            "height": 22,
            "weight": 400,
        })
        hdc.SelectObject(font)
    except Exception as e:
        print(f"Ошибка шрифта отчёта: {e}")

    y = 20
    line_height = 30

    # Рабочая высота страницы. Если принтер вернул ошибочные параметры,
    # используем безопасный запас.
    try:
        page_height = hdc.GetDeviceCaps(10) - 100  # VERTRES
        if page_height <= 0:
            page_height = 1800
    except Exception:
        page_height = 1800

    for line in wrap_text(text, MAX_LINE_WIDTH):
        if y > page_height:
            hdc.EndPage()
            hdc.StartPage()
            y = 20
            try:
                hdc.SelectObject(font)
            except Exception:
                pass

        try:
            hdc.TextOut(10, y, line)
        except Exception:
            hdc.TextOut(10, y, line.encode("ascii", "ignore").decode("ascii"))
        y += line_height

    hdc.EndPage()
    hdc.EndDoc()
    hdc.DeleteDC()

def format_receipt(order: dict, filename: str) -> str:
    receipt_number = get_receipt_number(filename)
    order_number = get_order_number(order)

    if not order_number:
        order_number = (
            f"OLD-{Path(filename).stem}"
        )

    lines = [
        "Smoke Factory BBQ co., LTD",
        COMPANY_ADDRESS,
        COMPANY_TAX_ID,
        f"Receipt № {receipt_number}",
        f"Order № {order_number}",
        ""
    ]
    lines += [
        f"Date: {order.get('date','')}",
        f"Name: {order.get('name','')}",
        f"Phone: {order.get('phone','')}",
        f"Address: {order.get('address','')}",
        "-" * 32,
    ]

    for item in order.get("items", []):
        name = TRANSLATIONS.get(item.get('name',''), item.get('name',''))
        qty = float(item.get('qty', 0) or 0)
        price = float(item.get('price', 0) or 0)
        total_line = round(qty * price, 2)
        qty_disp = int(qty) if float(qty).is_integer() else qty
        lines.append(f"{name} x{qty_disp} = {total_line:.2f} ฿")

    delivery = float(
        order.get(
            "delivery",
            0,
        )
        or 0
    )

    total = float(
        order.get(
            "total",
            0,
        )
        or 0
    )

    discount_percent = int(
        order.get(
            "discount_percent",
            0,
        )
        or 0
    )

    discount_amount = float(
        order.get(
            "discount_amount",
            0,
        )
        or 0
    )

    lines.append(
        "-" * 32
    )

    if discount_amount > 0:
        lines.append(
            f"Discount ({discount_percent}%): "
            f"-{discount_amount:.2f} ฿"
        )

    lines += [
        f"Delivery: {delivery:.2f} ฿",
        f"Total: {total:.2f} ฿",
    ]

    raw_payment = order.get('payment','')
    mapped_payment = PAYMENT_MAP.get(raw_payment, raw_payment)
    lines.append(f"Payment: {mapped_payment}")

    lines += ["", "Thank you for your order!"]
    return "\n".join(lines)

def save_order_to_file(fname: str, order: dict):
    path = ORDERS_DIR / fname
    with path.open("w", encoding="utf-8") as f:
        json.dump(order, f, ensure_ascii=False, indent=2)


def find_saved_order_by_number(order_number: str):
    """
    Идемпотентность /order: один SM-* может быть сохранён только один раз.

    Возвращает имя уже существующего JSON, если такой номер заказа
    ранее был принят чековой программой. Повторный POST тогда безопасно
    получает HTTP 200, но новый файл и новое уведомление не создаются.
    """
    wanted = str(order_number or "").strip()
    if not wanted:
        return None

    for path in iter_order_json_files():
        try:
            with path.open(encoding="utf-8-sig") as handle:
                saved = json.load(handle)
            if get_order_number(saved) == wanted:
                return path.name
        except Exception:
            # Повреждённый старый файл не должен ломать приём новых заказов.
            continue

    return None

# ===================== СКРЫТАЯ GRAB-ПЕЧАТЬ: СЛУЖЕБНЫЕ ФУНКЦИИ =====================
def validate_grab_order_number(order_number: str) -> bool:
    """Этикетки печатаются для номеров вида GF-342 и SM-472."""
    return bool(
        re.fullmatch(
            r"(?:GF|SM)-[0-9]+",
            str(order_number or "").strip().upper(),
        )
    )


def _grab_log(message: str):
    """Пишет служебные ошибки в файл, не меняя интерфейс чековой программы."""
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}\n"
    try:
        with (GRAB_DIR / "grab_print.log").open("a", encoding="utf-8") as handle:
            handle.write(line)
    except Exception:
        pass
    try:
        print(message)
    except Exception:
        pass


def _find_label_font(size: int):
    """Arial на Windows; резервные варианты нужны только для совместимости."""
    candidates = [
        Path(os.environ.get("WINDIR", r"C:\Windows")) / "Fonts" / "arial.ttf",
        Path(os.environ.get("WINDIR", r"C:\Windows")) / "Fonts" / "segoeui.ttf",
        BASE_DIR / "arial.ttf",
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
    ]

    for candidate in candidates:
        try:
            if candidate.exists():
                return ImageFont.truetype(str(candidate), size=size)
        except Exception:
            pass

    return ImageFont.load_default()


def create_grab_label_image(order_number: str, output_path: Path):
    """
    Берёт исходный макет без изменения дизайна.
    Закрывается только старый номер на шаблоне и на его месте рисуется новый GF-* или SM-*.
    Затем изображение без растягивания приводится к рабочему полю B1 384x240.
    """
    if not GRAB_TEMPLATE_PATH.exists():
        raise FileNotFoundError(
            f"Не найден макет этикетки: {GRAB_TEMPLATE_PATH}"
        )

    number = str(order_number or "").strip().upper()
    image = Image.open(GRAB_TEMPLATE_PATH).convert("RGBA")
    draw = ImageDraw.Draw(image)

    # Координаты только области номера в оригинальном макете 800x480.
    # Вертикальная линия, рамка, логотип и текст ниже не затрагиваются.
    number_box = (338, 90, 698, 188)
    draw.rectangle(number_box, fill=(255, 255, 255, 255))

    box_width = number_box[2] - number_box[0]
    box_height = number_box[3] - number_box[1]

    font = None
    text_bbox = None

    # Подбираем размер автоматически: короткие и длинные GF-/SM-номера помещаются
    # в ту же область, где был исходный GF-342.
    for font_size in range(110, 39, -2):
        candidate = _find_label_font(font_size)
        bbox = draw.textbbox((0, 0), number, font=candidate)
        width = bbox[2] - bbox[0]
        height = bbox[3] - bbox[1]
        if width <= box_width - 4 and height <= box_height - 4:
            font = candidate
            text_bbox = bbox
            break

    if font is None:
        font = _find_label_font(40)
        text_bbox = draw.textbbox((0, 0), number, font=font)

    text_width = text_bbox[2] - text_bbox[0]
    text_height = text_bbox[3] - text_bbox[1]
    center_x = (number_box[0] + number_box[2]) / 2
    center_y = 139

    text_x = center_x - text_width / 2 - text_bbox[0]
    text_y = center_y - text_height / 2 - text_bbox[1]

    draw.text(
        (round(text_x), round(text_y)),
        number,
        font=font,
        fill=(0, 0, 0, 255),
    )

    # Макет 800x480 имеет физическое соотношение 50x30.
    # Печатающая головка B1 — 384 пикселя (48 мм), поэтому сохраняем пропорции
    # и симметрично убираем только непечатаемые края, не растягивая дизайн.
    resampling = getattr(Image, "Resampling", Image)
    prepared = ImageOps.fit(
        image.convert("RGB"),
        (GRAB_LABEL_WIDTH, GRAB_LABEL_HEIGHT),
        method=resampling.LANCZOS,
        centering=(0.5, 0.5),
    )
    prepared.save(output_path, "PNG")


def save_grab_number(order_number: str):
    """Сохраняет в GRAB только полученный номер заказа."""
    number = str(order_number or "").strip().upper()
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    safe_number = re.sub(r"[^A-Z0-9_-]+", "_", number)

    json_path = GRAB_DIR / f"{stamp}_{safe_number}.json"
    image_path = GRAB_DIR / f"{stamp}_{safe_number}.png"

    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(
            {"order_number": number},
            handle,
            ensure_ascii=False,
            indent=2,
        )

    return json_path, image_path


def queue_grab_label(order_number: str):
    """Сохраняет номер и ставит невидимую задачу печати в очередь."""
    json_path, image_path = save_grab_number(order_number)
    GRAB_PRINT_QUEUE.put(
        {
            "order_number": str(order_number).strip().upper(),
            "json_path": json_path,
            "image_path": image_path,
        }
    )
    return json_path


def grab_print_worker():
    """Последовательно печатает этикетки, чтобы BLE-задания не пересекались."""
    while True:
        task = GRAB_PRINT_QUEUE.get()

        try:
            order_number = task["order_number"]
            image_path = Path(task["image_path"])

            create_grab_label_image(order_number, image_path)

            if print_niimbot_file is None:
                raise RuntimeError(
                    "Не установлена библиотека bleak или не загружен "
                    f"niimbot_b1_ble.py: {NIIMBOT_IMPORT_ERROR}"
                )

            asyncio.run(
                print_niimbot_file(
                    str(image_path),
                    GRAB_BLE_ADDRESS,
                    density=GRAB_PRINT_DENSITY,
                    threshold=GRAB_IMAGE_THRESHOLD,
                    offset_y=0,
                )
            )

            _grab_log(f"Grab-этикетка напечатана: {order_number}")

        except Exception as exc:
            _grab_log(
                "Ошибка печати Grab-этикетки: "
                f"{exc}\n{traceback.format_exc()}"
            )
        finally:
            GRAB_PRINT_QUEUE.task_done()


# ===================== НОРМАЛИЗАЦИЯ ЗАКАЗА =====================
def canonicalize(src: dict) -> dict:
    """
    Поддерживает оба формата:
      A) WebApp: items = {name:{qty,price}}, payMethod, orderDate, orderTime
      B) Старый : items = [{name,qty,price}], payment, date
    """
    order = {}

    # Номер назначается ботом.
    order_number = str(
        src.get("order_number")
        or src.get("orderNumber")
        or src.get("order_no")
        or src.get("orderNo")
        or ""
    ).strip()

    if order_number:
        order["order_number"] = order_number

    # Дата
    date_str = src.get("date") or src.get("orderDate")
    time_str = src.get("orderTime", "")
    dt = None
    if date_str:
        if time_str:
            dt = _parse_dt_safe(f"{date_str} {time_str}")
        if not dt:
            dt = _parse_dt_safe(date_str)
    if not dt:
        dt = datetime.now()
    order["date"] = dt.strftime("%Y-%m-%d %H:%M:%S")

    # Контакты
    order["name"] = src.get("name", "")
    order["phone"] = str(src.get("phone", "") or "")
    order["address"] = src.get("address", "")

    # Позиции
    items = src.get("items") or []
    items_list = []
    if isinstance(items, dict):
        for ru_name, v in items.items():
            try:
                qty = float(v.get("qty", 0) or 0)
                price = float(v.get("price", 0) or 0)
            except Exception:
                qty, price = 0, 0
            if qty > 0:
                items_list.append({"name": ru_name, "qty": qty, "price": price})
    else:
        for it in items:
            ru_name = str(it.get("name", ""))
            try:
                qty = float(it.get("qty", 0) or 0)
                price = float(it.get("price", 0) or 0)
            except Exception:
                qty, price = 0, 0
            if qty > 0:
                items_list.append({"name": ru_name, "qty": qty, "price": price})
    order["items"] = items_list

    # Деньги
    delivery = float(
        src.get(
            "delivery",
            src.get(
                "deliveryFee",
                src.get(
                    "delivery_fee",
                    0,
                ),
            ),
        )
        or 0
    )

    subtotal = sum(
        float(i["qty"])
        * float(i["price"])
        for i in items_list
    )

    try:
        discount_percent = int(
            src.get(
                "discount_percent",
                src.get(
                    "discountPercent",
                    0,
                ),
            )
            or 0
        )
    except Exception:
        discount_percent = 0

    try:
        discount_amount = float(
            src.get(
                "discount_amount",
                src.get(
                    "discountAmount",
                    src.get(
                        "discount",
                        0,
                    ),
                ),
            )
            or 0
        )
    except Exception:
        discount_amount = 0.0

    calculated_total = (
        subtotal
        - discount_amount
        + delivery
    )

    try:
        claimed_total = float(
            src.get(
                "total",
                calculated_total,
            )
        )
    except Exception:
        claimed_total = calculated_total

    # Бот уже присылает итог после скидки.
    # Не вычитаем скидку второй раз.
    order["delivery"] = round(
        delivery,
        2,
    )

    order["items_total"] = round(
        subtotal,
        2,
    )

    order["discount_percent"] = max(
        0,
        discount_percent,
    )

    order["discount_amount"] = round(
        max(
            0.0,
            discount_amount,
        ),
        2,
    )

    order["total"] = round(
        claimed_total,
        2,
    )

    # Оплата
    order["payment"] = (
        src.get("payment")
        or src.get("payMethod")
        or ""
    )

    return order

# ===================== HTTP-ПРИЁМНИК =====================
class OrderHandler(BaseHTTPRequestHandler):
    server_version = "OrderReceiver/1.0"

    def _send(self, code=200, payload=None):
        body = json.dumps(payload or {"ok": True}, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        # Для туннеля надёжнее завершать каждый короткий HTTP-ответ явно.
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()
        self.close_connection = True

    def do_GET(self):
        if urlparse(self.path).path == "/status":
            return self._send(200, {"ok": True, "time": datetime.now().isoformat()})
        return self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        path = urlparse(self.path).path

        # Второй бот присылает сюда только номер заказа GF-* или SM-*.
        # Этот маршрут работает на том же порту 8000, что и прежний /order.
        if path in ("/grab", "/grab-label"):
            try:
                length = int(self.headers.get("Content-Length", "0"))

                if length <= 0 or length > 4096:
                    return self._send(
                        400,
                        {"ok": False, "error": "invalid content length"},
                    )

                raw = self.rfile.read(length)

                try:
                    src = json.loads(raw.decode("utf-8"))
                except Exception as e:
                    return self._send(
                        400,
                        {"ok": False, "error": f"bad json: {e}"},
                    )

                order_number = str(
                    src.get("order_number")
                    or src.get("orderNumber")
                    or src.get("order_no")
                    or src.get("orderNo")
                    or src.get("number")
                    or ""
                ).strip().upper()

                if not validate_grab_order_number(order_number):
                    return self._send(
                        400,
                        {
                            "ok": False,
                            "error": (
                                "order_number is required "
                                "in format GF-342 or SM-472"
                            ),
                        },
                    )

                saved_file = queue_grab_label(order_number)

                return self._send(
                    200,
                    {
                        "ok": True,
                        "queued": True,
                        "order_number": order_number,
                        "file": saved_file.name,
                    },
                )

            except Exception as e:
                _grab_log(
                    "Ошибка получения Grab-номера: "
                    f"{e}\n{traceback.format_exc()}"
                )
                return self._send(
                    500,
                    {"ok": False, "error": repr(e)},
                )

        # Ниже старый маршрут чековой программы оставлен без изменений.
        if path != "/order":
            return self._send(404, {"ok": False, "error": "not found"})

        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            try:
                src = json.loads(raw.decode("utf-8"))
            except Exception as e:
                return self._send(400, {"ok": False, "error": f"bad json: {e}"})

            order_number = str(
                src.get("order_number")
                or src.get("orderNumber")
                or src.get("order_no")
                or src.get("orderNo")
                or ""
            ).strip()

            if not validate_order_number(
                order_number
            ):
                return self._send(
                    400,
                    {
                        "ok": False,
                        "error": (
                            "order_number is required "
                            "in format SM-472"
                        ),
                    },
                )

            # Повторный запрос с тем же SM-* не должен создавать второй чек.
            # Это особенно важно, если заказ уже сохранился локально, но ngrok
            # потерял/заменил ответ и бот повторил POST.
            existing_file = find_saved_order_by_number(order_number)
            if existing_file:
                return self._send(
                    200,
                    {
                        "ok": True,
                        "duplicate": True,
                        "file": existing_file,
                        "order_number": order_number,
                    },
                )

            canon = canonicalize(src)
            canon["order_number"] = order_number

            # Имя JSON снова формируется как раньше — по дате и времени.
            # Дополнительный SM-* хранится только внутри JSON и никак
            # не влияет ни на имя файла, ни на номер чека.
            base_name = datetime.now().strftime(
                "%Y%m%d-%H%M%S"
            )

            fname = f"{base_name}.json"
            duplicate_index = 2

            while (ORDERS_DIR / fname).exists():
                fname = (
                    f"{base_name}_{duplicate_index}.json"
                )
                duplicate_index += 1

            save_order_to_file(
                fname,
                canon,
            )

            root.after(
                0,
                on_new_order_received,
            )

            return self._send(
                200,
                {
                    "ok": True,
                    "file": fname,
                    "order_number": order_number,
                },
            )
        except Exception as e:
            return self._send(500, {"ok": False, "error": repr(e)})

def run_receiver():
    httpd = HTTPServer((RECEIVER_HOST, RECEIVER_PORT), OrderHandler)
    print(f"Order receiver: http://{RECEIVER_HOST}:{RECEIVER_PORT} (/order, /status)")
    httpd.serve_forever()

# ===================== GUI =====================
root = tk.Tk()
root.title("Smoke Factory BBQ — Чеки")

frame = tk.Frame(root)
frame.pack(padx=10, pady=10)

listbox_frame = tk.Frame(frame)
listbox_frame.grid(row=0, column=0, rowspan=6, padx=5, pady=5, sticky="ns")

orders_listbox = tk.Listbox(listbox_frame, width=44, height=20)
orders_scrollbar = tk.Scrollbar(listbox_frame, orient="vertical", command=orders_listbox.yview)
orders_listbox.configure(yscrollcommand=orders_scrollbar.set)
orders_listbox.pack(side="left", fill="both", expand=True)
orders_scrollbar.pack(side="right", fill="y")

status_var = tk.StringVar(value=f"Папка orders: {ORDERS_DIR}")
tk.Label(frame, textvariable=status_var, anchor="w", fg="gray").grid(row=6, column=0, columnspan=6, sticky="ew", padx=5, pady=(0, 5))

editor_text = tk.Text(frame, width=60, height=20)
editor_text.grid(row=0, column=1, columnspan=5, padx=5, pady=5)

# ===================== УВЕДОМЛЕНИЕ О НОВОМ ЗАКАЗЕ =====================
alert_window = None

def start_alarm_sound():
    try:
        if ALARM_SOUND.exists():
            winsound.PlaySound(str(ALARM_SOUND), winsound.SND_FILENAME | winsound.SND_ASYNC | winsound.SND_LOOP)
        else:
            winsound.MessageBeep(winsound.MB_ICONEXCLAMATION)
    except Exception as e:
        print(f"Ошибка звука: {e}")

def stop_alarm_sound():
    try:
        winsound.PlaySound(None, winsound.SND_PURGE)
    except Exception as e:
        print(f"Ошибка остановки звука: {e}")

def show_new_order_alert():
    global alert_window

    if alert_window is not None and alert_window.winfo_exists():
        alert_window.lift()
        alert_window.focus_force()
        return

    alert_window = tk.Toplevel(root)
    alert_window.title("НОВЫЙ ЗАКАЗ")
    alert_window.transient(root)
    alert_window.grab_set()
    alert_window.resizable(False, False)

    tk.Label(
        alert_window,
        text="НОВЫЙ ЗАКАЗ",
        font=("Arial", 24, "bold"),
        padx=40,
        pady=25
    ).pack()

    def accept_order():
        global alert_window
        stop_alarm_sound()
        if alert_window is not None and alert_window.winfo_exists():
            alert_window.destroy()
        alert_window = None

    tk.Button(
        alert_window,
        text="Принять заказ",
        font=("Arial", 16, "bold"),
        command=accept_order,
        padx=30,
        pady=10
    ).pack(padx=25, pady=(0, 25), fill="x")

    alert_window.protocol("WM_DELETE_WINDOW", accept_order)
    alert_window.lift()
    alert_window.focus_force()

def on_new_order_received():
    refresh_order_list()
    start_alarm_sound()
    show_new_order_alert()

def load_orders():
    """Показывает в программе печати ВСЕ json из папки orders, даже если JSON битый/старого формата."""
    files = iter_order_json_files()
    files.sort(key=get_order_sort_dt, reverse=True)
    return [p.name for p in files]

def refresh_order_list():
    orders_listbox.delete(0, tk.END)
    files = load_orders()
    for f in files:
        orders_listbox.insert(tk.END, f)
    try:
        status_var.set(f"Папка orders: {ORDERS_DIR} | JSON: {len(files)}")
    except Exception:
        pass

def on_select(event=None):
    idxs = orders_listbox.curselection()
    if not idxs:
        return
    fname = orders_listbox.get(idxs[0])
    path = ORDERS_DIR / fname
    try:
        with path.open(encoding="utf-8-sig") as f:
            order = json.load(f)
        editor_text.delete("1.0", tk.END)
        editor_text.insert(tk.END, json.dumps(order, ensure_ascii=False, indent=2))
    except Exception as e:
        # Файл всё равно должен отображаться в списке. Если JSON битый — показываем текст как есть.
        try:
            raw = path.read_text(encoding="utf-8-sig", errors="replace")
            editor_text.delete("1.0", tk.END)
            editor_text.insert(tk.END, raw)
            messagebox.showwarning("JSON открыт как текст", f"Файл есть в orders, но JSON читается с ошибкой:\n{e}")
        except Exception as e2:
            messagebox.showerror("Ошибка", f"Открытие: {e}\nПовторная попытка: {e2}")

orders_listbox.bind("<<ListboxSelect>>", on_select)

def print_selected_order():
    idxs = orders_listbox.curselection()
    if not idxs:
        messagebox.showwarning("Внимание", "Сначала выберите заказ.")
        return
    fname = orders_listbox.get(idxs[0])
    try:
        raw = editor_text.get("1.0", tk.END)
        order = json.loads(raw)
        save_order_to_file(fname, order)  # сохранить правки
        receipt = format_receipt(order, fname)
        print_to_windows_printer(receipt)
    except Exception as e:
        messagebox.showerror("Ошибка", f"Печать: {e}")

def save_edited_order():
    idxs = orders_listbox.curselection()
    if not idxs:
        messagebox.showwarning("Внимание", "Сначала выберите заказ.")
        return
    fname = orders_listbox.get(idxs[0])
    try:
        order = json.loads(editor_text.get("1.0", tk.END))
        save_order_to_file(fname, order)
        messagebox.showinfo("Сохранено", "Изменения сохранены.")
        refresh_order_list()
    except Exception as e:
        messagebox.showerror("Ошибка", f"Сохранение: {e}")

def delete_selected_order():
    idxs = orders_listbox.curselection()
    if not idxs:
        return
    fname = orders_listbox.get(idxs[0])
    if messagebox.askyesno("Аннулировать", f"Удалить {fname}?"):
        try:
            (ORDERS_DIR / fname).unlink(missing_ok=True)
            refresh_order_list()
            editor_text.delete("1.0", tk.END)
        except Exception as e:
            messagebox.showerror("Ошибка", f"Удаление: {e}")

def add_dish():
    idxs = orders_listbox.curselection()
    if not idxs:
        messagebox.showwarning("Внимание", "Сначала выберите заказ.")
        return
    fname = orders_listbox.get(idxs[0])
    path = ORDERS_DIR / fname
    try:
        with path.open(encoding="utf-8") as f:
            order = json.load(f)
    except Exception as e:
        messagebox.showerror("Ошибка", f"Открытие: {e}")
        return

    name = simpledialog.askstring("Добавить блюдо", "Название блюда:", initialvalue="Additional dish")
    if not name:
        return
    try:
        qty = float(simpledialog.askstring("Добавить блюдо", "Количество:", initialvalue="1"))
        price = float(simpledialog.askstring("Добавить блюдо", "Цена (฿):", initialvalue="0"))
    except Exception:
        messagebox.showerror("Ошибка", "Неверное количество или цена.")
        return

    order.setdefault("items", []).append({"name": name, "qty": qty, "price": price})
    order["total"] = round(float(order.get("total", 0) or 0) + qty * price, 2)
    save_order_to_file(fname, order)
    editor_text.delete("1.0", tk.END)
    editor_text.insert(tk.END, json.dumps(order, ensure_ascii=False, indent=2))
    refresh_order_list()

def apply_discount(percent: int):
    idxs = orders_listbox.curselection()
    if not idxs:
        messagebox.showwarning("Внимание", "Сначала выберите заказ.")
        return
    fname = orders_listbox.get(idxs[0])
    path = ORDERS_DIR / fname
    try:
        with path.open(encoding="utf-8") as f:
            order = json.load(f)
    except Exception as e:
        messagebox.showerror("Ошибка", f"Открытие: {e}")
        return
    total = float(order.get("total", 0) or 0)
    delivery = float(order.get("delivery", 0) or 0)
    discount_amount = round((total - delivery) * (percent / 100), 2)
    order["discount_percent"] = percent
    order["discount_amount"]  = discount_amount
    order["total"] = round(total - discount_amount, 2)
    save_order_to_file(fname, order)
    editor_text.delete("1.0", tk.END)
    editor_text.insert(tk.END, json.dumps(order, ensure_ascii=False, indent=2))
    refresh_order_list()
    # печать со скидкой
    receipt = format_receipt(order, fname)
    print_to_windows_printer(receipt)


# ===================== ОТЧЁТЫ =====================
def normalize_order_for_reports(src: dict, filename: str) -> dict:
    """
    Приводит к одному виду все старые и новые JSON-заказы.
    Если внутри JSON нет нормальной даты — берём дату из имени файла.
    """
    order = canonicalize(src)

    dt = _parse_dt_safe(order.get("date", "")) or parse_dt_from_filename(filename)
    if dt:
        order["date"] = dt.strftime("%Y-%m-%d %H:%M:%S")

    return order

def load_all_orders_for_reports():
    orders = []
    for p in iter_order_json_files():
        try:
            with p.open(encoding="utf-8-sig") as f:
                raw_order = json.load(f)

            order = normalize_order_for_reports(raw_order, p.name)
            dt = _parse_dt_safe(order.get("date", "")) or parse_dt_from_filename(p.name)
            if not dt:
                print(f"Пропуск заказа без даты: {p.name}")
                continue

            orders.append((dt, order, p.name))
        except Exception as e:
            print(f"Ошибка чтения заказа для отчёта {p.name}: {e}")

    orders.sort(key=lambda x: x[0])
    return orders

def order_items_subtotal(order: dict) -> float:
    subtotal = 0.0
    for item in order.get("items", []):
        try:
            qty = float(item.get("qty", 0) or 0)
            price = float(item.get("price", 0) or 0)
            subtotal += qty * price
        except Exception:
            pass

    try:
        subtotal -= float(order.get("discount_amount", 0) or 0)
    except Exception:
        pass

    return round(max(subtotal, 0), 2)

def order_delivery_for_report(order: dict) -> float:
    try:
        return round(float(order.get("delivery", 0) or 0), 2)
    except Exception:
        return 0.0

def order_full_total_for_report(order: dict) -> float:
    """
    Для отчётов берём полную сумму чека: блюда + доставка - скидка.
    Если в JSON уже есть корректный total, используем его.
    """
    food_sum = order_items_subtotal(order)
    delivery = order_delivery_for_report(order)
    calculated = round(food_sum + delivery, 2)

    try:
        total = round(float(order.get("total", calculated) or calculated), 2)
    except Exception:
        total = calculated

    # Если total в старом JSON был только по еде и не включал доставку — добавляем доставку.
    if delivery > 0 and abs(total - food_sum) < 0.01:
        total = round(total + delivery, 2)

    # Если total явно битый/нулевой, считаем сами.
    if total <= 0 and calculated > 0:
        total = calculated

    return round(total, 2)

def normalize_payment_for_report(order: dict) -> str:
    """
    Правила для отчетов Daily/Montly:
    - Банк РФ всегда считается как Cash
    - True money всегда считается как PromptPay
    - Thai bank всегда считается как PromptPay
    """
    raw_payment = str(order.get("payment", "") or order.get("payMethod", "") or "").strip()
    raw_key = raw_payment.lower().replace(" ", "").replace("_", "").replace("-", "")

    if raw_key in ("банкрф", "банкrf", "bankrf", "russianbank", "cash", "наличные"):
        return "Cash"

    if raw_key in (
        "truemoney", "truewallet", "true", "truemoneywallet",
        "thaibank", "thai", "promptpay", "promtpay", "prompt", "promt"
    ):
        return "PromptPay"

    mapped_payment = PAYMENT_MAP.get(raw_payment, raw_payment).strip()
    mapped_key = mapped_payment.lower().replace(" ", "").replace("_", "").replace("-", "")

    if mapped_key in ("cash", "банкрф", "банкrf", "bankrf", "russianbank", "наличные"):
        return "Cash"

    if mapped_key in ("promptpay", "promtpay", "thaibank", "truemoney", "truewallet", "prompt", "promt"):
        return "PromptPay"

    return "Unknown"

def make_report(title: str, period_line: str, filtered_orders: list) -> str:
    sold = {}
    total_food_sum = 0.0
    total_delivery_sum = 0.0
    total_receipts_sum = 0.0
    cash_total = 0.0
    promptpay_total = 0.0
    unknown_total = 0.0

    for _, order, _ in filtered_orders:
        food_sum = order_items_subtotal(order)
        delivery_sum = order_delivery_for_report(order)
        full_sum = order_full_total_for_report(order)

        total_food_sum += food_sum
        total_delivery_sum += delivery_sum
        total_receipts_sum += full_sum

        payment_type = normalize_payment_for_report(order)
        if payment_type == "Cash":
            cash_total += full_sum
        elif payment_type == "PromptPay":
            promptpay_total += full_sum
        else:
            unknown_total += full_sum

        for item in order.get("items", []):
            name_ru = str(item.get("name", ""))
            name = TRANSLATIONS.get(name_ru, name_ru)
            try:
                qty = float(item.get("qty", 0) or 0)
            except Exception:
                qty = 0
            if qty > 0:
                sold[name] = sold.get(name, 0) + qty

    lines = [
        "Smoke Factory BBQ co., LTD",
        title,
        period_line,
        f"Orders: {len(filtered_orders)}",
        "-" * 32,
        "Sold items:"
    ]

    if sold:
        for name in sorted(sold.keys()):
            qty = sold[name]
            qty_disp = int(qty) if float(qty).is_integer() else round(qty, 2)
            lines.append(f"{name} x{qty_disp}")
    else:
        lines.append("No sales")

    lines += [
        "-" * 32,
        f"Food sales: {total_food_sum:.2f} ฿",
        f"Delivery: {total_delivery_sum:.2f} ฿",
        f"Total receipts: {total_receipts_sum:.2f} ฿",
        f"Cash: {cash_total:.2f} ฿",
        f"PromptPay: {promptpay_total:.2f} ฿"
    ]

    if unknown_total > 0:
        lines.append(f"Unknown payment: {unknown_total:.2f} ฿")

    return "\n".join(lines)

def select_date_calendar(parent):
    selected = {"date": None}
    today = date.today()
    current = {"year": today.year, "month": today.month}

    win = tk.Toplevel(parent)
    win.title("Daily report — выберите дату")
    win.transient(parent)
    win.grab_set()

    header = tk.Frame(win)
    header.pack(padx=10, pady=5, fill="x")

    title_var = tk.StringVar()

    days_frame = tk.Frame(win)
    days_frame.pack(padx=10, pady=5)

    def rebuild_calendar():
        for w in days_frame.winfo_children():
            w.destroy()

        y = current["year"]
        m = current["month"]
        title_var.set(f"{calendar.month_name[m]} {y}")

        week_days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        for col, wd in enumerate(week_days):
            tk.Label(days_frame, text=wd, width=5).grid(row=0, column=col, padx=1, pady=1)

        month_cal = calendar.monthcalendar(y, m)
        for r, week in enumerate(month_cal, start=1):
            for c, day_num in enumerate(week):
                if day_num == 0:
                    tk.Label(days_frame, text="", width=5).grid(row=r, column=c, padx=1, pady=1)
                else:
                    def choose(d=day_num):
                        selected["date"] = date(y, m, d)
                    tk.Radiobutton(
                        days_frame,
                        text=str(day_num),
                        width=5,
                        indicatoron=False,
                        variable=date_var,
                        value=f"{y}-{m:02d}-{day_num:02d}",
                        command=choose
                    ).grid(row=r, column=c, padx=1, pady=1)

    def prev_month():
        m = current["month"] - 1
        y = current["year"]
        if m == 0:
            m = 12
            y -= 1
        current["year"], current["month"] = y, m
        rebuild_calendar()

    def next_month():
        m = current["month"] + 1
        y = current["year"]
        if m == 13:
            m = 1
            y += 1
        current["year"], current["month"] = y, m
        rebuild_calendar()

    date_var = tk.StringVar(value=today.strftime("%Y-%m-%d"))
    selected["date"] = today

    tk.Button(header, text="<", command=prev_month, width=4).pack(side="left")
    tk.Label(header, textvariable=title_var, width=20).pack(side="left", expand=True)
    tk.Button(header, text=">", command=next_month, width=4).pack(side="right")

    rebuild_calendar()

    buttons = tk.Frame(win)
    buttons.pack(padx=10, pady=10, fill="x")

    def ok():
        win.destroy()

    def cancel():
        selected["date"] = None
        win.destroy()

    tk.Button(buttons, text="OK", command=ok).pack(side="left", expand=True, fill="x", padx=3)
    tk.Button(buttons, text="Cancel", command=cancel).pack(side="left", expand=True, fill="x", padx=3)

    parent.wait_window(win)
    return selected["date"]

def daily_report():
    chosen = select_date_calendar(root)
    if not chosen:
        return

    all_orders = load_all_orders_for_reports()
    filtered = [(dt, order, fname) for dt, order, fname in all_orders if dt.date() == chosen]

    report = make_report(
        "DAILY REPORT",
        f"Date: {chosen.strftime('%d.%m.%Y')}",
        filtered
    )

    try:
        print_report_to_windows_printer(report)
    except Exception as e:
        messagebox.showerror("Ошибка", f"Печать отчёта: {e}")

def select_month_dialog(parent):
    all_orders = load_all_orders_for_reports()
    months = sorted({(dt.year, dt.month) for dt, _, _ in all_orders}, reverse=True)

    if not months:
        today = date.today()
        months = [(today.year, today.month)]

    selected = {"month": months[0]}

    win = tk.Toplevel(parent)
    win.title("Montly report — выберите месяц")
    win.transient(parent)
    win.grab_set()

    tk.Label(win, text="Выберите месяц:").pack(padx=10, pady=(10, 5))

    listbox = tk.Listbox(win, width=30, height=12)
    listbox.pack(padx=10, pady=5)

    for y, m in months:
        listbox.insert(tk.END, f"{calendar.month_name[m]} {y}")

    listbox.selection_set(0)

    def ok():
        idxs = listbox.curselection()
        if idxs:
            selected["month"] = months[idxs[0]]
        win.destroy()

    def cancel():
        selected["month"] = None
        win.destroy()

    buttons = tk.Frame(win)
    buttons.pack(padx=10, pady=10, fill="x")
    tk.Button(buttons, text="OK", command=ok).pack(side="left", expand=True, fill="x", padx=3)
    tk.Button(buttons, text="Cancel", command=cancel).pack(side="left", expand=True, fill="x", padx=3)

    parent.wait_window(win)
    return selected["month"]

def monthly_report():
    chosen = select_month_dialog(root)
    if not chosen:
        return

    year, month = chosen
    last_day = calendar.monthrange(year, month)[1]
    start_date = date(year, month, 1)
    end_date = date(year, month, last_day)

    all_orders = load_all_orders_for_reports()
    filtered = [
        (dt, order, fname)
        for dt, order, fname in all_orders
        if dt.year == year and dt.month == month
    ]

    report = make_report(
        "MONTHLY REPORT",
        f"Period: {start_date.strftime('%d.%m.%Y')} - {end_date.strftime('%d.%m.%Y')}",
        filtered
    )

    try:
        print_report_to_windows_printer(report)
    except Exception as e:
        messagebox.showerror("Ошибка", f"Печать отчёта: {e}")


# ===================== ОКНО РУЧНОЙ ПЕЧАТИ СТИКЕРОВ =====================
def iter_grab_json_files():
    """Возвращает JSON-файлы из папки GRAB, новые сверху."""
    files = []
    try:
        for path in GRAB_DIR.iterdir():
            if path.is_file() and path.suffix.lower() == ".json":
                files.append(path)
    except Exception as exc:
        _grab_log(f"Ошибка чтения папки GRAB: {exc}")
        return []

    def sort_key(path: Path):
        try:
            return path.stat().st_mtime
        except Exception:
            return 0

    files.sort(key=sort_key, reverse=True)
    return files


def read_grab_json_order_number(json_path: Path) -> str:
    """Читает номер заказа из выбранного JSON и проверяет формат."""
    with Path(json_path).open("r", encoding="utf-8-sig") as handle:
        data = json.load(handle)

    order_number = str(
        data.get("order_number")
        or data.get("orderNumber")
        or data.get("order_no")
        or data.get("orderNo")
        or data.get("number")
        or ""
    ).strip().upper()

    if not validate_grab_order_number(order_number):
        raise ValueError(
            "В выбранном JSON нет номера формата GF-342 или SM-472."
        )

    return order_number


def queue_existing_grab_json(json_path: Path) -> str:
    """
    Ставит на печать уже существующий JSON из папки GRAB.
    Новый JSON не создаётся, чековая часть программы не затрагивается.
    """
    json_path = Path(json_path)
    order_number = read_grab_json_order_number(json_path)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    image_path = GRAB_DIR / f"{json_path.stem}_manual_{stamp}.png"

    GRAB_PRINT_QUEUE.put(
        {
            "order_number": order_number,
            "json_path": json_path,
            "image_path": image_path,
        }
    )
    return order_number


def open_sticker_window():
    """Открывает список JSON из GRAB и позволяет повторно печатать выбранный стикер."""
    win = tk.Toplevel(root)
    win.title("Sticker")
    win.transient(root)
    win.geometry("780x500")
    win.minsize(680, 420)

    main = tk.Frame(win)
    main.pack(fill="both", expand=True, padx=10, pady=10)

    left = tk.Frame(main)
    left.pack(side="left", fill="y")

    tk.Label(left, text="GRAB JSON").pack(anchor="w")

    list_frame = tk.Frame(left)
    list_frame.pack(fill="both", expand=True, pady=(5, 0))

    sticker_listbox = tk.Listbox(
        list_frame,
        width=42,
        height=22,
        exportselection=False,
    )
    sticker_scrollbar = tk.Scrollbar(
        list_frame,
        orient="vertical",
        command=sticker_listbox.yview,
    )
    sticker_listbox.configure(yscrollcommand=sticker_scrollbar.set)
    sticker_listbox.pack(side="left", fill="both", expand=True)
    sticker_scrollbar.pack(side="right", fill="y")

    right = tk.Frame(main)
    right.pack(side="left", fill="both", expand=True, padx=(10, 0))

    tk.Label(right, text="Содержимое выбранного JSON").pack(anchor="w")

    json_text = tk.Text(right, wrap="word", state="disabled")
    json_text.pack(fill="both", expand=True, pady=(5, 0))

    status_var_sticker = tk.StringVar(value="")
    tk.Label(
        win,
        textvariable=status_var_sticker,
        anchor="w",
        fg="gray",
    ).pack(fill="x", padx=10)

    buttons = tk.Frame(win)
    buttons.pack(fill="x", padx=10, pady=(5, 10))

    grab_files = []

    def show_text(value: str):
        json_text.configure(state="normal")
        json_text.delete("1.0", tk.END)
        json_text.insert(tk.END, value)
        json_text.configure(state="disabled")

    def selected_path():
        selected = sticker_listbox.curselection()
        if not selected:
            return None
        index = selected[0]
        if index < 0 or index >= len(grab_files):
            return None
        return grab_files[index]

    def show_selected(_event=None):
        path = selected_path()
        if path is None:
            show_text("")
            return

        try:
            with path.open("r", encoding="utf-8-sig") as handle:
                data = json.load(handle)
            show_text(json.dumps(data, ensure_ascii=False, indent=2))
        except Exception:
            try:
                show_text(path.read_text(encoding="utf-8-sig", errors="replace"))
            except Exception as exc:
                show_text(f"Ошибка открытия файла:\n{exc}")

    def refresh_grab_list(select_first=True):
        nonlocal grab_files
        grab_files = iter_grab_json_files()
        sticker_listbox.delete(0, tk.END)

        for path in grab_files:
            try:
                number = read_grab_json_order_number(path)
                display_name = f"{number}   |   {path.name}"
            except Exception:
                display_name = f"ОШИБКА JSON   |   {path.name}"
            sticker_listbox.insert(tk.END, display_name)

        status_var_sticker.set(
            f"Папка: {GRAB_DIR} | JSON: {len(grab_files)}"
        )

        if grab_files and select_first:
            sticker_listbox.selection_set(0)
            sticker_listbox.activate(0)
            sticker_listbox.see(0)
            show_selected()
        elif not grab_files:
            show_text("В папке GRAB пока нет JSON-файлов.")

    def print_selected_sticker(_event=None):
        path = selected_path()
        if path is None:
            messagebox.showwarning(
                "Sticker",
                "Выберите JSON-файл из списка.",
                parent=win,
            )
            return

        try:
            order_number = queue_existing_grab_json(path)
            status_var_sticker.set(
                f"Стикер {order_number} поставлен в очередь печати."
            )
            messagebox.showinfo(
                "Sticker",
                f"Стикер {order_number} отправлен на печать.",
                parent=win,
            )
        except Exception as exc:
            _grab_log(
                "Ошибка ручной печати Grab-этикетки: "
                f"{exc}\n{traceback.format_exc()}"
            )
            messagebox.showerror(
                "Sticker",
                f"Не удалось напечатать выбранный стикер:\n{exc}",
                parent=win,
            )

    sticker_listbox.bind("<<ListboxSelect>>", show_selected)
    sticker_listbox.bind("<Double-Button-1>", print_selected_sticker)

    tk.Button(
        buttons,
        text="Обновить",
        command=refresh_grab_list,
    ).pack(side="left", padx=(0, 5))

    tk.Button(
        buttons,
        text="Печать",
        command=print_selected_sticker,
        font=("Arial", 10, "bold"),
    ).pack(side="left", fill="x", expand=True, padx=5)

    tk.Button(
        buttons,
        text="Закрыть",
        command=win.destroy,
    ).pack(side="right", padx=(5, 0))

    refresh_grab_list()

# Кнопки
tk.Button(frame, text="🔄 Обновить", command=refresh_order_list).grid(row=1, column=1, sticky="ew", pady=5)
tk.Button(frame, text="💾 Сохранить", command=save_edited_order).grid(row=1, column=2, sticky="ew", pady=5)
tk.Button(frame, text="🖨️ Печать", command=print_selected_order).grid(row=1, column=3, sticky="ew", pady=5)
tk.Button(frame, text="❌ Аннулировать", command=delete_selected_order).grid(row=1, column=4, sticky="ew", pady=5)

tk.Button(frame, text="➕ Добавить блюдо", command=add_dish).grid(row=2, column=1, columnspan=1, sticky="ew", pady=5)
tk.Button(frame, text="5% скидка", command=lambda: apply_discount(5)).grid(row=2, column=2, sticky="ew", pady=5)
tk.Button(frame, text="10% скидка", command=lambda: apply_discount(10)).grid(row=2, column=3, sticky="ew", pady=5)
tk.Button(frame, text="40% скидка", command=lambda: apply_discount(40)).grid(row=2, column=4, sticky="ew", pady=5)

# Кнопки отчётов
tk.Button(frame, text="Daily report", command=daily_report).grid(row=3, column=1, columnspan=2, sticky="ew", pady=5)
tk.Button(frame, text="Montly report", command=monthly_report).grid(row=3, column=3, columnspan=2, sticky="ew", pady=5)

# Ручная повторная печать этикеток из папки GRAB
tk.Button(frame, text="Sticker", command=open_sticker_window).grid(row=4, column=1, columnspan=4, sticky="ew", pady=5)

refresh_order_list()

# Запуск очереди Grab-печати в фоне.
# Автопечать и ручная печать из окна Sticker используют одну последовательную очередь.
grab_worker_thread = threading.Thread(
    target=grab_print_worker,
    name="grab-label-printer",
    daemon=True,
)
grab_worker_thread.start()

# Запуск HTTP-приёмника в фоне
srv_thread = threading.Thread(target=run_receiver, name="order-receiver", daemon=True)
srv_thread.start()

# GUI mainloop
root.mainloop()
