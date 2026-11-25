"""Add the separate MealPoint receipt endpoint to the existing SmokeFactory aiohttp server."""
import hmac
import logging
import os
import re

import aiohttp
from aiohttp import web

log = logging.getLogger(__name__)


async def print_mealpoint_receipt(request: web.Request):
    expected = os.getenv("MEALPOINT_PRINT_SECRET", "").strip().encode()
    actual = request.headers.get("X-MealPoint-Print-Secret", "").encode()
    if len(expected) < 24 or not hmac.compare_digest(expected, actual):
        return web.json_response({"ok": False}, status=401)
    url = os.getenv("MEALPOINT_PRINTER_URL", "").strip()
    secret = os.getenv("MEALPOINT_PRINTER_SECRET", "").strip()
    if not url or len(secret) < 24:
        return web.json_response({"ok": False, "error": "PRINTER_CONFIG"}, status=503)
    try:
        data = await request.json()
        if not isinstance(data, dict) or not re.fullmatch(r"MP-[1-9][0-9]{0,17}", str(data.get("receipt_number", ""))):
            return web.json_response({"ok": False, "error": "INVALID_RECEIPT"}, status=400)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=25)) as session:
            async with session.post(url, json=data, headers={"X-MealPoint-Printer-Secret": secret}, allow_redirects=False) as response:
                result = await response.json()
                if response.status == 200 and result.get("ok") and result.get("printed") is True:
                    return web.json_response({"ok": True,"printed": True})
    except Exception as exc:
        log.warning("MealPoint print relay failed: %s", type(exc).__name__)
    return web.json_response({"ok": False,"error": "PRINT_NOT_CONFIRMED"}, status=502)


def register_mealpoint_receipts(app: web.Application):
    app.router.add_post("/mealpoint/receipt/print", print_mealpoint_receipt)
