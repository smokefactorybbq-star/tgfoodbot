# tgfoodbot — единый обработчик заказов Smoke Factory

Это обновление существующего customer bot. Оно сохраняет текущую работу Mini App и добавляет обработку сайта Smoke Factory и уведомления MealPoint.

## Новая автоматика
- `POST /website-order` принимает заказ с `smokefactorybbq.com`;
- считает время кухни: максимальное время блюда (сейчас 5 мин для каждого) + 10 мин;
- для доставки запрашивает Google Routes `TWO_WHEELER` от 7.910335,98.368771 и прибавляет 10 мин;
- отправляет заказ в screen service;
- отправляет заказ в чековую программу;
- пишет клиенту ETA;
- через 5 минут один раз напоминает прислать чек, если PromptPay-чек не пришёл;
- фото/PDF клиента привязываются к последнему ожидающему PromptPay-заказу и пересылаются менеджеру;
- сообщение менеджера `SM-877 https://...` отправляет tracking link нужному клиенту;
- AI отвечает клиенту, а при неуверенности спрашивает менеджера и сохраняет ответ в базу знаний;
- `POST /mealpoint/subscription` и `/mealpoint/receipt` принимают уведомления от отдельного MealPoint.

## Переменные
Смотрите `.env.example`. Особенно должны совпасть три секрета между сервисами:
- `WEBSITE_ORDER_SECRET` — Smoke site <-> tgfoodbot;
- `MEALPOINT_BOT_SECRET` — MealPoint <-> tgfoodbot;
- `SCREEN_SERVICE_SECRET` — tgfoodbot <-> screen service.
