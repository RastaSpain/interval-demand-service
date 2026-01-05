# Изменения в коде - исправление расчёта коробок

## Проблема
Старый код не работал, потому что:
1. Модуль `cartonization.py` не был интегрирован в `main.py`
2. Логика маппинга была неправильной (пыталась маппить через текстовый Product ID)
3. Не загружались данные о коробках из Airtable

## Исправления

### 1. cartonization.py
**Удалены:**
- `listing_to_product_id()` - неправильная логика
- `build_products_map()` - не нужна
- `build_box_map()` - неправильный подход

**Добавлены:**
- `build_box_map_from_productmarket()` - правильная логика:
  - Берёт ProductMarket records
  - Через поле "Product and Box sizes cm" находит коробки
  - Возвращает маппинг: `listing_id (Record ID) -> units_per_carton`

**Изменены:**
- `cartonize_rows()` - теперь принимает готовый маппинг `listing_id -> units_per_carton`

### 2. main.py
**Добавлено:**
- Импорт функций из `cartonization`
- Загрузка данных ProductMarket и Box из Airtable
- Вызов `build_box_map_from_productmarket()`
- Вызов `cartonize_rows()`
- Расширенные totals с данными по коробкам

**Изменено:**
- Обработка поля "Listing ID" как списка Record IDs
- Возвращаемая структура данных включает поля cartons, overstock и т.д.

### 3. Новые переменные окружения
```
AIRTABLE_TABLE_PRODUCTMARKET=tblenrjgpDcP6240C
AIRTABLE_TABLE_BOX=tblLoWfbXpNlJoTjz
```

## Структура данных

### Маппинг связей:
```
Listing ID (Record ID из Sales Plan Daily)
    ↓ хранится как связь в поле "Listing ID"
ProductMarket Record (rec01nlDFcKrEgoEv)
    ↓ поле "Product and Box sizes cm" → ["recbJHgnJHu4FfGiA"]
Box Record (recbJHgnJHu4FfGiA)
    ↓ поле "Кол-во в коробке" → 18
```

### Пример ответа API:
```json
{
  "listing_id": "rec01nlDFcKrEgoEv",
  "forecast_units": 45.0,
  "start_stock": 0.0,
  "order_qty": 45.0,
  "units_per_carton": 18,
  "cartons": 3,
  "rounded_units": 54,
  "overstock_units": 9.0,
  "overstock_pct": 0.2,
  "status": "OK"
}
```

## Как это работает

1. **Загрузка прогноза:**
   - Читаем Sales Plan Daily за период
   - Поле "Listing ID" содержит список Record IDs из ProductMarket
   - Берём первый ID из списка
   - Суммируем forecast по каждому listing_id

2. **Загрузка данных о коробках:**
   - Читаем все записи ProductMarket
   - Читаем все записи Box
   - Строим маппинг: listing_id → units_per_carton

3. **Расчёт коробок:**
   - Для каждого listing_id находим units_per_carton
   - Округляем order_qty вверх: `cartons = ceil(order_qty / units_per_carton)`
   - Считаем overstock: `(cartons * units_per_carton) - order_qty`

## Тестирование

1. Запустить локально:
```bash
uvicorn main:app --reload --port 8000
```

2. Запустить тесты:
```bash
python test_api.py
```

3. Проверить конкретный период:
```bash
curl -X POST http://localhost:8000/calc/interval-demand \
  -H "Content-Type: application/json" \
  -d '{
    "market": "USA",
    "interval_start": "2026-02-01",
    "interval_end": "2026-02-28",
    "start_stock_mode": "ZERO"
  }'
```

## Обработка ошибок

### BOX_NOT_FOUND
Возникает когда:
- У ProductMarket не заполнено поле "Product and Box sizes cm"
- Связанная коробка не найдена в таблице Box
- У коробки отсутствует поле "Кол-во в коробке"

### Конфликты
Если ProductMarket связан с несколькими коробками с разным количеством:
- Берётся первая найденная коробка
- В консоль выводится warning

## Деплой

Railway автоматически:
1. Установит зависимости из `requirements.txt`
2. Запустит команду из `Procfile`: `web: uvicorn main:app --host 0.0.0.0 --port $PORT`

Не забудьте установить переменные окружения:
- `AIRTABLE_API_KEY`
- `AIRTABLE_BASE_ID` (опционально, по умолчанию appHbiHFRAWtx2ErO)
