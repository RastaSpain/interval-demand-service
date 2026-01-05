# Amazon Purchase Order Calculator

Система расчёта заказов с учётом коробок для Amazon логистики.

## Архитектура

```
Sales Plan Daily (forecast по дням)
    ↓
ProductMarket (listing_id)
    ↓ "Product and Box sizes cm"
Shiping Box sizes cm
    ↓ "Кол-во в коробке"
Расчёт коробок
```

## Основные возможности

1. **Расчёт спроса по интервалу** - суммирует прогноз продаж за период
2. **Учёт начальных остатков** - вычитает текущие остатки из потребности
3. **Расчёт коробок** - округляет заказ до целых коробок
4. **Расчёт переизбытка** - показывает overstock в штуках и процентах

## Установка локально

```bash
# 1. Установить зависимости
pip install -r requirements.txt

# 2. Создать .env файл
cp .env.example .env

# 3. Заполнить AIRTABLE_API_KEY в .env

# 4. Запустить сервер
uvicorn main:app --reload --port 8000
```

## API Endpoints

### GET /health
Проверка работоспособности сервера

### POST /calc/interval-demand

**Request:**
```json
{
  "market": "USA",
  "interval_start": "2026-04-01",
  "interval_end": "2026-05-15",
  "start_stock_mode": "ZERO",
  "start_stock": {},
  "safety_days": 0
}
```

**Response:**
```json
{
  "market": "USA",
  "interval_start": "2026-04-01",
  "interval_end": "2026-05-15",
  "rows": [
    {
      "listing_id": "rec01nlDFcKrEgoEv",
      "forecast_units": 45.0,
      "start_stock": 0.0,
      "safety_units": 0.0,
      "order_qty": 45.0,
      "units_per_carton": 18,
      "cartons": 3,
      "rounded_units": 54,
      "overstock_units": 9.0,
      "overstock_pct": 0.2,
      "status": "OK"
    }
  ],
  "totals": {
    "listings": 10,
    "forecast_units": 150.0,
    "order_qty": 150.0,
    "cartons": 12,
    "rounded_units": 180,
    "overstock_units": 30.0,
    "errors": 0
  }
}
```

## Деплой на Railway

1. **Создать новый проект на Railway**
2. **Подключить GitHub репозиторий** или загрузить файлы
3. **Настроить переменные окружения:**
   - `AIRTABLE_API_KEY` - ваш API ключ
   - `AIRTABLE_BASE_ID` - ID базы (по умолчанию: appHbiHFRAWtx2ErO)

4. **Railway автоматически определит:**
   - `requirements.txt` для установки зависимостей
   - `Procfile` для запуска приложения

## Структура данных

### Sales Plan Daily
- **Date** - дата прогноза
- **Listing ID** - связь с ProductMarket (Record ID)
- **Marketplace** - рынок (USA, UK, etc)
- **Planned units** - прогноз продаж (число)

### ProductMarket
- **Product and Box sizes cm** - связь с таблицей коробок

### Shiping Box sizes cm
- **Кол-во в коробке** - количество единиц товара в коробке

## Логика расчёта

1. **Агрегация спроса**: суммируются planned_units за период по каждому listing_id
2. **Вычет остатков**: `order_qty = forecast_units - start_stock`
3. **Округление до коробок**: `cartons = ceil(order_qty / units_per_carton)`
4. **Расчёт переизбытка**: `overstock = (cartons * units_per_carton) - order_qty`

## Обработка ошибок

- **BOX_NOT_FOUND** - не найдена информация о коробке для товара
- **Конфликты** - если ProductMarket связан с несколькими коробками с разным количеством

## Мониторинг

Проверка работоспособности:
```bash
curl https://your-app.railway.app/health
```

## Troubleshooting

### Ошибка "BOX_NOT_FOUND"
- Проверьте, что у ProductMarket заполнено поле "Product and Box sizes cm"
- Проверьте, что в таблице Box есть поле "Кол-во в коробке"

### Пустой ответ
- Проверьте формат дат (YYYY-MM-DD)
- Проверьте название маркета (точное совпадение)
- Проверьте, есть ли данные в Sales Plan Daily за этот период

### Неправильные данные по коробкам
- Проверьте связи между таблицами в Airtable
- Проверьте логи на Railway на наличие warnings о конфликтах
