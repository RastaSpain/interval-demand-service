import os
from datetime import datetime, timedelta
from typing import Dict, Any, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from pyairtable import Api

from cartonization import build_box_map_from_productmarket, cartonize_rows

app = FastAPI(title="Interval Demand Calculator", version="1.0.0")

# ======================
# ENV CONFIG
# ======================
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_DAILY = os.getenv("AIRTABLE_TABLE_DAILY", "tblRLB6E83lHg6h7b")
AIRTABLE_TABLE_PRODUCTMARKET = os.getenv("AIRTABLE_TABLE_PRODUCTMARKET", "tblenrjgpDcP6240C")
AIRTABLE_TABLE_BOX = os.getenv("AIRTABLE_TABLE_BOX", "tblLoWfbXpNlJoTjz")

F_DATE = os.getenv("AIRTABLE_FIELD_DATE", "Date")
F_LISTING_ID = os.getenv("AIRTABLE_FIELD_LISTING_ID", "Listing ID")
F_MARKET = os.getenv("AIRTABLE_FIELD_MARKET", "Marketplace")
F_FORECAST = os.getenv("AIRTABLE_FIELD_FORECAST", "Planned units")


class CalcRequest(BaseModel):
    market: str = Field(..., example="USA")
    interval_start: str = Field(..., example="2026-04-01")
    interval_end: str = Field(..., example="2026-05-15")  # inclusive
    start_stock_mode: str = Field("ZERO", example="ZERO")
    start_stock: Dict[str, float] = Field(default_factory=dict)
    safety_days: float = 0


def parse_date(s: str) -> datetime:
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid date format: {s}. Use YYYY-MM-DD."
        )


def build_filter(market: str, start: str, end_exclusive: str) -> str:
    """
    Работает для:
    - Date поля
    - ISO-строк YYYY-MM-DD
    """
    return (
        f"AND("
        f"{{{F_MARKET}}} = '{market}',"
        f"{{{F_DATE}}} >= '{start}',"
        f"{{{F_DATE}}} < '{end_exclusive}'"
        f")"
    )


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/debug/box-data")
def debug_box_data():
    """
    Debug endpoint - показывает какие данные о коробках загружаются
    """
    if not all([AIRTABLE_API_KEY, AIRTABLE_BASE_ID]):
        raise HTTPException(
            status_code=500,
            detail="Airtable environment variables are not fully configured."
        )
    
    api = Api(AIRTABLE_API_KEY)
    
    # Тестовый listing_id
    test_listing_id = "rec01nlDFcKrEgoEv"
    
    try:
        # Загружаем ProductMarket
        table_pm = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_PRODUCTMARKET)
        pm_rec = table_pm.get(test_listing_id)
        
        # Загружаем все Box records
        table_box = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_BOX)
        box_records = table_box.all(fields=["Кол-во в коробке"])
        
        # Строим маппинг
        from cartonization import build_box_map_from_productmarket
        box_map = build_box_map_from_productmarket([pm_rec], box_records)
        
        return {
            "test_listing_id": test_listing_id,
            "productmarket_record": pm_rec,
            "box_records_count": len(box_records),
            "box_map": box_map,
            "result": box_map.get(test_listing_id)
        }
    except Exception as e:
        return {
            "error": str(e),
            "test_listing_id": test_listing_id
        }


@app.post("/calc/interval-demand")
def calc_interval_demand(req: CalcRequest) -> Dict[str, Any]:
    """
    Рассчитывает потребность в заказах по интервалу с учётом коробок.
    
    Возвращает:
    - rows: список товаров с расчётами (включая данные по коробкам)
    - totals: агрегированная статистика
    """

    if not all([AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_DAILY]):
        raise HTTPException(
            status_code=500,
            detail="Airtable environment variables are not fully configured."
        )

    start_dt = parse_date(req.interval_start)
    end_dt = parse_date(req.interval_end)

    if end_dt < start_dt:
        raise HTTPException(
            status_code=400,
            detail="interval_end must be >= interval_start"
        )

    # inclusive → exclusive
    start_str = start_dt.strftime("%Y-%m-%d")
    end_exclusive = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d")

    api = Api(AIRTABLE_API_KEY)
    
    # 1. Получаем данные из Sales Plan Daily
    table_daily = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_DAILY)
    formula = build_filter(req.market, start_str, end_exclusive)

    records = table_daily.all(
        formula=formula,
        fields=[F_DATE, F_LISTING_ID, F_MARKET, F_FORECAST]
    )

    # 2. Агрегируем по listing_id
    aggregated: Dict[str, float] = {}

    for r in records:
        f = r.get("fields", {})
        
        # Listing ID это список record IDs
        listing_ids = f.get(F_LISTING_ID)
        if not listing_ids:
            continue
            
        if not isinstance(listing_ids, list):
            listing_ids = [listing_ids]
        
        # Берём первый listing_id из связей
        listing_id = listing_ids[0] if listing_ids else None
        if not listing_id:
            continue

        try:
            units = float(f.get(F_FORECAST, 0))
        except (TypeError, ValueError):
            units = 0.0

        aggregated[listing_id] = aggregated.get(listing_id, 0.0) + units

    # 3. Получаем данные о коробках
    # Загружаем только нужные ProductMarket records (по listing_ids из aggregated)
    table_pm = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_PRODUCTMARKET)
    
    # Загружаем каждый ProductMarket record по ID
    pm_records = []
    for listing_id in aggregated.keys():
        try:
            pm_rec = table_pm.get(listing_id)
            pm_records.append(pm_rec)
        except Exception as e:
            print(f"Warning: Could not load ProductMarket {listing_id}: {e}")
            continue
    
    # Загружаем Box records
    table_box = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_BOX)
    box_records = table_box.all(fields=["Кол-во в коробке"])
    
    # Строим маппинг listing_id -> units_per_carton
    listing_to_box = build_box_map_from_productmarket(
        pm_records, 
        box_records
    )

    # 4. Формируем строки с расчётами
    rows: List[Dict[str, Any]] = []
    total_forecast = 0.0

    for listing_id, forecast_units in aggregated.items():
        start_stock = 0.0
        if req.start_stock_mode.upper() == "MANUAL":
            start_stock = float(req.start_stock.get(listing_id, 0) or 0)

        order_qty = max(0.0, forecast_units - start_stock)

        rows.append({
            "listing_id": listing_id,
            "forecast_units": round(forecast_units, 2),
            "start_stock": round(start_stock, 2),
            "safety_units": 0.0,
            "order_qty": round(order_qty, 2)
        })

        total_forecast += forecast_units

    # 5. Применяем cartonization
    rows_with_boxes = cartonize_rows(rows, listing_to_box)
    
    # 6. Сортируем по order_qty
    rows_with_boxes.sort(key=lambda x: x["order_qty"], reverse=True)
    
    # 7. Подсчитываем totals
    total_cartons = sum(r.get("cartons", 0) or 0 for r in rows_with_boxes if r.get("status") == "OK")
    total_rounded = sum(r.get("rounded_units", 0) or 0 for r in rows_with_boxes if r.get("status") == "OK")
    total_overstock = sum(r.get("overstock_units", 0) or 0 for r in rows_with_boxes if r.get("status") == "OK")
    
    errors_count = sum(1 for r in rows_with_boxes if r.get("status") == "ERROR")

    return {
        "market": req.market,
        "interval_start": req.interval_start,
        "interval_end": req.interval_end,
        "rows": rows_with_boxes,
        "totals": {
            "listings": len(rows_with_boxes),
            "forecast_units": round(total_forecast, 2),
            "order_qty": round(sum(r["order_qty"] for r in rows_with_boxes), 2),
            "cartons": total_cartons,
            "rounded_units": total_rounded,
            "overstock_units": round(total_overstock, 2),
            "errors": errors_count
        }
    }
