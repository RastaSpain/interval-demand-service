import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
import math

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from pyairtable import Api

app = FastAPI(title="Interval Demand Calculator", version="1.2.0")

# ======================
# ENV CONFIG
# ======================
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_DAILY = os.getenv("AIRTABLE_TABLE_DAILY")

F_DATE = os.getenv("AIRTABLE_FIELD_DATE", "Date")
F_LISTING_ID = os.getenv("AIRTABLE_FIELD_LISTING_ID", "Listing ID")
F_MARKET = os.getenv(
    "AIRTABLE_FIELD_MARKET",
    "Marketplace (from Marketplace) (from Listing ID)"
)
F_FORECAST = os.getenv("AIRTABLE_FIELD_FORECAST", "Planned units")

# --- NEW: tables for cartonization ---
AIRTABLE_TABLE_PRODUCTMARKET = os.getenv("AIRTABLE_TABLE_PRODUCTMARKET", "ProductMarket")
AIRTABLE_TABLE_PRODUCTS = os.getenv("AIRTABLE_TABLE_PRODUCTS", "Products")
AIRTABLE_TABLE_BOXES = os.getenv("AIRTABLE_TABLE_BOXES", "Shiping Box sizes cm")

# --- NEW: field names (override via ENV if needed) ---
PM_FIELD_PRODUCT_LINK = os.getenv("AIRTABLE_PM_FIELD_PRODUCT_LINK", "Product")
BOX_FIELD_PRODUCTS_LINK = os.getenv("AIRTABLE_BOX_FIELD_PRODUCTS_LINK", "Products")
BOX_FIELD_UNITS = os.getenv("AIRTABLE_BOX_FIELD_UNITS", "Кол-во в коробке")
PRODUCTS_FIELD_PRODUCT_ID = os.getenv("AIRTABLE_PRODUCTS_FIELD_PRODUCT_ID", "Product ID")

# Optional: lightweight protection for debug endpoints
DEBUG_TOKEN = os.getenv("DEBUG_TOKEN", "")  # set in Railway Variables if you want


class CalcRequest(BaseModel):
    market: str = Field(..., examples=["USA"])
    interval_start: str = Field(..., examples=["2026-04-01"])
    interval_end: str = Field(..., examples=["2026-05-15"])  # inclusive
    start_stock_mode: str = Field("ZERO", examples=["ZERO", "MANUAL"])
    start_stock: Dict[str, float] = Field(default_factory=dict)  # listing_id -> qty


def parse_date(s: str) -> datetime:
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {s}. Use YYYY-MM-DD.")


def build_date_filter(start: str, end_exclusive: str) -> str:
    """
    Only date filter here.
    We DO NOT filter by market in Airtable formula because Market is a list/lookup.
    """
    return (
        f"AND("
        f"{{{F_DATE}}} >= '{start}',"
        f"{{{F_DATE}}} < '{end_exclusive}'"
        f")"
    )


def get_airtable_api() -> Api:
    if not all([AIRTABLE_API_KEY, AIRTABLE_BASE_ID]):
        raise HTTPException(status_code=500, detail="Airtable environment variables are not fully configured.")
    return Api(AIRTABLE_API_KEY)


def get_airtable_table_daily():
    if not AIRTABLE_TABLE_DAILY:
        raise HTTPException(status_code=500, detail="AIRTABLE_TABLE_DAILY is not configured.")
    api = get_airtable_api()
    return api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_DAILY)


# --- NEW: helpers to access other tables ---
def get_airtable_table(name: str):
    api = get_airtable_api()
    return api.table(AIRTABLE_BASE_ID, name)


def as_list(value) -> List[Any]:
    """
    Airtable can return:
    - list for linked/lookup fields
    - scalar for text/number fields
    Normalize to list.
    """
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def first_or_none(value) -> Optional[Any]:
    lst = as_list(value)
    return lst[0] if lst else None


@app.get("/health")
def health():
    return {"ok": True}


# ======================
# CORE CALC (existing)
# ======================
def _calc_interval_demand_internal(req: CalcRequest) -> Dict[str, Any]:
    start_dt = parse_date(req.interval_start)
    end_dt = parse_date(req.interval_end)
    if end_dt < start_dt:
        raise HTTPException(status_code=400, detail="interval_end must be >= interval_start")

    start_str = start_dt.strftime("%Y-%m-%d")
    end_exclusive = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d")

    table = get_airtable_table_daily()
    formula = build_date_filter(start_str, end_exclusive)

    records = table.all(
        formula=formula,
        fields=[F_DATE, F_LISTING_ID, F_MARKET, F_FORECAST],
    )

    aggregated: Dict[str, float] = {}

    for r in records:
        f = r.get("fields", {})

        markets = as_list(f.get(F_MARKET))
        if req.market not in markets:
            continue

        listing_id = first_or_none(f.get(F_LISTING_ID))  # record id of ProductMarket
        if not listing_id:
            continue

        raw_units = f.get(F_FORECAST, 0)
        try:
            units = float(raw_units or 0)
        except (TypeError, ValueError):
            units = 0.0

        aggregated[listing_id] = aggregated.get(listing_id, 0.0) + units

    rows: List[Dict[str, Any]] = []
    total_forecast = 0.0

    for listing_id, forecast_units in aggregated.items():
        start_stock = 0.0
        if req.start_stock_mode.upper() == "MANUAL":
            try:
                start_stock = float(req.start_stock.get(listing_id, 0) or 0)
            except (TypeError, ValueError):
                start_stock = 0.0

        order_qty = max(0.0, forecast_units - start_stock)

        rows.append({
            "listing_id": listing_id,
            "forecast_units": round(forecast_units, 4),
            "start_stock": round(start_stock, 4),
            "safety_units": 0.0,
            "order_qty": round(order_qty, 4)
        })

        total_forecast += forecast_units

    rows.sort(key=lambda x: x["order_qty"], reverse=True)

    return {
        "market": req.market,
        "interval_start": req.interval_start,
        "interval_end": req.interval_end,
        "rows": rows,
        "totals": {
            "listings": len(rows),
            "forecast_units": round(total_forecast, 4),
            "order_qty": round(sum(r["order_qty"] for r in rows), 4)
        }
    }


@app.post("/calc/interval-demand")
def calc_interval_demand(req: CalcRequest) -> Dict[str, Any]:
    return _calc_interval_demand_internal(req)


# ======================
# --- NEW: CARTONIZATION
# ======================

def chunked(lst: List[str], size: int) -> List[List[str]]:
    return [lst[i:i + size] for i in range(0, len(lst), size)]


def fetch_productmarket_listing_to_product(listing_ids: List[str]) -> Dict[str, str]:
    """
    listing_id (ProductMarket record id) -> product_rec_id (Products record id)
    """
    if not listing_ids:
        return {}

    pm_table = get_airtable_table(AIRTABLE_TABLE_PRODUCTMARKET)
    out: Dict[str, str] = {}

    # Airtable formula size is limited — бьём на чанки
    for chunk in chunked(listing_ids, 50):
        or_part = ",".join([f"RECORD_ID()='{rid}'" for rid in chunk])
        formula = f"OR({or_part})"

        recs = pm_table.all(formula=formula, fields=[PM_FIELD_PRODUCT_LINK])
        for r in recs:
            rid = r.get("id")
            fields = r.get("fields", {}) or {}
            prod_rec = first_or_none(fields.get(PM_FIELD_PRODUCT_LINK))
            if rid and prod_rec:
                out[rid] = prod_rec

    return out


def fetch_products_product_id(product_rec_ids: List[str]) -> Dict[str, str]:
    """
    product_rec_id -> "Product ID" (human)
    """
    if not product_rec_ids:
        return {}

    products_table = get_airtable_table(AIRTABLE_TABLE_PRODUCTS)
    out: Dict[str, str] = {}

    for chunk in chunked(product_rec_ids, 50):
        or_part = ",".join([f"RECORD_ID()='{rid}'" for rid in chunk])
        formula = f"OR({or_part})"

        recs = products_table.all(formula=formula, fields=[PRODUCTS_FIELD_PRODUCT_ID])
        for r in recs:
            rid = r.get("id")
            fields = r.get("fields", {}) or {}
            pid = (fields.get(PRODUCTS_FIELD_PRODUCT_ID) or "").strip()
            if rid and pid:
                out[rid] = pid

    return out


def fetch_units_per_box_map() -> Dict[str, int]:
    """
    product_rec_id -> units_per_box
    Table: Shiping Box sizes cm
    Fields:
      - Products (linked, can be multiple)
      - Кол-во в коробке (number)
    """
    boxes_table = get_airtable_table(AIRTABLE_TABLE_BOXES)
    recs = boxes_table.all(fields=[BOX_FIELD_PRODUCTS_LINK, BOX_FIELD_UNITS])

    out: Dict[str, int] = {}
    for r in recs:
        f = r.get("fields", {}) or {}
        prod_ids = as_list(f.get(BOX_FIELD_PRODUCTS_LINK))
        units_raw = f.get(BOX_FIELD_UNITS)

        if units_raw is None:
            continue
        try:
            units = int(units_raw)
        except Exception:
            continue
        if units <= 0:
            continue

        for pid in prod_ids:
            if not pid:
                continue
            # если продукт встречается в нескольких строках — оставим первое (или можно валидировать)
            out.setdefault(pid, units)

    return out


@app.post("/calc/interval-demand-cartons")
def calc_interval_demand_cartons(req: CalcRequest) -> Dict[str, Any]:
    """
    То же, что interval-demand, но добавляет расчёт коробок:
    - округляем всегда вверх
    - считаем overstock_units / overstock_pct
    - если нет коробки/продукта — status=ERROR
    """
    base = _calc_interval_demand_internal(req)
    rows = base.get("rows", [])

    listing_ids = [r["listing_id"] for r in rows if r.get("listing_id")]
    listing_to_product = fetch_productmarket_listing_to_product(listing_ids)
    units_per_box_map = fetch_units_per_box_map()

    # (опционально) вернём Product ID строкой, чтобы менеджеру было проще
    product_rec_ids = list({pid for pid in listing_to_product.values()})
    productid_map = fetch_products_product_id(product_rec_ids)

    total_cartons = 0
    total_rounded_units = 0.0
    total_overstock_units = 0.0

    new_rows: List[Dict[str, Any]] = []

    for r in rows:
        listing_id = r.get("listing_id")
        need_units = float(r.get("order_qty") or 0)

        product_rec_id = listing_to_product.get(listing_id)
        product_id_human = productid_map.get(product_rec_id, "") if product_rec_id else ""

        if not product_rec_id:
            new_rows.append({
                **r,
                "product_record_id": None,
                "product_id": product_id_human,
                "units_per_box": None,
                "cartons": None,
                "rounded_units": None,
                "overstock_units": None,
                "overstock_pct": None,
                "status": "ERROR",
                "error_reason": "PRODUCT_NOT_FOUND_IN_PRODUCTMARKET",
            })
            continue

        units_per_box = units_per_box_map.get(product_rec_id)
        if not units_per_box:
            new_rows.append({
                **r,
                "product_record_id": product_rec_id,
                "product_id": product_id_human,
                "units_per_box": None,
                "cartons": None,
                "rounded_units": None,
                "overstock_units": None,
                "overstock_pct": None,
                "status": "ERROR",
                "error_reason": "BOX_NOT_FOUND_FOR_PRODUCT",
            })
            continue

        if need_units <= 0:
            new_rows.append({
                **r,
                "product_record_id": product_rec_id,
                "product_id": product_id_human,
                "units_per_box": units_per_box,
                "cartons": 0,
                "rounded_units": 0,
                "overstock_units": 0,
                "overstock_pct": 0,
                "status": "OK",
            })
            continue

        cartons = int(math.ceil(need_units / units_per_box))
        rounded_units = cartons * units_per_box
        overstock_units = rounded_units - need_units
        overstock_pct = (overstock_units / need_units) if need_units > 0 else 0

        total_cartons += cartons
        total_rounded_units += float(rounded_units)
        total_overstock_units += float(overstock_units)

        new_rows.append({
            **r,
            "product_record_id": product_rec_id,
            "product_id": product_id_human,        # удобно для менеджера
            "units_per_box": units_per_box,
            "cartons": cartons,
            "rounded_units": round(float(rounded_units), 4),
            "overstock_units": round(float(overstock_units), 4),
            "overstock_pct": round(float(overstock_pct), 6),
            "status": "OK",
        })

    base["rows"] = new_rows
    base["totals"] = {
        **(base.get("totals") or {}),
        "cartons": total_cartons,
        "rounded_units": round(total_rounded_units, 4),
        "overstock_units": round(total_overstock_units, 4),
    }
    return base


# ======================
# DEBUG ENDPOINTS (SAFE)
# ======================

def require_debug_token(token: str):
    if DEBUG_TOKEN and token != DEBUG_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid debug token.")


@app.get("/debug/sample")
def debug_sample(limit: int = 5, token: str = ""):
    require_debug_token(token)

    table = get_airtable_table_daily()

    recs = table.all(
        max_records=limit,
        fields=[F_DATE, F_LISTING_ID, F_MARKET, F_FORECAST]
    )

    return {
        "table": AIRTABLE_TABLE_DAILY,
        "fields_used": [F_DATE, F_LISTING_ID, F_MARKET, F_FORECAST],
        "count": len(recs),
        "sample": [r.get("fields", {}) for r in recs],
    }


@app.get("/debug/interval")
def debug_interval(start: str, end: str, market: str = "", limit: int = 5, token: str = ""):
    require_debug_token(token)

    start_dt = parse_date(start)
    end_dt = parse_date(end)
    if end_dt < start_dt:
        raise HTTPException(status_code=400, detail="end must be >= start")

    end_excl = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d")
    start_str = start_dt.strftime("%Y-%m-%d")

    table = get_airtable_table_daily()
    formula = build_date_filter(start_str, end_excl)

    recs = table.all(
        formula=formula,
        max_records=200,
        fields=[F_DATE, F_LISTING_ID, F_MARKET, F_FORECAST]
    )

    out = []
    for r in recs:
        f = r.get("fields", {})
        if market:
            markets = as_list(f.get(F_MARKET))
            if market not in markets:
                continue
        out.append(f)
        if len(out) >= limit:
            break

    return {
        "formula": formula,
        "market_filter": market,
        "returned": len(out),
        "sample": out
    }
