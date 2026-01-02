import os
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from pyairtable import Api

app = FastAPI(title="Interval Demand Calculator", version="1.1.0")

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


def get_airtable_table():
    if not all([AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_DAILY]):
        raise HTTPException(status_code=500, detail="Airtable environment variables are not fully configured.")
    api = Api(AIRTABLE_API_KEY)
    return api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_DAILY)


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


@app.post("/calc/interval-demand")
def calc_interval_demand(req: CalcRequest) -> Dict[str, Any]:
    start_dt = parse_date(req.interval_start)
    end_dt = parse_date(req.interval_end)
    if end_dt < start_dt:
        raise HTTPException(status_code=400, detail="interval_end must be >= interval_start")

    start_str = start_dt.strftime("%Y-%m-%d")
    end_exclusive = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d")

    table = get_airtable_table()

    formula = build_date_filter(start_str, end_exclusive)

    # Pull only needed fields
    records = table.all(
        formula=formula,
        fields=[F_DATE, F_LISTING_ID, F_MARKET, F_FORECAST],
    )

    aggregated: Dict[str, float] = {}

    for r in records:
        f = r.get("fields", {})

        # Market is LIST (lookup/linked). Example: ["USA"]
        markets = as_list(f.get(F_MARKET))
        if req.market not in markets:
            continue

        # Listing ID also comes as LIST. Example: ["recmZ0LLMczkduqX2"]
        listing_id = first_or_none(f.get(F_LISTING_ID))
        if not listing_id:
            continue

        # Planned units should be numeric; handle None/strings safely
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


# ======================
# DEBUG ENDPOINTS (SAFE)
# ======================

def require_debug_token(token: str):
    if DEBUG_TOKEN and token != DEBUG_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid debug token.")


@app.get("/debug/sample")
def debug_sample(limit: int = 5, token: str = ""):
    """
    Returns a few raw rows so we can inspect how Airtable returns fields.
    If DEBUG_TOKEN is set in env, you must pass ?token=...
    """
    require_debug_token(token)

    table = get_airtable_table()

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
    """
    Shows sample rows for a date interval and optionally market filtering (in python).
    """
    require_debug_token(token)

    # validate dates
    start_dt = parse_date(start)
    end_dt = parse_date(end)
    if end_dt < start_dt:
        raise HTTPException(status_code=400, detail="end must be >= start")

    end_excl = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d")
    start_str = start_dt.strftime("%Y-%m-%d")

    table = get_airtable_table()
    formula = build_date_filter(start_str, end_excl)

    recs = table.all(
        formula=formula,
        max_records=200,  # fetch more then filter
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
