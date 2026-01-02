import os
from datetime import datetime, timedelta
from typing import Dict, Any, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from pyairtable import Api

app = FastAPI(title="Interval Demand Calculator", version="1.0.0")

# ======================
# ENV CONFIG
# ======================
AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_DAILY = os.getenv("AIRTABLE_TABLE_DAILY")

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


@app.post("/calc/interval-demand")
def calc_interval_demand(req: CalcRequest) -> Dict[str, Any]:

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
    table = api.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_DAILY)

    formula = build_filter(req.market, start_str, end_exclusive)

    records = table.all(
        formula=formula,
        fields=[F_DATE, F_LISTING_ID, F_MARKET, F_FORECAST]
    )

    aggregated: Dict[str, float] = {}

    for r in records:
        f = r.get("fields", {})
        listing_id = f.get(F_LISTING_ID)

        if not listing_id:
            continue

        try:
            units = float(f.get(F_FORECAST, 0))
        except (TypeError, ValueError):
            units = 0.0

        aggregated[listing_id] = aggregated.get(listing_id, 0.0) + units

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

    rows.sort(key=lambda x: x["order_qty"], reverse=True)

    return {
        "market": req.market,
        "interval_start": req.interval_start,
        "interval_end": req.interval_end,
        "rows": rows,
        "totals": {
            "listings": len(rows),
            "forecast_units": round(total_forecast, 2),
            "order_qty": round(sum(r["order_qty"] for r in rows), 2)
        }
    }
