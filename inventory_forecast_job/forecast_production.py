#!/usr/bin/env python3
"""
Amazon Inventory Forecast - Webhook-ready Production Script

Запуск:
- Через FastAPI endpoint (см. app.py)
- Рассчитывает прогноз по всем товарам и marketplace
- Берёт последние остатки по asin+marketplace
- Sales Planned берёт из Sales Plan Daily за период TODAY..TARGET
- Сохраняет результат в Airtable Results

ВАЖНО:
- Germany (DE) без данных НЕ вызывает ошибку: marketplace просто пропустится.
"""

import os
import requests
from datetime import date
from typing import Dict, List, Optional, Any


# ============================================================================
# CONFIG
# ============================================================================

AIRTABLE_TOKEN = os.environ.get("AIRTABLE_TOKEN")
BASE_ID = os.environ.get("AIRTABLE_BASE_ID", "appHbiHFRAWtx2ErO")

if not AIRTABLE_TOKEN:
    raise RuntimeError("AIRTABLE_TOKEN environment variable not set")

HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json",
}

# Airtable Table IDs (как у тебя)
TABLE_INVENTORY = "tblvdUXLGMbN5rVJL"   # Остатки Амазон
TABLE_SALES_PLAN = "tblRLB6E83lHg6h7b"  # Sales Plan Daily
TABLE_RESULTS = "tblU17E0bqiQ8PMfD"     # Inventory Forecast Results

DEFAULT_MARKETPLACES = ["USA", "CA", "UK", "DE"]


# ============================================================================
# AIRTABLE HELPERS
# ============================================================================

def get_records(table_id: str, formula: str = None, fields: List[str] = None) -> List[Dict[str, Any]]:
    url = f"https://api.airtable.com/v0/{BASE_ID}/{table_id}"
    params: Dict[str, Any] = {"pageSize": 100}

    if formula:
        params["filterByFormula"] = formula
    if fields:
        # Airtable expects repeated fields[] query params
        params["fields[]"] = fields

    all_records: List[Dict[str, Any]] = []
    while True:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        all_records.extend(data.get("records", []))

        offset = data.get("offset")
        if offset:
            params["offset"] = offset
        else:
            break

    return all_records


def create_records(table_id: str, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    url = f"https://api.airtable.com/v0/{BASE_ID}/{table_id}"
    created: List[Dict[str, Any]] = []

    # Airtable batch limit 10
    for i in range(0, len(records), 10):
        batch = records[i:i + 10]
        payload = {"records": batch}
        resp = requests.post(url, headers=HEADERS, json=payload, timeout=30)
        resp.raise_for_status()
        created.extend(resp.json().get("records", []))

    return created


# ============================================================================
# DATA FETCH
# ============================================================================

def get_all_products_inventory(marketplace: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Берём все записи inventory, фильтруем, и выбираем latest по asin+marketplace.
    Ожидается, что в таблице есть поля:
      - asin
      - Marketplace (from Maketplace)  (linked/lookup list)
      - Product ID (from Products)     (lookup list)
      - PHYSICAL_FBA_STOCK
      - AWD
      - INBOUND_TOTAL
      - lastUpdatedTime
    """

    formula_parts = ['NOT({Product ID (from Products)} = "")']
    if marketplace:
        # Пытаемся отфильтровать marketplace
        formula_parts.append(f'FIND("{marketplace}", {{Marketplace (from Maketplace)}})')

    formula = f"AND({', '.join(formula_parts)})"

    records = get_records(TABLE_INVENTORY, formula=formula)

    latest: Dict[str, Dict[str, Any]] = {}

    for r in records:
        f = r.get("fields", {})

        asin = f.get("asin")
        if not asin:
            continue

        mp_list = f.get("Marketplace (from Maketplace)", [""])
        mp = mp_list[0] if isinstance(mp_list, list) and mp_list else ""
        if not mp:
            continue

        last_updated = f.get("lastUpdatedTime", "")

        key = f"{asin}-{mp}"

        current = latest.get(key)
        # сравниваем строкой (ISO обычно сравнимо)
        if (current is None) or (last_updated and last_updated > current.get("last_updated", "")):
            product_id_list = f.get("Product ID (from Products)", [""])
            product_id = product_id_list[0] if isinstance(product_id_list, list) and product_id_list else ""

            latest[key] = {
                "asin": asin,
                "marketplace": mp,
                "product_id": product_id,
                "physical_fba_stock": f.get("PHYSICAL_FBA_STOCK", 0) or 0,
                "awd": f.get("AWD", 0) or 0,
                "inbound_total": f.get("INBOUND_TOTAL", 0) or 0,
                "last_updated": last_updated,
            }

    return list(latest.values())


def get_sales_plan(asin: str, marketplace: str, start_date: date, end_date: date) -> Dict[str, Any]:
    """
    Достаём записи Sales Plan Daily за период и суммируем Planned units.
    Важно: Airtable поля должны совпадать с формулой.
    """

    formula = (
        f'AND('
        f'FIND("{asin}", {{ASIN (from Listing ID) 2}}), '
        f'FIND("{marketplace}", {{Marketplace (from Marketplace) (from Listing ID)}}), '
        f'{{Date}} >= "{start_date.isoformat()}", '
        f'{{Date}} <= "{end_date.isoformat()}"'
        f')'
    )

    records = get_records(TABLE_SALES_PLAN, formula=formula)

    total_units = 0
    for r in records:
        total_units += r.get("fields", {}).get("Planned units", 0) or 0

    period_days = max((end_date - start_date).days, 0)
    avg_daily = (total_units / period_days) if period_days > 0 else 0

    return {
        "total_units": float(total_units),
        "days_count": len(records),
        "period_days": period_days,
        "avg_daily": float(avg_daily),
    }


# ============================================================================
# FORECAST
# ============================================================================

def calculate_forecast(
    inventory: Dict[str, Any],
    start_date: date,
    end_date: date,
    verbose: bool = False,
) -> Optional[Dict[str, Any]]:
    asin = inventory["asin"]
    marketplace = inventory["marketplace"]

    starting_stock = (inventory["physical_fba_stock"] or 0) + (inventory["awd"] or 0)
    inbound_expected = inventory["inbound_total"] or 0

    sales = get_sales_plan(asin, marketplace, start_date, end_date)
    sales_planned = sales["total_units"]

    projected_stock = starting_stock + inbound_expected - sales_planned
    days_supply = int(projected_stock / sales["avg_daily"]) if sales["avg_daily"] > 0 else 0

    if verbose:
        print(f"\n--- {inventory.get('product_id','Unknown')} ({asin}) [{marketplace}] ---")
        print(f"Start: {starting_stock} (FBA {inventory['physical_fba_stock']}, AWD {inventory['awd']})")
        print(f"Inbound: {inbound_expected}")
        print(f"Sales planned: {sales_planned} (avg {sales['avg_daily']}/day)")
        print(f"Projected: {projected_stock} | Days supply: {days_supply}")

    return {
        "asin": asin,
        "marketplace": marketplace,
        "product_id": inventory.get("product_id", ""),
        "starting_stock_fba": int(inventory["physical_fba_stock"]),
        "starting_stock_awd": int(inventory["awd"]),
        "starting_stock_total": int(starting_stock),
        "inbound_expected": int(inbound_expected),
        "sales_planned": float(sales_planned),
        "projected_stock": int(projected_stock),
        "days_of_supply": int(days_supply),
        "avg_daily_sales": float(sales["avg_daily"]),
    }


def save_forecast_results(
    forecasts: List[Dict[str, Any]],
    start_date: date,
    end_date: date
) -> int:
    if not forecasts:
        return 0

    records: List[Dict[str, Any]] = []
    for f in forecasts:
        records.append({
            "fields": {
                "ASIN": f["asin"],
                "Marketplace": f["marketplace"],
                "Product ID": f["product_id"],
                "Calculation Date": start_date.isoformat(),
                "Target Date": end_date.isoformat(),
                "Scenario": "base",

                "Current Stock Total": f["starting_stock_total"],
                "Stock AWD": f["starting_stock_awd"],
                "Inbound Expected": f["inbound_expected"],
                "Sales Planned": f["sales_planned"],
                "Projected Stock": f["projected_stock"],
                "Days of Supply": f["days_of_supply"],

                "Validation Status": "NOT_CHECKED",
                "Notes": f"Auto-generated forecast. Period: {start_date} to {end_date}"
            }
        })

    created = create_records(TABLE_RESULTS, records)
    return len(created)


# ============================================================================
# PUBLIC ENTRY (for webhook)
# ============================================================================

def run_forecast(
    target_date: date,
    marketplaces: Optional[List[str]] = None,
    verbose: bool = False
) -> Dict[str, Any]:
    """
    Главная функция: считает прогноз и сохраняет результаты.
    Возвращает summary (удобно для n8n).
    """
    today = date.today()
    marketplaces = marketplaces or DEFAULT_MARKETPLACES

    all_forecasts: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for mp in marketplaces:
        try:
            products = get_all_products_inventory(mp)
            if not products:
                # Нормально: по DE может быть пусто
                continue

            for inv in products:
                try:
                    f = calculate_forecast(inv, today, target_date, verbose=verbose)
                    if f:
                        all_forecasts.append(f)
                except Exception as e:
                    errors.append({"asin": inv.get("asin"), "marketplace": mp, "error": str(e)})

        except Exception as e:
            errors.append({"marketplace": mp, "error": str(e)})

    saved_count = save_forecast_results(all_forecasts, today, target_date)

    low_stock = [f for f in all_forecasts if f["days_of_supply"] < 30]
    critical_stock = [f for f in all_forecasts if f["projected_stock"] < 0]

    by_marketplace: Dict[str, int] = {}
    for f in all_forecasts:
        by_marketplace[f["marketplace"]] = by_marketplace.get(f["marketplace"], 0) + 1

    return {
        "ok": True,
        "calculation_date": today.isoformat(),
        "target_date": target_date.isoformat(),
        "marketplaces_requested": marketplaces,
        "forecasts_calculated": len(all_forecasts),
        "forecasts_saved": saved_count,
        "by_marketplace": by_marketplace,
        "warnings": {
            "low_stock_count": len(low_stock),
            "critical_stock_count": len(critical_stock),
            "low_stock": [
                {
                    "product_id": f["product_id"],
                    "asin": f["asin"],
                    "marketplace": f["marketplace"],
                    "days_of_supply": f["days_of_supply"],
                    "projected_stock": f["projected_stock"],
                } for f in low_stock
            ],
            "critical_stock": [
                {
                    "product_id": f["product_id"],
                    "asin": f["asin"],
                    "marketplace": f["marketplace"],
                    "days_of_supply": f["days_of_supply"],
                    "projected_stock": f["projected_stock"],
                } for f in critical_stock
            ],
        },
        "errors": errors,
    }
