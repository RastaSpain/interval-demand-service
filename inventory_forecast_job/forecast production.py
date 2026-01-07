#!/usr/bin/env python3
"""
Amazon Inventory Forecast - Production Script
Базовый прогноз остатков для всех товаров и marketplace

Поддерживаемые marketplace: USA, CA, UK, DE
Логика:
- Starting Stock = PHYSICAL_FBA_STOCK + AWD
- Inbound Expected = INBOUND_TOTAL (все прибывает в срок)
- Sales Planned = реальные данные из Sales Plan Daily
- Projected Stock = Starting Stock + Inbound Expected - Sales Planned
"""

import os
import sys
import requests
from datetime import date, datetime
import json
from typing import Dict, List, Optional

# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

# Airtable credentials (через environment variables)
AIRTABLE_TOKEN = os.environ.get("AIRTABLE_TOKEN")
BASE_ID = os.environ.get("AIRTABLE_BASE_ID", "appHbiHFRAWtx2ErO")

if not AIRTABLE_TOKEN:
    print("❌ ERROR: AIRTABLE_TOKEN environment variable not set!")
    print("   Set it with: export AIRTABLE_TOKEN='your_token'")
    sys.exit(1)

HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_TOKEN}",
    "Content-Type": "application/json"
}

# Таблицы
TABLE_INVENTORY = "tblvdUXLGMbN5rVJL"  # Остатки Амазон
TABLE_SALES_PLAN = "tblRLB6E83lHg6h7b"  # Sales Plan Daily
TABLE_RESULTS = "tblU17E0bqiQ8PMfD"     # Inventory Forecast Results

# Даты расчета (можно передать через аргументы)
TODAY = date.today()
TARGET_DATE = date(2026, 4, 1)  # По умолчанию

# Marketplace для расчета (все активные)
MARKETPLACES = ["USA", "CA", "UK", "DE"]

# ============================================================================
# ФУНКЦИИ AIRTABLE API
# ============================================================================

def get_records(table_id: str, formula: str = None, fields: List[str] = None) -> List[Dict]:
    """Получить записи из таблицы Airtable"""
    url = f"https://api.airtable.com/v0/{BASE_ID}/{table_id}"
    params = {"pageSize": 100}
    
    if formula:
        params["filterByFormula"] = formula
    if fields:
        for field in fields:
            params.setdefault("fields[]", []).append(field)
    
    all_records = []
    while True:
        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            all_records.extend(data.get("records", []))
            
            if "offset" in data:
                params["offset"] = data["offset"]
            else:
                break
        except Exception as e:
            print(f"❌ Error fetching records: {e}")
            break
    
    return all_records


def create_records(table_id: str, records: List[Dict]) -> List[Dict]:
    """Создать записи в таблице Airtable (батчами по 10)"""
    url = f"https://api.airtable.com/v0/{BASE_ID}/{table_id}"
    
    results = []
    for i in range(0, len(records), 10):
        batch = records[i:i+10]
        payload = {"records": batch}
        
        try:
            response = requests.post(url, headers=HEADERS, json=payload, timeout=30)
            response.raise_for_status()
            results.extend(response.json().get("records", []))
        except Exception as e:
            print(f"❌ Error creating records (batch {i//10 + 1}): {e}")
    
    return results


# ============================================================================
# ПОЛУЧЕНИЕ ДАННЫХ
# ============================================================================

def get_all_products_inventory(marketplace: str = None) -> List[Dict]:
    """Получить остатки для всех товаров (опционально по marketplace)"""
    formula_parts = ['NOT({Product ID (from Products)} = "")']
    
    if marketplace:
        formula_parts.append(f'FIND("{marketplace}", {{Marketplace (from Maketplace)}})')
    
    formula = f"AND({', '.join(formula_parts)})"
    
    records = get_records(TABLE_INVENTORY, formula=formula)
    
    # Группируем по ASIN-Marketplace и берем последние записи
    latest_records = {}
    for record in records:
        fields = record["fields"]
        asin = fields.get("asin")
        mp = fields.get("Marketplace (from Maketplace)", [""])[0]
        last_updated = fields.get("lastUpdatedTime", "")
        
        key = f"{asin}-{mp}"
        
        if key not in latest_records or last_updated > latest_records[key].get("lastUpdatedTime", ""):
            latest_records[key] = {
                "asin": asin,
                "marketplace": mp,
                "product_id": fields.get("Product ID (from Products)", [""])[0],
                "physical_fba_stock": fields.get("PHYSICAL_FBA_STOCK", 0),
                "awd": fields.get("AWD", 0),
                "inbound_total": fields.get("INBOUND_TOTAL", 0),
                "last_updated": last_updated,
            }
    
    return list(latest_records.values())


def get_sales_plan(asin: str, marketplace: str, start_date: date, end_date: date) -> Dict:
    """Получить Sales Plan для товара за период"""
    formula = (
        f'AND('
        f'FIND("{asin}", {{ASIN (from Listing ID) 2}}), '
        f'FIND("{marketplace}", {{Marketplace (from Marketplace) (from Listing ID)}}), '
        f'{{Date}} >= "{start_date.isoformat()}", '
        f'{{Date}} <= "{end_date.isoformat()}"'
        f')'
    )
    
    records = get_records(TABLE_SALES_PLAN, formula=formula)
    
    # Суммируем Planned units
    total_sales = sum(r["fields"].get("Planned units", 0) for r in records)
    days_diff = (end_date - start_date).days
    
    return {
        "total_units": total_sales,
        "days_count": len(records),
        "avg_daily": total_sales / days_diff if days_diff > 0 else 0,
        "period_days": days_diff
    }


# ============================================================================
# РАСЧЕТ ПРОГНОЗА
# ============================================================================

def calculate_forecast(
    asin: str, 
    marketplace: str,
    inventory: Dict,
    start_date: date,
    end_date: date,
    verbose: bool = True
) -> Optional[Dict]:
    """Рассчитать прогноз для товара"""
    
    if verbose:
        print(f"\n{'─'*80}")
        print(f"📊 {inventory.get('product_id', 'Unknown')} ({asin}) - {marketplace}")
        print(f"{'─'*80}")
    
    # 1. Starting Stock
    starting_stock = inventory['physical_fba_stock'] + inventory['awd']
    
    if verbose:
        print(f"Starting Stock: {starting_stock} (FBA: {inventory['physical_fba_stock']}, AWD: {inventory['awd']})")
    
    # 2. Inbound Expected
    inbound_expected = inventory['inbound_total']
    
    if verbose:
        print(f"Inbound Expected: {inbound_expected}")
    
    # 3. Sales Planned
    sales = get_sales_plan(asin, marketplace, start_date, end_date)
    sales_planned = sales['total_units']
    
    if verbose:
        print(f"Sales Planned: {sales_planned:.2f} ({sales['days_count']} days, avg: {sales['avg_daily']:.2f}/day)")
    
    # 4. Прогноз
    projected_stock = starting_stock + inbound_expected - sales_planned
    days_supply = int(projected_stock / sales['avg_daily']) if sales['avg_daily'] > 0 else 0
    
    if verbose:
        print(f"➡️  Projected: {projected_stock:.0f} units, {days_supply} days supply")
        
        if days_supply < 30:
            print(f"   ⚠️  LOW STOCK WARNING!")
        if projected_stock < 0:
            print(f"   🚨 CRITICAL: Negative stock!")
    
    return {
        "asin": asin,
        "marketplace": marketplace,
        "product_id": inventory.get('product_id', ''),
        "starting_stock_fba": inventory['physical_fba_stock'],
        "starting_stock_awd": inventory['awd'],
        "starting_stock_total": starting_stock,
        "inbound_expected": inbound_expected,
        "sales_planned": sales_planned,
        "projected_stock": int(projected_stock),
        "days_of_supply": days_supply,
        "avg_daily_sales": sales['avg_daily'],
    }


# ============================================================================
# СОХРАНЕНИЕ РЕЗУЛЬТАТОВ
# ============================================================================

def save_forecast_results(forecasts: List[Dict], start_date: date, end_date: date) -> List[Dict]:
    """Сохранить результаты прогноза в Airtable"""
    
    if not forecasts:
        print("\n❌ No forecasts to save")
        return []
    
    records = []
    for f in forecasts:
        records.append({
            "fields": {
                "ASIN": f['asin'],
                "Marketplace": f['marketplace'],
                "Product ID": f['product_id'],
                "Calculation Date": start_date.isoformat(),
                "Target Date": end_date.isoformat(),
                "Scenario": "base",
                
                "Current Stock Total": f['starting_stock_total'],
                "Stock AWD": f['starting_stock_awd'],
                "Inbound Expected": f['inbound_expected'],
                "Sales Planned": f['sales_planned'],
                "Projected Stock": f['projected_stock'],
                "Days of Supply": f['days_of_supply'],
                
                "Validation Status": "NOT_CHECKED",
                "Notes": f"Auto-generated forecast. Period: {start_date} to {end_date}"
            }
        })
    
    print(f"\n{'='*80}")
    print(f"💾 Saving {len(records)} forecast(s) to Airtable...")
    print(f"{'='*80}")
    
    created = create_records(TABLE_RESULTS, records)
    
    print(f"✅ Successfully saved {len(created)} records")
    
    return created


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Основная функция"""
    
    print(f"\n{'='*80}")
    print(f"🚀 AMAZON INVENTORY FORECAST - PRODUCTION")
    print(f"{'='*80}")
    print(f"📅 Calculation Date: {TODAY}")
    print(f"🎯 Target Date: {TARGET_DATE}")
    print(f"⏱️  Period: {(TARGET_DATE - TODAY).days} days")
    print(f"🌍 Marketplaces: {', '.join(MARKETPLACES)}")
    
    all_forecasts = []
    
    # Расчет по каждому marketplace
    for marketplace in MARKETPLACES:
        print(f"\n{'='*80}")
        print(f"🌍 MARKETPLACE: {marketplace}")
        print(f"{'='*80}")
        
        # Получаем остатки для всех товаров в marketplace
        products = get_all_products_inventory(marketplace)
        
        if not products:
            print(f"⚠️  No inventory data found for {marketplace}")
            continue
        
        print(f"Found {len(products)} product(s) in {marketplace}")
        
        # Рассчитываем прогноз для каждого товара
        for inventory in products:
            try:
                forecast = calculate_forecast(
                    asin=inventory['asin'],
                    marketplace=marketplace,
                    inventory=inventory,
                    start_date=TODAY,
                    end_date=TARGET_DATE,
                    verbose=True
                )
                
                if forecast:
                    all_forecasts.append(forecast)
                    
            except Exception as e:
                print(f"❌ Error calculating forecast for {inventory.get('asin')}: {e}")
    
    # Сохранение результатов
    if all_forecasts:
        print(f"\n{'='*80}")
        print(f"📊 SUMMARY")
        print(f"{'='*80}")
        print(f"Total forecasts calculated: {len(all_forecasts)}")
        
        # Группировка по marketplace
        by_marketplace = {}
        for f in all_forecasts:
            mp = f['marketplace']
            by_marketplace.setdefault(mp, []).append(f)
        
        for mp, forecasts in by_marketplace.items():
            print(f"  {mp}: {len(forecasts)} product(s)")
        
        # Сохраняем в Airtable
        saved = save_forecast_results(all_forecasts, TODAY, TARGET_DATE)
        
        # Показываем предупреждения
        print(f"\n{'='*80}")
        print(f"⚠️  WARNINGS")
        print(f"{'='*80}")
        
        low_stock = [f for f in all_forecasts if f['days_of_supply'] < 30]
        critical_stock = [f for f in all_forecasts if f['projected_stock'] < 0]
        
        if low_stock:
            print(f"\n🟡 Low Stock ({len(low_stock)} products):")
            for f in low_stock:
                print(f"  - {f['product_id']} ({f['marketplace']}): {f['days_of_supply']} days")
        
        if critical_stock:
            print(f"\n🔴 Critical Stock ({len(critical_stock)} products):")
            for f in critical_stock:
                print(f"  - {f['product_id']} ({f['marketplace']}): {f['projected_stock']} units (NEGATIVE!)")
        
        if not low_stock and not critical_stock:
            print("✅ All products have sufficient stock!")
    
    print(f"\n{'='*80}")
    print(f"✅ FORECAST COMPLETED!")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
