"""
Тестовый скрипт для проверки API
"""
import requests
import json
from datetime import datetime, timedelta

# URL вашего сервера (измените на актуальный)
BASE_URL = "http://localhost:8000"

def test_health():
    """Проверка health endpoint"""
    print("=" * 60)
    print("Testing /health endpoint...")
    response = requests.get(f"{BASE_URL}/health")
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}")
    print()

def test_interval_demand():
    """Проверка расчёта спроса"""
    print("=" * 60)
    print("Testing /calc/interval-demand endpoint...")
    
    # Формируем даты (следующие 30 дней)
    start = datetime.now().date()
    end = start + timedelta(days=30)
    
    payload = {
        "market": "USA",
        "interval_start": start.strftime("%Y-%m-%d"),
        "interval_end": end.strftime("%Y-%m-%d"),
        "start_stock_mode": "ZERO",
        "start_stock": {},
        "safety_days": 0
    }
    
    print(f"Request payload:")
    print(json.dumps(payload, indent=2))
    print()
    
    response = requests.post(
        f"{BASE_URL}/calc/interval-demand",
        json=payload
    )
    
    print(f"Status: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"\nResults:")
        print(f"Market: {data['market']}")
        print(f"Period: {data['interval_start']} to {data['interval_end']}")
        print(f"\nTotals:")
        print(f"  Listings: {data['totals']['listings']}")
        print(f"  Forecast units: {data['totals']['forecast_units']}")
        print(f"  Order qty: {data['totals']['order_qty']}")
        print(f"  Cartons: {data['totals']['cartons']}")
        print(f"  Rounded units: {data['totals']['rounded_units']}")
        print(f"  Overstock: {data['totals']['overstock_units']}")
        print(f"  Errors: {data['totals']['errors']}")
        
        print(f"\nFirst 5 rows:")
        for i, row in enumerate(data['rows'][:5], 1):
            print(f"\n{i}. Listing: {row['listing_id']}")
            print(f"   Forecast: {row['forecast_units']} units")
            print(f"   Order: {row['order_qty']} units")
            if row['status'] == 'OK':
                print(f"   Box: {row['units_per_carton']} units/carton")
                print(f"   Cartons: {row['cartons']}")
                print(f"   Rounded: {row['rounded_units']} units")
                print(f"   Overstock: {row['overstock_units']} ({row['overstock_pct']:.1%})")
            else:
                print(f"   ERROR: {row['error_reason']}")
    else:
        print(f"Error response:")
        print(json.dumps(response.json(), indent=2))

if __name__ == "__main__":
    try:
        print("\n" + "=" * 60)
        print("TESTING API")
        print("=" * 60 + "\n")
        
        test_health()
        test_interval_demand()
        
        print("\n" + "=" * 60)
        print("TESTS COMPLETED")
        print("=" * 60 + "\n")
        
    except requests.exceptions.ConnectionError:
        print("\nERROR: Cannot connect to server.")
        print("Make sure the server is running:")
        print("  uvicorn main:app --reload --port 8000")
    except Exception as e:
        print(f"\nERROR: {e}")
