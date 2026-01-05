import math
from typing import Dict, Any, List, Tuple, Optional


def build_box_map_from_productmarket(
    productmarket_records: List[Dict[str, Any]],
    box_records: List[Dict[str, Any]],
    productmarket_box_field: str = "Product and Box sizes cm",
    box_units_field: str = "Кол-во в коробке"
) -> Dict[str, int]:
    """
    Строит маппинг: ProductMarket Record ID -> units_per_carton
    
    Логика:
    1. Для каждого ProductMarket берём поле "Product and Box sizes cm" (список Record IDs коробок)
    2. Находим эти коробки в box_records
    3. Берём поле "Кол-во в коробке"
    4. Маппим ProductMarket Record ID -> units_per_carton
    
    Args:
        productmarket_records: записи из таблицы ProductMarket
        box_records: записи из таблицы Shiping Box sizes cm
        productmarket_box_field: название поля связи с коробками
        box_units_field: название поля с количеством в коробке
    
    Returns:
        Dict[str, int]: { "recProductMarketXXX": 18, ... }
    """
    # Сначала строим box_id -> units
    box_map: Dict[str, int] = {}
    for box_rec in box_records:
        box_id = box_rec.get("id")
        fields = box_rec.get("fields") or {}
        units = fields.get(box_units_field)
        
        if not box_id or units is None:
            continue
            
        try:
            units_int = int(units)
            if units_int > 0:
                box_map[box_id] = units_int
        except (ValueError, TypeError):
            continue
    
    # Теперь маппим ProductMarket -> units через связи с коробками
    result: Dict[str, int] = {}
    conflicts: List[Tuple[str, int, int]] = []
    
    for pm_rec in productmarket_records:
        pm_id = pm_rec.get("id")
        fields = pm_rec.get("fields") or {}
        box_ids = fields.get(productmarket_box_field) or []
        
        if not pm_id:
            continue
            
        if not isinstance(box_ids, list):
            box_ids = [box_ids] if box_ids else []
        
        # Берём первую коробку из связей
        for box_id in box_ids:
            if box_id in box_map:
                units = box_map[box_id]
                
                # Проверяем конфликты (если один ProductMarket связан с несколькими коробками)
                if pm_id in result and result[pm_id] != units:
                    conflicts.append((pm_id, result[pm_id], units))
                else:
                    result[pm_id] = units
                break  # Берём только первую найденную коробку
    
    if conflicts:
        # Логируем конфликты, но не падаем
        print(f"Warning: Box mapping conflicts detected: {conflicts[:5]}")
    
    return result


def cartonize_rows(
    rows: List[Dict[str, Any]],
    listing_id_to_units_per_carton: Dict[str, int],
) -> List[Dict[str, Any]]:
    """
    Добавляет к каждой строке расчёты по коробкам.
    
    Args:
        rows: [{listing_id, forecast_units, start_stock, order_qty}, ...]
        listing_id_to_units_per_carton: { "recListingXXX": 18, ... }
    
    Returns:
        List с добавленными полями:
        - units_per_carton: int
        - cartons: int (количество коробок)
        - rounded_units: int (units_per_carton * cartons)
        - overstock_units: float (rounded_units - order_qty)
        - overstock_pct: float (overstock_units / order_qty)
        - status: "OK" | "ERROR"
        - error_reason: str (если status == "ERROR")
    """
    out: List[Dict[str, Any]] = []

    for r in rows:
        listing_id = r.get("listing_id", "").strip()
        need_units = float(r.get("order_qty") or 0)
        
        units_per_carton = listing_id_to_units_per_carton.get(listing_id)
        
        if not units_per_carton:
            out.append({
                **r,
                "units_per_carton": None,
                "cartons": None,
                "rounded_units": None,
                "overstock_units": None,
                "overstock_pct": None,
                "status": "ERROR",
                "error_reason": "BOX_NOT_FOUND",
            })
            continue
        
        if need_units <= 0:
            out.append({
                **r,
                "units_per_carton": units_per_carton,
                "cartons": 0,
                "rounded_units": 0,
                "overstock_units": 0,
                "overstock_pct": 0,
                "status": "OK",
            })
            continue
        
        # Округляем ВВЕРХ до целого количества коробок
        cartons = int(math.ceil(need_units / units_per_carton))
        rounded_units = cartons * units_per_carton
        overstock_units = rounded_units - need_units
        overstock_pct = (overstock_units / need_units) if need_units > 0 else 0
        
        out.append({
            **r,
            "units_per_carton": units_per_carton,
            "cartons": cartons,
            "rounded_units": rounded_units,
            "overstock_units": round(overstock_units, 2),
            "overstock_pct": round(overstock_pct, 4),
            "status": "OK",
        })
    
    return out
