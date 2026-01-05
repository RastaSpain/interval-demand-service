import math
from typing import Dict, Any, List, Tuple, Optional

def listing_to_product_id(listing_id: str) -> str:
    """
    'LP-Red-USA' -> 'LP-Red'
    '4-Roses-Red-USA' -> '4-Roses-Red'
    """
    if not listing_id:
        return ""
    parts = listing_id.split("-")
    if len(parts) <= 1:
        return listing_id
    # отрезаем последний сегмент (market)
    return "-".join(parts[:-1])


def build_products_map(products_records: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    Airtable Products: { "id": "recXXX", "fields": { "Product ID": "LP-Red", ... } }
    -> { "LP-Red": "recXXX" }
    """
    m: Dict[str, str] = {}
    for r in products_records:
        rid = r.get("id")
        fields = r.get("fields") or {}
        pid = (fields.get("Product ID") or "").strip()
        if rid and pid:
            m[pid] = rid
    return m


def build_box_map(box_records: List[Dict[str, Any]],
                  products_link_field: str = "Products",
                  units_field: str = "Кол-во в коробке") -> Dict[str, int]:
    """
    Airtable Shiping Box sizes cm:
      fields["Products"] = ["recProd1", "recProd2", ...]
      fields["Кол-во в коробке"] = 24
    -> { "recProd1": 24, "recProd2": 24, ... }
    Если один продукт встречается в двух строках с разными units — это конфликт.
    """
    m: Dict[str, int] = {}
    conflicts: List[Tuple[str, int, int]] = []

    for r in box_records:
        fields = r.get("fields") or {}
        prod_ids = fields.get(products_link_field) or []
        units = fields.get(units_field)

        if units is None:
            continue

        try:
            units_int = int(units)
        except Exception:
            continue

        if units_int <= 0:
            continue

        if not isinstance(prod_ids, list):
            prod_ids = [prod_ids]

        for pid in prod_ids:
            if not pid:
                continue
            if pid in m and m[pid] != units_int:
                conflicts.append((pid, m[pid], units_int))
            else:
                m[pid] = units_int

    if conflicts:
        # можно сделать жёсткий raise, но я бы лучше логировал и оставлял как есть (первое значение)
        # raise ValueError(f"Box mapping conflicts: {conflicts[:10]}")
        pass

    return m


def cartonize_rows(
    rows: List[Dict[str, Any]],
    product_id_to_record_id: Dict[str, str],
    product_record_id_to_units_per_carton: Dict[str, int],
) -> List[Dict[str, Any]]:
    """
    rows: [{listing_id, forecast_units, start_stock, safety_units, order_qty}, ...]
    Возвращает rows с добавленными полями cartons/rounded_units/overstock.
    """
    out: List[Dict[str, Any]] = []

    for r in rows:
        listing_id = (r.get("listing_id") or "").strip()
        need_units = float(r.get("order_qty") or 0)

        product_id = listing_to_product_id(listing_id)
        prod_rec = product_id_to_record_id.get(product_id)

        if not product_id or not prod_rec:
            out.append({
                **r,
                "product_id": product_id,
                "units_per_carton": None,
                "cartons": None,
                "rounded_units": None,
                "overstock_units": None,
                "overstock_pct": None,
                "status": "ERROR",
                "error_reason": "PRODUCT_NOT_FOUND",
            })
            continue

        units_per_carton = product_record_id_to_units_per_carton.get(prod_rec)

        if not units_per_carton:
            out.append({
                **r,
                "product_id": product_id,
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
            # если нужно 0 — можно вернуть 0 коробок без ошибки
            out.append({
                **r,
                "product_id": product_id,
                "units_per_carton": units_per_carton,
                "cartons": 0,
                "rounded_units": 0,
                "overstock_units": 0,
                "overstock_pct": 0,
                "status": "OK",
            })
            continue

        cartons = int(math.ceil(need_units / units_per_carton))
        rounded_units = cartons * units_per_carton
        overstock_units = rounded_units - need_units
        overstock_pct = overstock_units / need_units if need_units > 0 else 0

        out.append({
            **r,
            "product_id": product_id,
            "units_per_carton": units_per_carton,
            "cartons": cartons,
            "rounded_units": rounded_units,
            "overstock_units": overstock_units,
            "overstock_pct": overstock_pct,
            "status": "OK",
        })

    return out
