import json

SINGULAR_RULE_REASONS = {
    "assert_delivered_orders_have_delivery_date": (
        "Đơn hàng trạng thái 'delivered' nhưng thiếu ngày giao hàng thực tế (delivered_date_key NULL hoặc < 0)"
    ),
    "assert_fact_orders_match_sales_amount": (
        "Tổng tiền mặt hàng trên fact_orders bị lệch so với tổng item_price trên fact_sales (> 0.01)"
    ),
    "assert_fact_orders_match_sales_items_count": (
        "Số lượng mặt hàng trên fact_orders không khớp số dòng thực tế trên fact_sales"
    ),
    "assert_order_dates_chronology": (
        "Thứ tự thời gian đơn hàng không hợp lệ (approved_date_key hoặc delivered_date_key nhỏ hơn purchase_date_key)"
    ),
    "assert_sales_amounts_positive": (
        "Giá trị doanh thu hoặc phí vận chuyển âm (< 0) trên fact_sales"
    ),
    "assert_valid_review_score": (
        "Điểm đánh giá review_score nằm ngoài khoảng hợp lệ 1.0 - 5.0"
    ),
}


def infer_layer(node, fallback_layer=None):
    sources = node.get("sources") or []
    if sources:
        return "bronze"

    attached = str(node.get("attached_node") or "").lower()
    refs = [str(r.get("name") or "").lower() for r in (node.get("refs") or []) if isinstance(r, dict)]
    fqn = [str(x).lower() for x in (node.get("fqn") or [])]

    if "staging" in fqn or ".stg_" in attached or ".snp_" in attached:
        return "silver"
    if any(name.startswith("stg_") or name.startswith("snp_") for name in refs):
        return "silver"

    if "marts" in fqn or ".dim_" in attached or ".fact_" in attached:
        return "gold"
    if any(name.startswith("dim_") or name.startswith("fact_") for name in refs):
        return "gold"

    if fallback_layer in ("bronze", "silver", "gold"):
        return fallback_layer
    return "gold"


def infer_table_name(node):
    sources = node.get("sources") or []
    if sources and isinstance(sources[0], list) and sources[0]:
        return str(sources[0][-1])

    attached = str(node.get("attached_node") or "")
    if attached:
        return attached.split(".")[-1]

    refs = [str(r.get("name")) for r in (node.get("refs") or []) if isinstance(r, dict) and r.get("name")]
    if refs:
        unique_refs = list(dict.fromkeys(refs))
        return unique_refs[0] if len(unique_refs) == 1 else " & ".join(unique_refs)

    return str(node.get("name") or "unknown")


def infer_test_info(node):
    test_meta = node.get("test_metadata") or {}
    col_name = node.get("column_name")

    if test_meta:
        kwargs = test_meta.get("kwargs") or {}
        if not col_name and kwargs.get("column_name"):
            col_name = str(kwargs.get("column_name"))
        short_name = str(test_meta.get("name") or node.get("name") or "generic_test")

        if short_name == "not_null":
            reason = f"Cột '{col_name}' bị NULL" if col_name else "Giá trị bắt buộc bị NULL"
        elif short_name == "unique":
            reason = f"Giá trị cột '{col_name}' bị trùng lặp" if col_name else "Khóa chính bị trùng lặp"
        elif short_name == "unique_combination":
            cols = kwargs.get("combination_of_columns") or []
            if not col_name and cols:
                col_name = ", ".join(str(c) for c in cols)
            reason = f"Tổ hợp cột ({col_name}) bị trùng lặp" if col_name else "Tổ hợp khóa bị trùng lặp"
        elif short_name == "accepted_values":
            vals = kwargs.get("values") or []
            reason = f"Giá trị cột '{col_name}' nằm ngoài tập hợp lệ {vals}"
        elif short_name == "relationships":
            to_expr = str(kwargs.get("to") or "")
            field = str(kwargs.get("field") or "")
            reason = f"Vi phạm khóa ngoại '{col_name}' -> {to_expr}({field})"
        elif short_name == "positive_value":
            reason = f"Giá trị cột '{col_name}' bị âm (< 0)"
        else:
            reason = f"Vi phạm kiểm định '{short_name}'" + (f" trên cột '{col_name}'" if col_name else "")

        return short_name, col_name, reason

    test_name = str(node.get("name") or "singular_test")
    reason = SINGULAR_RULE_REASONS.get(test_name, f"Vi phạm quy tắc nghiệp vụ '{test_name}'")
    return test_name, col_name, reason


def normalize_failure_row(row_dict, run_id, layer, node, severity="error"):
    clean_row = {k: v for k, v in row_dict.items() if k != "_duplicate_count"}
    table_name = infer_table_name(node)
    test_name, column_name, error_reason = infer_test_info(node)

    return {
        "run_id": str(run_id),
        "layer": str(layer),
        "table_name": str(table_name),
        "test_name": str(test_name),
        "column_name": str(column_name) if column_name is not None else None,
        "error_reason": str(error_reason),
        "severity": str(severity),
        "row_data_json": json.dumps(clean_row, ensure_ascii=False, default=str),
    }
