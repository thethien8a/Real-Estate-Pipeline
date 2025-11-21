import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def get_newest_data(supabase, function_name: str):
    # B1: Lấy ra ngày cuối cùng đã xử lý 
    last_processed_date = supabase.call_rpc_function(function_name=function_name)
    if last_processed_date["success"]:
        # B2: Lấy ra dữ liệu mới nhất từ staging
        newest_data = supabase.query_with_conditions(table="staging", conditions={"created_at": {"gt": last_processed_date["data"]}}, schema="bronze")
        if newest_data["success"]:
            return newest_data["data"]
        else:
            logger.error(f"Lỗi khi lấy dữ liệu mới: {newest_data['error']}")
            return None
    else:
        logger.error(f"Lỗi khi lấy ngày cuối cùng đã xử lý: {last_processed_date['error']}")
        return None

def update_last_processed(supabase, last_processed_date):
    # Chuyển datetime thành string ISO format
    date_string = last_processed_date.isoformat() if isinstance(last_processed_date, datetime) else last_processed_date
    
    result = supabase.update(
        table="last_transform_date", 
        data={"last_date": date_string},  
        filters={"singleton": True}
    )
    
    if result["success"]:
        logger.info(f"Đã cập nhật ngày mới nhất: {date_string}")
        return result["data"]
    else:
        logger.error(f"Lỗi khi cập nhật ngày mới nhất: {result['error']}")
        return None


def _parse_datetime(value: str):
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except Exception:
        logger.warning("Không parse được store_staging_at '%s'", value)
        return None


def deduplicate_latest(rows, id_field: str = "post_id", timestamp_field: str = "store_staging_at"):
    """
    Loại bỏ các bản ghi trùng theo id_field, chỉ giữ bản ghi có timestamp_field mới nhất.
    """
    deduped = {}
    for row in rows:
        key = row.get(id_field)
        if key is None:
            continue

        current_dt = _parse_datetime(row.get(timestamp_field))
        existing = deduped.get(key)

        if existing is None:
            deduped[key] = row
            continue

        existing_dt = _parse_datetime(existing.get(timestamp_field))

        if existing_dt is None and current_dt is None:
            deduped[key] = row
            continue

        if existing_dt is None or (current_dt and current_dt >= existing_dt):
            deduped[key] = row

    if deduped:
        logger.info("Đã loại bỏ %s bản ghi trùng %s", len(rows) - len(deduped), id_field)
    return list(deduped.values())