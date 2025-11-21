import logging
from datetime import datetime
from transformators import Transformators
from utils import get_newest_data, update_last_processed, deduplicate_latest
import sys
from validator import validate
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
    
from load.supabase_class import SupabaseManager
from load.load_silver import load_to_silver

logger = logging.getLogger(__name__)

def main():
    supabase = SupabaseManager(default_schema="silver")
    # Read data from staging
    data = get_newest_data(supabase, "get_last_transform_date")
    
    if not data:
        logger.info("Không có dữ liệu mới để xử lý")
        return None
    
    # Khởi tạo ngày rất bé (mục tiêu để tìm ra ngày mới nhất ở dưới)
    last_processed_date = datetime(1000, 1, 1)
    
    valid_rows = []
    error_rows = []

    for row in data:
        if validate(row):
            try:
                clean_row = Transformators.transform_row(row)
                valid_rows.append(clean_row)
            except Exception as e:
                error_rows.append({**row, "error_message": str(e), "retry_status": "pending"})
        else:
            error_rows.append({**row, "error_message": "Thiếu các trường bắt buộc", "retry_status": "pending"})

        created_at_str = row.get("created_at")
        if created_at_str:
            normalized_created_at = created_at_str.replace("Z", "+00:00")
            try:
                created_at_dt = datetime.fromisoformat(normalized_created_at)
                last_processed_date = max(last_processed_date, created_at_dt)
            except ValueError:
                logger.warning("Không parse được created_at '%s'", created_at_str)
        else:
            logger.warning("Bản ghi thiếu trường created_at, bỏ qua cập nhật thời gian")
    
    
    valid_rows = deduplicate_latest(valid_rows, id_field="subpage_url")

    # Save data to silver (cơ chế upsert từ staging sang silver)
    load_to_silver(supabase, valid_rows, error_rows, on_conflict="subpage_url")
    logger.info(f"Đã xử lý {len(valid_rows)} dữ liệu hợp lệ và {len(error_rows)} dữ liệu lỗi")

    # Cập nhật ngày mới nhất
    update_last_processed(supabase, last_processed_date)
    logger.info(f"Đã cập nhật ngày mới nhất: {last_processed_date}")
    return None

if __name__ == "__main__":
    main()