import logging
from datetime import datetime
from transformators import transform_row, update_last_processed
from validators import validate_row, get_newest_data

from ..load.load_silver import load_to_silver
logger = logging.getLogger(__name__)

def main():
    # Read data from staging
    data = get_newest_data(...)
    
    if not data:
        logger.info("Không có dữ liệu mới để xử lý")
        return None
    
    # Khởi tạo ngày rất bé (mục tiêu để tìm ra ngày mới nhất ở dưới)
    last_processed_date = datetime(1000, 1, 1)
    
    valid_rows = []
    error_rows = []
    
    for row in data:
        status_row, error_msg = validate_row(row)
        
        if status_row:
            try:
                clean_row = transform_row(row)
                valid_rows.append(clean_row)
            except Exception as e:
                error_rows.append({**row, "error": str(e), "fixed_row": "No"})
        else:
            error_rows.append({**row, "error": error_msg, "fixed_row": "No"})
        
        # Lấy ngày mới nhất
        temp = row['created_at']
        if temp > last_processed_date:
            last_processed_date = temp
            
    # Save data to silver
    load_to_silver(valid_rows,error_rows)
    logger.info(f"Đã xử lý {len(valid_rows)} dữ liệu hợp lệ và {len(error_rows)} dữ liệu lỗi")

    # Cập nhật ngày mới nhất
    update_last_processed(last_processed_date)
    logger.info(f"Đã cập nhật ngày mới nhất: {last_processed_date}")
    return None

if __name__ == "__main__":
    main()