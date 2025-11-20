import pandas as pd
import logging
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from load.supabase_class import SupabaseManager
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
            logger.error(f"Lỗi khi lấy dữ liệu mới: {newest_data["error"]}")
            return None
    else:
        logger.error(f"Lỗi khi lấy ngày cuối cùng đã xử lý: {last_processed_date["error"]}")
        return None

def update_last_processed(supabase, last_processed_date):
    pass

def load_to_silver(supabase, valid_rows, error_rows):
    pass

if __name__ == "__main__":
    supabase = SupabaseManager(default_schema="silver")
    last_processed_date = get_newest_data(supabase, function_name="get_last_transform_date")
    print(len(last_processed_date))