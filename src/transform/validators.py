import pandas as pd
import logging
from ..load.supabase_class import SupabaseManager
logger = logging.getLogger(__name__)

def get_newest_data(...):
    # B1: Lấy ra ngày cuối cùng đã xử lý 
    supabase = SupabaseManager(default_schema="silver")
    last_processed_date = supabase.read(table="last_processed_date", columns="last_date")
    
    # B2: Lấy ra dữ liệu mới nhất từ staging
    data = supabase.read(table="staging", columns="*", filters={"created_at": ">", "last_processed_date"})
    return data
    
class Validators:
    def __init__(self):
        pass
    
    @staticmethod
    def validate_price(price):
        return price
    
    @staticmethod
    def validate_area(area):
        return area
    
    @staticmethod
    def validate_house_direction(house_direction):
    
    ...
    
    def validate_row(row):
        return row