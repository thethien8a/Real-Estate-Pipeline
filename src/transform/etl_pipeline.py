"""
ETL Pipeline: Raw → Staging → Warehouse
Xử lý dữ liệu từ Raw layer sang Staging và Warehouse
"""

from src.load.delta_lake_manager import DeltaLakeManager
import pandas as pd
import re
from datetime import datetime
import logging

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)


class BDSDataTransformer:
    """Transform dữ liệu BĐS qua các layer"""
    
    def __init__(self, base_path="data"):
        self.delta_manager = DeltaLakeManager(base_path=base_path)
    
    # ========== RAW → STAGING ==========
    
    def transform_raw_to_staging(self):
        """
        Transform dữ liệu từ Raw sang Staging
        - Làm sạch dữ liệu
        - Chuẩn hóa định dạng
        - Loại bỏ duplicate
        """
        logging.info("="*80)
        logging.info("TRANSFORM: RAW → STAGING")
        logging.info("="*80)
        
        # 1. Đọc dữ liệu Raw
        df = self.delta_manager.read_raw_data()
        logging.info(f"Read {len(df)} records from Raw layer")
        
        # 2. Làm sạch dữ liệu
        df_clean = self._clean_data(df)
        
        # 3. Chuẩn hóa giá, diện tích
        df_clean = self._normalize_price_area(df_clean)
        
        # 4. Loại bỏ duplicate
        df_clean = df_clean.drop_duplicates(subset=["Link tin"], keep="last")
        
        # 5. Lưu vào Staging
        self.delta_manager.save_staging_data(df_clean, mode="overwrite")
        
        logging.info(f"✓ Transformed {len(df_clean)} records to Staging layer")
        return df_clean
    
    def _clean_data(self, df):
        """Làm sạch dữ liệu"""
        df_clean = df.copy()
        
        # Loại bỏ các row không có Link tin
        df_clean = df_clean[df_clean["Link tin"].notna()]
        
        # Loại bỏ khoảng trắng thừa
        for col in df_clean.columns:
            if df_clean[col].dtype == 'object':
                df_clean[col] = df_clean[col].str.strip()
        
        logging.info("✓ Data cleaned")
        return df_clean
    
    def _normalize_price_area(self, df):
        """Chuẩn hóa giá và diện tích thành số"""
        df_norm = df.copy()
        
        # Giá (price_text → price_vnd)
        if "price_text" in df_norm.columns:
            df_norm["price_vnd"] = df_norm["price_text"].apply(self._parse_price)
        
        # Diện tích (area_text → area_m2)
        if "area_text" in df_norm.columns:
            df_norm["area_m2"] = df_norm["area_text"].apply(self._parse_area)
        
        # Giá/m² (price_per_m2_text → price_per_m2_vnd)
        if "price_per_m2_text" in df_norm.columns:
            df_norm["price_per_m2_vnd"] = df_norm["price_per_m2_text"].apply(self._parse_price)
        
        logging.info("✓ Price and area normalized")
        return df_norm
    
    @staticmethod
    def _parse_price(price_str):
        """Parse giá từ text → VND"""
        if pd.isna(price_str):
            return None
        
        try:
            price_str = str(price_str).lower().replace(",", ".").strip()
            
            # Tìm số
            number_match = re.search(r'([\d.]+)', price_str)
            if not number_match:
                return None
            
            number = float(number_match.group(1))
            
            # Xác định đơn vị
            if 'tỷ' in price_str or 'ty' in price_str:
                return number * 1_000_000_000  # tỷ
            elif 'triệu' in price_str or 'trieu' in price_str:
                return number * 1_000_000  # triệu
            else:
                return number
        except:
            return None
    
    @staticmethod
    def _parse_area(area_str):
        """Parse diện tích từ text → m²"""
        if pd.isna(area_str):
            return None
        
        try:
            area_str = str(area_str).lower().replace(",", ".").strip()
            number_match = re.search(r'([\d.]+)', area_str)
            
            if number_match:
                return float(number_match.group(1))
        except:
            return None
        
        return None
    
    # ========== STAGING → WAREHOUSE ==========
    
    def transform_staging_to_warehouse(self):
        """
        Transform dữ liệu từ Staging sang Warehouse
        - Tạo Star Schema (Fact + Dimensions)
        """
        logging.info("="*80)
        logging.info("TRANSFORM: STAGING → WAREHOUSE")
        logging.info("="*80)
        
        # 1. Đọc dữ liệu Staging
        df = self.delta_manager.read_staging_data()
        logging.info(f"Read {len(df)} records from Staging layer")
        
        # 2. Tạo Dimension tables
        dim_location = self._create_dim_location(df)
        dim_property_type = self._create_dim_property_type(df)
        dim_time = self._create_dim_time(df)
        
        # 3. Tạo Fact table
        fact_listings = self._create_fact_listings(df, dim_location, dim_property_type, dim_time)
        
        # 4. Lưu vào Warehouse
        self.delta_manager.save_dimension_table(dim_location, "dim_location")
        self.delta_manager.save_dimension_table(dim_property_type, "dim_property_type")
        self.delta_manager.save_dimension_table(dim_time, "dim_time")
        self.delta_manager.save_fact_listings(fact_listings)
        
        logging.info(f"✓ Transformed to Warehouse (Star Schema)")
        logging.info(f"  - Fact Listings: {len(fact_listings)} records")
        logging.info(f"  - Dim Location: {len(dim_location)} records")
        logging.info(f"  - Dim Property Type: {len(dim_property_type)} records")
        logging.info(f"  - Dim Time: {len(dim_time)} records")
        
        return fact_listings
    
    def _create_dim_location(self, df):
        """Tạo Dimension: Location"""
        if "location" not in df.columns:
            return pd.DataFrame({"location_id": [1], "location": ["Unknown"]})
        
        locations = df["location"].dropna().unique()
        dim_location = pd.DataFrame({
            "location_id": range(1, len(locations) + 1),
            "location": locations
        })
        
        return dim_location
    
    def _create_dim_property_type(self, df):
        """Tạo Dimension: Property Type"""
        if "Loại tin" not in df.columns:
            return pd.DataFrame({"property_type_id": [1], "property_type": ["Unknown"]})
        
        property_types = df["Loại tin"].dropna().unique()
        dim_property_type = pd.DataFrame({
            "property_type_id": range(1, len(property_types) + 1),
            "property_type": property_types
        })
        
        return dim_property_type
    
    def _create_dim_time(self, df):
        """Tạo Dimension: Time"""
        if "Ngày đăng" not in df.columns:
            return pd.DataFrame({"time_id": [1], "date": [datetime.now().strftime("%Y-%m-%d")]})
        
        dates = df["Ngày đăng"].dropna().unique()
        dim_time = pd.DataFrame({
            "time_id": range(1, len(dates) + 1),
            "date": dates
        })
        
        return dim_time
    
    def _create_fact_listings(self, df, dim_location, dim_property_type, dim_time):
        """Tạo Fact table: Listings"""
        fact = df.copy()
        
        # Map với Dimension tables để lấy foreign keys
        location_map = dict(zip(dim_location["location"], dim_location["location_id"]))
        property_type_map = dict(zip(dim_property_type["property_type"], dim_property_type["property_type_id"]))
        time_map = dict(zip(dim_time["date"], dim_time["time_id"]))
        
        fact["location_id"] = fact["location"].map(location_map)
        fact["property_type_id"] = fact["Loại tin"].map(property_type_map)
        fact["time_id"] = fact["Ngày đăng"].map(time_map)
        
        # Thêm status column (mặc định: active)
        fact["status"] = "active"
        
        return fact
    
    def run_full_pipeline(self):
        """Chạy toàn bộ pipeline: Raw → Staging → Warehouse"""
        logging.info("="*80)
        logging.info("FULL ETL PIPELINE: RAW → STAGING → WAREHOUSE")
        logging.info("="*80)
        
        try:
            # Step 1: Deduplicate Raw
            self.delta_manager.deduplicate_raw_data()
            
            # Step 2: Raw → Staging
            self.transform_raw_to_staging()
            
            # Step 3: Staging → Warehouse
            self.transform_staging_to_warehouse()
            
            # Step 4: Update expired listings
            self.delta_manager.update_expired_listings()
            
            logging.info("="*80)
            logging.info("✓ FULL PIPELINE COMPLETED")
            logging.info("="*80)
            
        finally:
            self.delta_manager.stop_spark()


# ========== STANDALONE FUNCTIONS ==========

def run_etl_pipeline(base_path="data"):
    """
    Chạy ETL pipeline đầy đủ
    
    Usage:
        from src.transform.etl_pipeline import run_etl_pipeline
        run_etl_pipeline()
    """
    transformer = BDSDataTransformer(base_path=base_path)
    transformer.run_full_pipeline()


if __name__ == "__main__":
    run_etl_pipeline()
