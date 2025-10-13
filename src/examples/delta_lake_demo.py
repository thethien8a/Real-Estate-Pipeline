"""
Script demo: Sử dụng Delta Lake trong dự án BDS
Hướng dẫn các use case phổ biến
"""

from src.load.delta_lake_manager import DeltaLakeManager
from src.transform.etl_pipeline import BDSDataTransformer
import pandas as pd
from datetime import datetime

def demo_basic_operations():
    """Demo các thao tác cơ bản với Delta Lake"""
    print("="*80)
    print("DEMO: BASIC DELTA LAKE OPERATIONS")
    print("="*80)
    
    manager = DeltaLakeManager(base_path="data")
    
    try:
        # 1. Lưu dữ liệu mẫu
        print("\n1. Saving sample data to Raw layer...")
        sample_data = pd.DataFrame({
            "Link tin": ["https://example.com/1", "https://example.com/2"],
            "title": ["Nhà đẹp quận 1", "Căn hộ Hà Nội"],
            "price_text": ["5 tỷ", "3.5 tỷ"],
            "area_text": ["100 m²", "80 m²"],
            "location": ["Quận 1, TP.HCM", "Cầu Giấy, Hà Nội"],
            "Thời gian crawl": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * 2,
            "Trang": [1, 1]
        })
        
        manager.save_raw_data(sample_data)
        print("✓ Saved to Raw layer")
        
        # 2. Đọc dữ liệu
        print("\n2. Reading data from Raw layer...")
        df_read = manager.read_raw_data()
        print(f"✓ Read {len(df_read)} records")
        print(df_read.head())
        
        # 3. Xem lịch sử version
        print("\n3. Checking table history...")
        history = manager.get_table_history("raw")
        print(f"✓ Found {len(history)} versions")
        print(history[["version", "timestamp", "operation", "operationMetrics"]])
        
        # 4. Time Travel - đọc version cũ
        print("\n4. Time Travel: Reading version 0...")
        df_v0 = manager.read_raw_data(version=0)
        print(f"✓ Read version 0: {len(df_v0)} records")
        
        # 5. Deduplicate
        print("\n5. Deduplicating data...")
        removed = manager.deduplicate_raw_data()
        print(f"✓ Removed {removed} duplicates")
        
    finally:
        manager.stop_spark()
    
    print("\n" + "="*80)
    print("✓ DEMO COMPLETED")
    print("="*80)


def demo_etl_pipeline():
    """Demo ETL Pipeline: Raw → Staging → Warehouse"""
    print("="*80)
    print("DEMO: FULL ETL PIPELINE")
    print("="*80)
    
    transformer = BDSDataTransformer(base_path="data")
    
    try:
        # Chạy full pipeline
        transformer.run_full_pipeline()
        
        # Đọc kết quả từ Warehouse
        print("\n" + "="*80)
        print("WAREHOUSE RESULTS")
        print("="*80)
        
        fact_listings = transformer.delta_manager.read_warehouse_table("fact_listings")
        print(f"\n✓ Fact Listings: {len(fact_listings)} records")
        print(fact_listings.head())
        
        dim_location = transformer.delta_manager.read_warehouse_table("dim_location")
        print(f"\n✓ Dim Location: {len(dim_location)} records")
        print(dim_location)
        
    finally:
        transformer.delta_manager.stop_spark()


def demo_upsert():
    """Demo Upsert (Update + Insert)"""
    print("="*80)
    print("DEMO: UPSERT OPERATION")
    print("="*80)
    
    manager = DeltaLakeManager(base_path="data")
    
    try:
        # 1. Đọc dữ liệu hiện tại
        print("\n1. Current data in warehouse...")
        fact_listings = manager.read_warehouse_table("fact_listings")
        print(f"Current records: {len(fact_listings)}")
        
        # 2. Tạo dữ liệu mới (có cả update và insert)
        print("\n2. Preparing new data (update + insert)...")
        
        # Lấy 1 record cũ để update
        if len(fact_listings) > 0:
            old_link = fact_listings.iloc[0]["Link tin"]
            
            new_data = pd.DataFrame({
                "Link tin": [old_link, "https://example.com/new"],
                "title": ["UPDATED: Nhà đẹp", "NEW: Căn hộ mới"],
                "price_vnd": [6_000_000_000, 4_500_000_000],
                "status": ["active", "active"]
            })
            
            # 3. Upsert
            print("\n3. Performing upsert...")
            manager.upsert_listings(new_data, merge_key="Link tin")
            print("✓ Upsert completed")
            
            # 4. Kiểm tra kết quả
            print("\n4. Checking results...")
            fact_listings_after = manager.read_warehouse_table("fact_listings")
            print(f"Records after upsert: {len(fact_listings_after)}")
        
    finally:
        manager.stop_spark()
    
    print("\n" + "="*80)
    print("✓ DEMO COMPLETED")
    print("="*80)


def demo_time_travel():
    """Demo Time Travel - truy cập dữ liệu cũ"""
    print("="*80)
    print("DEMO: TIME TRAVEL")
    print("="*80)
    
    manager = DeltaLakeManager(base_path="data")
    
    try:
        # 1. Xem lịch sử
        print("\n1. Table history:")
        history = manager.get_table_history("raw")
        
        if history is not None and len(history) > 0:
            print(history[["version", "timestamp", "operation"]])
            
            # 2. Đọc từng version
            print("\n2. Reading different versions:")
            for version in range(min(3, len(history))):  # Đọc tối đa 3 version
                print(f"\n   Version {version}:")
                df = manager.read_raw_data(version=version)
                print(f"   - Records: {len(df)}")
            
            # 3. Đọc theo timestamp
            if len(history) > 0:
                timestamp = history.iloc[0]["timestamp"]
                print(f"\n3. Reading by timestamp: {timestamp}")
                df_time = manager.read_raw_data(timestamp=str(timestamp))
                print(f"   - Records: {len(df_time)}")
        else:
            print("No history available yet. Run crawler first.")
        
    finally:
        manager.stop_spark()
    
    print("\n" + "="*80)
    print("✓ DEMO COMPLETED")
    print("="*80)


if __name__ == "__main__":
    print("\n" + "="*80)
    print("BDS DELTA LAKE DEMO SCRIPTS")
    print("="*80)
    print("\nAvailable demos:")
    print("1. Basic Operations (demo_basic_operations)")
    print("2. ETL Pipeline (demo_etl_pipeline)")
    print("3. Upsert (demo_upsert)")
    print("4. Time Travel (demo_time_travel)")
    print("\nRun specific demo:")
    print("  python -c 'from src.examples.delta_lake_demo import demo_basic_operations; demo_basic_operations()'")
    print("\n" + "="*80)
