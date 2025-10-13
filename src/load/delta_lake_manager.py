"""
Delta Lake Manager for BDS Data Warehouse
Quản lý tất cả các thao tác với Delta Lake
"""

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
import pandas as pd
from datetime import datetime
import logging
from pathlib import Path

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)

class DeltaLakeManager:
    """Quản lý Delta Lake operations cho BDS project"""
    
    def __init__(self, base_path="data"):
        """
        Initialize Delta Lake Manager
        
        Args:
            base_path (str): Đường dẫn gốc chứa dữ liệu
        """
        self.base_path = Path(base_path)
        self.spark = None
        
        # Định nghĩa các đường dẫn Delta Tables
        self.tables = {
            "raw": str(self.base_path / "raw" / "batdongsan"),
            "staging": str(self.base_path / "staging" / "batdongsan_clean"),
            "warehouse": {
                "fact_listings": str(self.base_path / "warehouse" / "fact_listings"),
                "dim_location": str(self.base_path / "warehouse" / "dim_location"),
                "dim_property_type": str(self.base_path / "warehouse" / "dim_property_type"),
                "dim_time": str(self.base_path / "warehouse" / "dim_time"),
            }
        }
    
    def get_spark_session(self):
        """Khởi tạo hoặc lấy Spark session với Delta Lake"""
        if self.spark is None:
            builder = SparkSession.builder \
                .appName("BDS-DeltaLake") \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            
            self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
            logging.info("✓ Spark session with Delta Lake initialized")
        
        return self.spark
    
    def stop_spark(self):
        """Dừng Spark session"""
        if self.spark:
            self.spark.stop()
            self.spark = None
            logging.info("✓ Spark session stopped")
    
    # ========== RAW LAYER OPERATIONS ==========
    
    def save_raw_data(self, df_pandas, mode="append"):
        """
        Lưu dữ liệu thô từ crawler vào Raw Delta Table
        
        Args:
            df_pandas (pd.DataFrame): DataFrame từ crawler
            mode (str): "append" hoặc "overwrite"
        """
        spark = self.get_spark_session()
        table_path = self.tables["raw"]
        
        # Tạo thư mục nếu chưa tồn tại
        Path(table_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Convert Pandas → Spark DataFrame
        df_spark = spark.createDataFrame(df_pandas)
        
        # Ghi vào Delta Lake
        df_spark.write \
            .format("delta") \
            .mode(mode) \
            .save(table_path)
        
        logging.info(f"✓ Saved {len(df_pandas)} records to Raw layer ({mode} mode)")
        return table_path
    
    def read_raw_data(self, version=None, timestamp=None):
        """
        Đọc dữ liệu từ Raw Delta Table
        
        Args:
            version (int): Version cụ thể (Time Travel)
            timestamp (str): Timestamp (format: 'YYYY-MM-DD HH:MM:SS')
        
        Returns:
            pd.DataFrame: Dữ liệu raw
        """
        spark = self.get_spark_session()
        table_path = self.tables["raw"]
        
        reader = spark.read.format("delta")
        
        # Time Travel
        if version is not None:
            reader = reader.option("versionAsOf", version)
        elif timestamp:
            reader = reader.option("timestampAsOf", timestamp)
        
        df_spark = reader.load(table_path)
        df_pandas = df_spark.toPandas()
        
        logging.info(f"✓ Read {len(df_pandas)} records from Raw layer")
        return df_pandas
    
    def deduplicate_raw_data(self, unique_columns=["Link tin"]):
        """
        Loại bỏ duplicate trong Raw layer
        
        Args:
            unique_columns (list): Các cột dùng để xác định duplicate
        """
        spark = self.get_spark_session()
        table_path = self.tables["raw"]
        
        # Đọc dữ liệu
        df = spark.read.format("delta").load(table_path)
        count_before = df.count()
        
        # Deduplicate (giữ record mới nhất theo "Thời gian crawl")
        df_dedup = df.orderBy("Thời gian crawl", ascending=False) \
                     .dropDuplicates(unique_columns)
        
        count_after = df_dedup.count()
        
        # Overwrite table
        df_dedup.write \
            .format("delta") \
            .mode("overwrite") \
            .save(table_path)
        
        removed = count_before - count_after
        logging.info(f"✓ Deduplicated Raw layer: {removed} duplicates removed ({count_after} records remain)")
        return removed
    
    # ========== STAGING LAYER OPERATIONS ==========
    
    def save_staging_data(self, df_pandas, mode="overwrite"):
        """
        Lưu dữ liệu đã clean vào Staging Delta Table
        
        Args:
            df_pandas (pd.DataFrame): DataFrame đã được transform
            mode (str): "append" hoặc "overwrite"
        """
        spark = self.get_spark_session()
        table_path = self.tables["staging"]
        
        # Tạo thư mục nếu chưa tồn tại
        Path(table_path).parent.mkdir(parents=True, exist_ok=True)
        
        df_spark = spark.createDataFrame(df_pandas)
        
        df_spark.write \
            .format("delta") \
            .mode(mode) \
            .save(table_path)
        
        logging.info(f"✓ Saved {len(df_pandas)} records to Staging layer ({mode} mode)")
        return table_path
    
    def read_staging_data(self):
        """Đọc dữ liệu từ Staging layer"""
        spark = self.get_spark_session()
        df_spark = spark.read.format("delta").load(self.tables["staging"])
        df_pandas = df_spark.toPandas()
        logging.info(f"✓ Read {len(df_pandas)} records from Staging layer")
        return df_pandas
    
    # ========== WAREHOUSE LAYER OPERATIONS ==========
    
    def save_fact_listings(self, df_pandas, mode="overwrite"):
        """Lưu Fact table (fact_listings) vào Warehouse"""
        spark = self.get_spark_session()
        table_path = self.tables["warehouse"]["fact_listings"]
        
        Path(table_path).parent.mkdir(parents=True, exist_ok=True)
        
        df_spark = spark.createDataFrame(df_pandas)
        df_spark.write.format("delta").mode(mode).save(table_path)
        
        logging.info(f"✓ Saved {len(df_pandas)} records to Fact Listings")
        return table_path
    
    def save_dimension_table(self, df_pandas, dimension_name, mode="overwrite"):
        """
        Lưu Dimension table vào Warehouse
        
        Args:
            df_pandas (pd.DataFrame): DataFrame dimension
            dimension_name (str): "dim_location", "dim_property_type", "dim_time"
            mode (str): write mode
        """
        spark = self.get_spark_session()
        table_path = self.tables["warehouse"][dimension_name]
        
        Path(table_path).parent.mkdir(parents=True, exist_ok=True)
        
        df_spark = spark.createDataFrame(df_pandas)
        df_spark.write.format("delta").mode(mode).save(table_path)
        
        logging.info(f"✓ Saved dimension table: {dimension_name}")
        return table_path
    
    def read_warehouse_table(self, table_name):
        """
        Đọc bảng từ Warehouse layer
        
        Args:
            table_name (str): "fact_listings", "dim_location", etc.
        """
        spark = self.get_spark_session()
        table_path = self.tables["warehouse"][table_name]
        
        df_spark = spark.read.format("delta").load(table_path)
        df_pandas = df_spark.toPandas()
        
        logging.info(f"✓ Read {len(df_pandas)} records from {table_name}")
        return df_pandas
    
    # ========== UPDATE/DELETE OPERATIONS ==========
    
    def update_expired_listings(self):
        """Update status cho các tin đã hết hạn"""
        spark = self.get_spark_session()
        table_path = self.tables["warehouse"]["fact_listings"]
        
        try:
            deltaTable = DeltaTable.forPath(spark, table_path)
            
            today = datetime.now().strftime('%Y-%m-%d')
            
            deltaTable.update(
                condition = f"`Ngày hết hạn` < '{today}'",
                set = {"status": "'expired'"}
            )
            
            logging.info(f"✓ Updated expired listings")
        except Exception as e:
            logging.warning(f"Update expired listings failed: {e}")
    
    def upsert_listings(self, df_pandas, merge_key="Link tin"):
        """
        Upsert (Merge) dữ liệu mới vào Fact table
        
        Args:
            df_pandas (pd.DataFrame): Dữ liệu mới cần merge
            merge_key (str): Key để merge (thường là "Link tin" hoặc "Mã tin")
        """
        spark = self.get_spark_session()
        table_path = self.tables["warehouse"]["fact_listings"]
        
        # Convert new data to Spark DataFrame
        df_new = spark.createDataFrame(df_pandas)
        df_new.createOrReplaceTempView("new_data")
        
        try:
            deltaTable = DeltaTable.forPath(spark, table_path)
            
            # Merge (Upsert)
            deltaTable.alias("target").merge(
                df_new.alias("source"),
                f"target.`{merge_key}` = source.`{merge_key}`"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            
            logging.info(f"✓ Upserted {len(df_pandas)} records into Fact Listings")
        except Exception as e:
            logging.error(f"Upsert failed: {e}")
    
    # ========== UTILITY OPERATIONS ==========
    
    def get_table_history(self, layer="raw"):
        """
        Xem lịch sử version của Delta Table
        
        Args:
            layer (str): "raw", "staging", hoặc warehouse table name
        """
        spark = self.get_spark_session()
        
        if layer in ["raw", "staging"]:
            table_path = self.tables[layer]
        else:
            table_path = self.tables["warehouse"].get(layer)
        
        if not table_path:
            logging.error(f"Invalid layer: {layer}")
            return None
        
        try:
            deltaTable = DeltaTable.forPath(spark, table_path)
            history = deltaTable.history().toPandas()
            logging.info(f"✓ Retrieved history for {layer} ({len(history)} versions)")
            return history
        except Exception as e:
            logging.error(f"Get history failed: {e}")
            return None
    
    def vacuum_table(self, layer="raw", retention_hours=168):
        """
        Xóa các file cũ không cần thiết (retention: 7 days = 168 hours)
        
        Args:
            layer (str): "raw", "staging", hoặc warehouse table name
            retention_hours (int): Số giờ giữ lại file cũ (mặc định 7 ngày)
        """
        spark = self.get_spark_session()
        
        if layer in ["raw", "staging"]:
            table_path = self.tables[layer]
        else:
            table_path = self.tables["warehouse"].get(layer)
        
        try:
            deltaTable = DeltaTable.forPath(spark, table_path)
            deltaTable.vacuum(retentionHours=retention_hours)
            logging.info(f"✓ Vacuumed {layer} (retention: {retention_hours}h)")
        except Exception as e:
            logging.error(f"Vacuum failed: {e}")
    
    def optimize_table(self, layer="raw"):
        """
        Tối ưu hóa Delta Table (compaction)
        
        Args:
            layer (str): "raw", "staging", hoặc warehouse table name
        """
        spark = self.get_spark_session()
        
        if layer in ["raw", "staging"]:
            table_path = self.tables[layer]
        else:
            table_path = self.tables["warehouse"].get(layer)
        
        try:
            deltaTable = DeltaTable.forPath(spark, table_path)
            deltaTable.optimize().executeCompaction()
            logging.info(f"✓ Optimized {layer}")
        except Exception as e:
            logging.error(f"Optimize failed: {e}")


# ========== HELPER FUNCTIONS ==========

def quick_save_raw(df_pandas, base_path="data"):
    """
    Quick function để lưu raw data (cho crawler)
    
    Usage:
        from src.load.delta_lake_manager import quick_save_raw
        quick_save_raw(df)
    """
    manager = DeltaLakeManager(base_path=base_path)
    try:
        manager.save_raw_data(df_pandas)
    finally:
        manager.stop_spark()


def quick_read_raw(base_path="data", version=None):
    """
    Quick function để đọc raw data
    
    Usage:
        from src.load.delta_lake_manager import quick_read_raw
        df = quick_read_raw()
    """
    manager = DeltaLakeManager(base_path=base_path)
    try:
        return manager.read_raw_data(version=version)
    finally:
        manager.stop_spark()
