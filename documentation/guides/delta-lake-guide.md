# Delta Lake Integration Guide

## Tổng quan

Delta Lake đã được tích hợp vào dự án BDS để cung cấp:
- **ACID Transactions**: Đảm bảo tính toàn vẹn dữ liệu
- **Time Travel**: Truy cập lịch sử dữ liệu theo version/timestamp
- **Schema Evolution**: Tự động adapt với thay đổi schema
- **Upsert Support**: Update/Insert dữ liệu dễ dàng

## Kiến trúc 3 Layers

### Layer 1: Raw (`data/raw/batdongsan/`)
- **Mục đích**: Lưu trữ dữ liệu thô từ crawler
- **Format**: Delta Table (Parquet + Transaction Log)
- **Write Mode**: Append only (giữ lịch sử)
- **Operations**: Save, Read, Deduplicate, Time Travel

### Layer 2: Staging (`data/staging/batdongsan_clean/`)
- **Mục đích**: Dữ liệu đã clean và normalize
- **Transform**: Parse price/area, remove nulls, deduplicate
- **Write Mode**: Overwrite (latest clean version)
- **Operations**: Save, Read

### Layer 3: Warehouse (`data/warehouse/`)
- **Mục đích**: Star Schema cho analytics
- **Tables**:
  - `fact_listings`: Fact table chính
  - `dim_location`: Dimension địa điểm
  - `dim_property_type`: Dimension loại BĐS
  - `dim_time`: Dimension thời gian
- **Operations**: Save, Read, Upsert, Update

## API Reference

### DeltaLakeManager Class

```python
from src.load.delta_lake_manager import DeltaLakeManager

manager = DeltaLakeManager(base_path="data")
```

#### Raw Layer Operations

**1. Save Raw Data**
```python
manager.save_raw_data(df_pandas, mode="append")
```

**2. Read Raw Data**
```python
# Latest version
df = manager.read_raw_data()

# Specific version (Time Travel)
df_v1 = manager.read_raw_data(version=1)

# By timestamp
df = manager.read_raw_data(timestamp="2025-10-12 14:30:00")
```

**3. Deduplicate**
```python
removed = manager.deduplicate_raw_data(unique_columns=["Link tin"])
```

#### Staging Layer Operations

**1. Save Staging Data**
```python
manager.save_staging_data(df_pandas, mode="overwrite")
```

**2. Read Staging Data**
```python
df = manager.read_staging_data()
```

#### Warehouse Layer Operations

**1. Save Fact/Dimension Tables**
```python
# Fact table
manager.save_fact_listings(df_fact)

# Dimension tables
manager.save_dimension_table(df_location, "dim_location")
manager.save_dimension_table(df_property_type, "dim_property_type")
manager.save_dimension_table(df_time, "dim_time")
```

**2. Read Warehouse Tables**
```python
df_fact = manager.read_warehouse_table("fact_listings")
df_location = manager.read_warehouse_table("dim_location")
```

**3. Update/Upsert**
```python
# Update expired listings
manager.update_expired_listings()

# Upsert (merge)
manager.upsert_listings(new_df, merge_key="Link tin")
```

#### Utility Operations

**1. Table History**
```python
history = manager.get_table_history("raw")
print(history[["version", "timestamp", "operation"]])
```

**2. Optimize (Compaction)**
```python
manager.optimize_table("raw")
```

**3. Vacuum (Cleanup old files)**
```python
manager.vacuum_table("raw", retention_hours=168)  # 7 days
```

**4. Stop Spark**
```python
manager.stop_spark()  # Always call when done
```

### Quick Helper Functions

```python
from src.load.delta_lake_manager import quick_save_raw, quick_read_raw

# Quick save
quick_save_raw(df)

# Quick read
df = quick_read_raw()
df_v1 = quick_read_raw(version=1)
```

## ETL Pipeline

### Full Pipeline

```python
from src.transform.etl_pipeline import run_etl_pipeline

# Chạy toàn bộ: Raw → Staging → Warehouse
run_etl_pipeline()
```

### Custom Pipeline

```python
from src.transform.etl_pipeline import BDSDataTransformer

transformer = BDSDataTransformer(base_path="data")

try:
    # Step 1: Raw → Staging
    df_staging = transformer.transform_raw_to_staging()
    
    # Step 2: Staging → Warehouse
    df_warehouse = transformer.transform_staging_to_warehouse()
    
finally:
    transformer.delta_manager.stop_spark()
```

## Best Practices

### 1. Always Stop Spark
```python
manager = DeltaLakeManager()
try:
    # Your operations
    df = manager.read_raw_data()
finally:
    manager.stop_spark()  # IMPORTANT!
```

### 2. Use Time Travel for Auditing
```python
# So sánh giá hôm nay vs hôm qua
df_today = manager.read_raw_data()
df_yesterday = manager.read_raw_data(timestamp="2025-10-12")

# Phân tích sự thay đổi
df_changes = pd.merge(df_today, df_yesterday, on="Link tin", suffixes=("_today", "_yesterday"))
```

### 3. Regular Optimization
```python
# Nên chạy định kỳ (weekly)
manager.optimize_table("raw")
manager.optimize_table("staging")
manager.optimize_table("fact_listings")

# Cleanup old files (monthly)
manager.vacuum_table("raw", retention_hours=168*4)  # 4 weeks
```

### 4. Upsert Instead of Overwrite
```python
# ❌ BAD: Mất dữ liệu cũ
manager.save_fact_listings(new_df, mode="overwrite")

# ✅ GOOD: Merge với dữ liệu cũ
manager.upsert_listings(new_df, merge_key="Link tin")
```

## Troubleshooting

### Issue 1: Java not found
```bash
# Install Java 8 or 11
# Windows: Download from Oracle
# Linux: sudo apt-get install openjdk-11-jdk
```

### Issue 2: Memory error
```python
# Tăng memory cho Spark
builder = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g")
```

### Issue 3: Slow queries
```python
# Run optimization
manager.optimize_table("raw")

# Increase parallelism
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

## Migration from CSV

Nếu đã có dữ liệu CSV cũ:

```python
import pandas as pd
from src.load.delta_lake_manager import quick_save_raw

# Đọc CSV cũ
df = pd.read_csv("old_data.csv")

# Migrate sang Delta Lake
quick_save_raw(df)

print("✓ Migrated to Delta Lake")
```

## Performance Tips

1. **Partition by date**: Cân nhắc partition table theo ngày nếu data lớn
2. **Z-Ordering**: Optimize cho queries thường dùng
3. **Compact files**: Chạy optimize() thường xuyên
4. **Vacuum old versions**: Giải phóng storage

## Next Steps

- [ ] Implement Airflow DAG cho auto ETL
- [ ] Add data quality checks
- [ ] Setup monitoring & alerting
- [ ] Integrate with BI tools (PowerBI/Tableau)
