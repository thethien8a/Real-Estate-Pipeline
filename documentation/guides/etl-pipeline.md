# ETL Pipeline Design

## Overview

ETL Pipeline cho dự án BDS sử dụng kiến trúc 3-layer với Delta Lake:

```
Raw Layer → Staging Layer → Warehouse Layer
```

## Pipeline Stages

### Stage 1: Data Collection (Extract)

**Tool**: `src/crawl_da_luong.py`

**Process**:
1. Concurrent web scraping (ThreadPoolExecutor, 4 workers)
2. Parse HTML với BeautifulSoup
3. Fetch details với Requests (parallel)
4. Save to Delta Lake Raw layer

**Output**: `data/raw/batdongsan/` (Delta Table)

**Features**:
- Concurrent crawling
- Auto-retry on failure
- Human-like delays
- Delta Lake ACID transactions

### Stage 2: Data Cleaning (Transform - Staging)

**Tool**: `src/transform/etl_pipeline.py` → `transform_raw_to_staging()`

**Process**:
1. Read from Raw layer
2. Clean data:
   - Remove nulls in critical fields
   - Strip whitespaces
   - Remove duplicates by "Link tin"
3. Normalize data:
   - Parse price text → VND (number)
   - Parse area text → m² (number)
   - Parse price/m² → VND/m²
4. Save to Staging layer

**Input**: `data/raw/batdongsan/`
**Output**: `data/staging/batdongsan_clean/`

**Transform Rules**:

```python
# Price parsing
"5 tỷ" → 5,000,000,000
"3.5 tỷ" → 3,500,000,000
"800 triệu" → 800,000,000

# Area parsing
"100 m²" → 100.0
"80m2" → 80.0
```

### Stage 3: Data Modeling (Transform - Warehouse)

**Tool**: `src/transform/etl_pipeline.py` → `transform_staging_to_warehouse()`

**Process**:
1. Read from Staging layer
2. Create Dimension tables:
   - `dim_location`: Extract unique locations
   - `dim_property_type`: Extract unique property types
   - `dim_time`: Extract unique dates
3. Create Fact table:
   - `fact_listings`: All listings with foreign keys to dimensions
4. Save to Warehouse layer

**Input**: `data/staging/batdongsan_clean/`
**Output**: 
- `data/warehouse/fact_listings/`
- `data/warehouse/dim_location/`
- `data/warehouse/dim_property_type/`
- `data/warehouse/dim_time/`

**Star Schema**:

```
       dim_location
            |
            |
fact_listings ------- dim_property_type
            |
            |
        dim_time
```

### Stage 4: Data Maintenance

**Process**:
1. Update expired listings
2. Deduplicate
3. Optimize tables
4. Vacuum old files

## Running the Pipeline

### Full Pipeline

```bash
python src/transform/etl_pipeline.py
```

### Step-by-step

```python
from src.transform.etl_pipeline import BDSDataTransformer

transformer = BDSDataTransformer()

try:
    # Step 1: Deduplicate raw
    transformer.delta_manager.deduplicate_raw_data()
    
    # Step 2: Raw → Staging
    transformer.transform_raw_to_staging()
    
    # Step 3: Staging → Warehouse
    transformer.transform_staging_to_warehouse()
    
    # Step 4: Maintenance
    transformer.delta_manager.update_expired_listings()
    
finally:
    transformer.delta_manager.stop_spark()
```

## Data Quality Checks

### Check 1: Null Values

```python
df = manager.read_staging_data()

# Check critical fields
critical_fields = ["Link tin", "title", "location"]
for field in critical_fields:
    null_count = df[field].isna().sum()
    print(f"{field}: {null_count} nulls")
```

### Check 2: Duplicates

```python
df = manager.read_raw_data()
duplicates = df.duplicated(subset=["Link tin"]).sum()
print(f"Duplicates: {duplicates}")

# Auto-remove
manager.deduplicate_raw_data()
```

### Check 3: Price/Area Parsing

```python
df = manager.read_staging_data()

# Check parsing success rate
total = len(df)
price_parsed = df["price_vnd"].notna().sum()
area_parsed = df["area_m2"].notna().sum()

print(f"Price parsing: {price_parsed/total*100:.1f}%")
print(f"Area parsing: {area_parsed/total*100:.1f}%")
```

## Scheduling (Future)

### Airflow DAG

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'bds-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bds_etl_pipeline',
    default_args=default_args,
    description='BDS ETL Pipeline',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
)

def run_crawler():
    from src.crawl_da_luong import main
    main()

def run_etl():
    from src.transform.etl_pipeline import run_etl_pipeline
    run_etl_pipeline()

task_crawl = PythonOperator(
    task_id='crawl_data',
    python_callable=run_crawler,
    dag=dag,
)

task_etl = PythonOperator(
    task_id='etl_transform',
    python_callable=run_etl,
    dag=dag,
)

task_crawl >> task_etl
```

## Monitoring

### Metrics to Track

1. **Data Volume**: Số records mỗi layer
2. **Data Quality**: % null, % parsed successfully
3. **Pipeline Duration**: Thời gian chạy mỗi stage
4. **Error Rate**: Số lỗi trong crawling/transform

### Logging

```python
import logging

logging.basicConfig(
    filename='logs/etl_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
```

## Optimization Tips

1. **Partition Data**: Partition by date nếu data lớn
2. **Parallel Processing**: Sử dụng Spark parallelism
3. **Incremental Load**: Chỉ process dữ liệu mới
4. **Caching**: Cache intermediate results

## Error Handling

```python
try:
    transformer.run_full_pipeline()
except Exception as e:
    logging.error(f"Pipeline failed: {e}")
    # Send alert (email/Slack)
    # Rollback if needed
    raise
```
