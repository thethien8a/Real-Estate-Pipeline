# 🏠 BDS - Real Estate Data Warehouse with Delta Lake

Dự án Data Warehouse chuyên nghiệp cho phân tích thị trường bất động sản, sử dụng **Delta Lake** và kiến trúc Lakehouse hiện đại.

## ⭐ Tính năng nổi bật

- ✅ **Delta Lake Integration**: ACID transactions, Time Travel, Schema Evolution
- 🚀 **Concurrent Web Scraping**: Thu thập dữ liệu song song với ThreadPoolExecutor
- 🏗️ **Star Schema Design**: Fact & Dimension tables tối ưu cho phân tích
- 🔄 **ETL Pipeline**: Raw → Staging → Warehouse với data quality checks
- 📊 **Time Series Analysis**: Phân tích xu hướng giá BĐS theo thời gian
- 🔍 **Data Versioning**: Time Travel để audit và rollback

## 🏗️ Kiến trúc dự án

```
BDS/
├── data/                          # Delta Lake Storage
│   ├── raw/                       # 🗄️ Raw Layer (Delta Tables)
│   │   └── batdongsan/            #    - Dữ liệu thô từ crawler
│   │       ├── _delta_log/        #    - Transaction logs
│   │       └── *.parquet          #    - Parquet data files
│   ├── staging/                   # 🧹 Staging Layer (Delta Tables)
│   │   └── batdongsan_clean/      #    - Dữ liệu đã clean & normalize
│   └── warehouse/                 # 🏛️ Warehouse Layer (Star Schema)
│       ├── fact_listings/         #    - Fact: Listings
│       ├── dim_location/          #    - Dimension: Locations
│       ├── dim_property_type/     #    - Dimension: Property Types
│       └── dim_time/              #    - Dimension: Time
│
├── src/                           # Source Code
│   ├── crawl_da_luong.py          # 🕷️ Concurrent web crawler
│   ├── load/                      # 💾 Delta Lake Manager
│   │   └── delta_lake_manager.py  #    - CRUD operations cho Delta Tables
│   ├── transform/                 # 🔄 ETL Pipeline
│   │   └── etl_pipeline.py        #    - Raw → Staging → Warehouse
│   ├── examples/                  # 📚 Demo & Examples
│   │   └── delta_lake_demo.py     #    - Use cases & tutorials
│   └── utils/                     # 🛠️ Utilities
│
├── documentation/                 # 📖 Documentation
│   ├── config/                    # Configurations
│   ├── schemas/                   # Data schemas
│   └── guides/                    # Technical guides
│
├── notebooks/                     # 📊 Analysis Notebooks
├── dags/                          # 🔄 Airflow DAGs
├── requirements.txt               # 📦 Dependencies
└── README.md                      # This file
```

## 🎯 Kiến trúc Data Lakehouse (3 Layers)

### 1️⃣ Raw Data Layer (`data/raw/`)
**Delta Table**: Dữ liệu thô từ web crawler
- ✅ **ACID Transactions**: Đảm bảo tính toàn vẹn khi crawler chạy song song
- 📸 **Time Travel**: Truy cập snapshot dữ liệu theo ngày
- 🔄 **Append-only**: Giữ nguyên lịch sử thu thập
- 📊 **Parquet Format**: Compressed columnar storage

**Dữ liệu**: Listings từ batdongsan.com.vn (title, price, area, location...)

### 2️⃣ Staging Layer (`data/staging/`)
**Delta Table**: Dữ liệu đã clean & normalize
- 🧹 **Data Cleaning**: Loại bỏ duplicate, null values
- 🔢 **Normalization**: Parse giá (tỷ/triệu → VND), diện tích (text → m²)
- ✂️ **Deduplication**: Loại bỏ duplicate theo `Link tin`
- ✅ **Schema Enforcement**: Đảm bảo data quality

**Transform**: Raw → Staging (làm sạch + chuẩn hóa)

### 3️⃣ Warehouse Layer (`data/warehouse/`)
**Star Schema**: Fact & Dimension tables
- 🌟 **Fact Table**: `fact_listings` (measures: price, area, etc.)
- 📍 **Dim Location**: Địa điểm (Quận/Huyện/TP)
- 🏘️ **Dim Property Type**: Loại BĐS (Nhà, Căn hộ, Đất...)
- 📅 **Dim Time**: Thời gian đăng tin
- 🔄 **Upsert Support**: Update giá, status khi có thay đổi

**Transform**: Staging → Warehouse (Star Schema modeling)

## 🛠️ Tech Stack

### Core Technologies
- **Storage Format**: Delta Lake 3.0+ (ACID, Time Travel)
- **Processing Engine**: Apache Spark 3.5+ (PySpark)
- **Data Format**: Parquet (Columnar, Compressed)
- **Web Scraping**: Selenium + BeautifulSoup + Requests
- **Concurrency**: Python ThreadPoolExecutor

### Data Pipeline
- **ETL Framework**: Custom Python + Apache Airflow (planned)
- **Data Modeling**: Star Schema (Kimball methodology)
- **Language**: Python 3.9+
- **Libraries**: Pandas, NumPy, Delta-Spark

### Infrastructure
- **Version Control**: Git + GitHub
- **CI/CD**: GitHub Actions
- **Documentation**: Markdown + Jupyter Notebooks

## 🚀 Quick Start

### 1. Cài đặt môi trường

```bash
# Clone repository
git clone https://github.com/thethien8a/Real-Estate.git
cd BDS

# Tạo virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate.bat

# Activate (Linux/Mac)
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Chạy Web Crawler (Thu thập dữ liệu)

```bash
# Chạy crawler với Delta Lake
python src/crawl_da_luong.py
```

**Cấu hình trong file** (`src/crawl_da_luong.py`):
```python
START_PAGE = 1          # Trang bắt đầu
END_PAGE = 10           # Trang kết thúc
CRAWL_DETAILS = True    # Crawl chi tiết tin
USE_DELTA_LAKE = True   # Lưu vào Delta Lake
MAX_WORKERS = 4         # Số thread song song
```

**Output**:
- ✅ CSV backup: `batdongsan_YYYYMMDD_HHMMSS.csv`
- ✅ Delta Lake: `data/raw/batdongsan/`

### 3. Chạy ETL Pipeline (Transform dữ liệu)

```bash
# Full pipeline: Raw → Staging → Warehouse
python src/transform/etl_pipeline.py
```

**Pipeline thực hiện**:
1. Deduplicate Raw data
2. Transform Raw → Staging (clean + normalize)
3. Transform Staging → Warehouse (Star Schema)
4. Update expired listings

### 4. Demo & Examples

```bash
# Basic operations
python -c "from src.examples.delta_lake_demo import demo_basic_operations; demo_basic_operations()"

# ETL Pipeline demo
python -c "from src.examples.delta_lake_demo import demo_etl_pipeline; demo_etl_pipeline()"

# Upsert demo
python -c "from src.examples.delta_lake_demo import demo_upsert; demo_upsert()"

# Time Travel demo
python -c "from src.examples.delta_lake_demo import demo_time_travel; demo_time_travel()"
```

## � Use Cases & Code Examples

### 🔹 1. Lưu dữ liệu vào Delta Lake

```python
from src.load.delta_lake_manager import quick_save_raw
import pandas as pd

# Sau khi crawler xong
df = pd.DataFrame(data)
quick_save_raw(df)  # Tự động lưu vào data/raw/batdongsan/
```

### 🔹 2. Đọc dữ liệu từ Delta Lake

```python
from src.load.delta_lake_manager import quick_read_raw

# Đọc version mới nhất
df = quick_read_raw()

# Time Travel: Đọc version cũ
df_v1 = quick_read_raw(version=1)

# Đọc theo timestamp
df_yesterday = quick_read_raw(timestamp="2025-10-12 14:30:00")
```

### 🔹 3. Chạy ETL Pipeline

```python
from src.transform.etl_pipeline import run_etl_pipeline

# Full pipeline: Raw → Staging → Warehouse
run_etl_pipeline()
```

### 🔹 4. Update/Upsert dữ liệu

```python
from src.load.delta_lake_manager import DeltaLakeManager

manager = DeltaLakeManager()

# Update expired listings
manager.update_expired_listings()

# Upsert new data
new_df = pd.DataFrame({...})
manager.upsert_listings(new_df, merge_key="Link tin")

manager.stop_spark()
```

### � 5. Xem lịch sử thay đổi

```python
from src.load.delta_lake_manager import DeltaLakeManager

manager = DeltaLakeManager()
history = manager.get_table_history("raw")
print(history[["version", "timestamp", "operation"]])
manager.stop_spark()
```

## 📋 Data Flow

```
┌─────────────────┐
│  Web Scraping   │  batdongsan.com.vn
│  (Concurrent)   │  → ThreadPoolExecutor (4 workers)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   RAW LAYER     │  Delta Table
│  (Append only)  │  → ACID, Time Travel
└────────┬────────┘
         │  deduplicate
         ▼
┌─────────────────┐
│ STAGING LAYER   │  Delta Table  
│ (Clean + Norm)  │  → Parse price/area, remove nulls
└────────┬────────┘
         │  star schema modeling
         ▼
┌─────────────────┐
│ WAREHOUSE LAYER │  Delta Tables (Star Schema)
│  (Analytics)    │  → Fact + Dimensions
└─────────────────┘
         │
         ▼
    📊 BI Tools / Analysis
```

## 🎯 Tính năng Delta Lake trong dự án

| Tính năng | Use Case | Lợi ích |
|-----------|----------|---------|
| **ACID Transactions** | Crawler chạy song song | Không bị corrupt data |
| **Time Travel** | So sánh giá theo ngày | `df = read_raw_data(version=1)` |
| **Schema Evolution** | Website thay đổi cấu trúc | Tự động adapt schema mới |
| **Upsert** | Update giá BĐS | `upsert_listings(new_df)` |
| **Deduplication** | Loại bỏ tin trùng | `deduplicate_raw_data()` |
| **Compaction** | Tối ưu storage | `optimize_table("raw")` |
| **Vacuum** | Xóa file cũ | `vacuum_table(retention=168h)` |

## 📚 Documentation

- [Delta Lake Architecture](./documentation/guides/delta-lake.md)
- [ETL Pipeline Design](./documentation/guides/etl-pipeline.md)
- [Data Schema](./documentation/schemas/)
- [API Reference](./documentation/guides/api.md)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📧 Contact

- **Author**: thethien8a
- **Repository**: [Real-Estate](https://github.com/thethien8a/Real-Estate)
- **Issues**: [GitHub Issues](https://github.com/thethien8a/Real-Estate/issues)

## 🤝 Đóng góp

1. Fork dự án
2. Tạo feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Tạo Pull Request

## 📄 License

Distributed under the MIT License. See `LICENSE` for more information.
