# 📁 Thư mục Load - Hướng Dẫn Sử Dụng

## 🎯 Tổng Quan
Thư mục `load` chứa các công cụ để **tải dữ liệu bất động sản từ file CSV lên Supabase database**. Đây là bước đầu tiên trong pipeline xử lý dữ liệu bất động sản.

## 📋 Công Dụng Từng File

### 🔧 `load.py` (228 dòng) - Bộ Não Chính
**Chức năng chính:**
- 🔍 **Tự động phát hiện file mới**: Quét thư mục `data/raw/` tìm file CSV chưa xử lý
- 📥 **Đọc và làm sạch dữ liệu**: Chuyển đổi file CSV thành định dạng JSON an toàn
- ☁️ **Upload lên Supabase**: Tải dữ liệu vào bảng `bronze.staging` theo batch
- 📊 **Theo dõi tiến trình**: Ghi log từng file đã xử lý, bao gồm số lượng records và trạng thái
- 📁 **Tổ chức file**: Tự động di chuyển file đã xử lý vào thư mục `processed/` hoặc `error/`

**Cách hoạt động:**
1. So sánh timestamp file với lần xử lý cuối cùng trong database
2. Chỉ xử lý file mới hơn timestamp cuối cùng
3. Đọc file → Làm sạch dữ liệu → Upload → Ghi log → Di chuyển file

### 🔗 `supabase_class.py` (256 dòng) - Kết Nối Database
**Chức năng chính:**
- 🌐 **Quản lý kết nối**: Tạo và cache các client Supabase cho nhiều schema khác nhau
- 📝 **CRUD Operations**: Create, Read, Update, Delete records
- 📦 **Batch Processing**: Upload dữ liệu lớn theo từng batch nhỏ (1000 records/batch)
- 🔍 **Query Linh Hoạt**: Tìm kiếm với nhiều điều kiện, sắp xếp, phân trang
- ✅ **Kiểm tra kết nối**: Test connection trước khi thực hiện thao tác

**Tính năng nâng cao:**
- Hỗ trợ multi-schema (bronze, silver, gold)
- Upsert (Insert or Update)
- Query với điều kiện phức tạp (>, <, LIKE, IN)
- Phân trang tự động

### 📄 `__init__.py` - Đánh Dấu Package
File rỗng để Python nhận diện đây là một package có thể import.

## 🚀 Hướng Dẫn Sử Dụng

### Bước 1: Chuẩn Bị Database Supabase

1. **Tạo file .env** với thông tin Supabase của bạn:
   ```
   SUPABASE_URL=https://your-project-id.supabase.co
   SUPABASE_KEY=sb_secret_your_service_role_key_here
   ```

2. **Vào Supabase SQL Editor, chạy script tạo bảng:**

```sql
-- Tạo schema bronze
CREATE SCHEMA IF NOT EXISTS bronze;

-- Grant permissions cho service role
GRANT ALL ON SCHEMA bronze TO service_role;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA bronze TO service_role;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA bronze TO service_role;

-- Tạo bảng staging cho dữ liệu bất động sản
CREATE TABLE bronze.staging (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  main_page_url TEXT,           
  subpage_url TEXT,             
  title TEXT,                   
  address TEXT,                 
  price TEXT,                   
  area TEXT,                    
  house_direction TEXT,         
  balcony_direction TEXT,       
  facade TEXT,                  
  legal TEXT,                   
  furniture TEXT,               
  number_bedroom TEXT,          
  number_bathroom TEXT,         
  number_floor TEXT,            
  way_in TEXT,                  
  project_name TEXT,            
  project_status TEXT,          
  project_investor TEXT,        
  post_id TEXT,                 
  post_start_time TEXT,         
  post_end_time TEXT,           
  post_type TEXT,               
  source TEXT,                  
  crawled_at TEXT,              
  created_at TIMESTAMP DEFAULT NOW()
);

-- Tạo bảng log processed files
CREATE TABLE bronze.processed_files_log (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    file_path TEXT NOT NULL,
    file_timestamp TIMESTAMP NOT NULL,
    processed_at TIMESTAMP DEFAULT NOW(),
    record_count INTEGER,
    status VARCHAR(20) NOT NULL, -- 'success' hoặc 'failed'
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Grant permissions cho tables
GRANT ALL ON bronze.staging TO service_role;
GRANT ALL ON bronze.processed_files_log TO service_role;
GRANT USAGE ON SCHEMA bronze TO service_role;

```

### Bước 2: Chuẩn Bị Thư Mục Dữ Liệu

Tạo cấu trúc thư mục như sau:
```
data/
├── raw/          # Đặt file CSV thô vào đây
├── processed/    # File đã xử lý thành công (tự động tạo)
└── error/        # File xử lý thất bại (tự động tạo)
```

### Bước 3: Cài Đặt Dependencies

```bash
pip install supabase python-dotenv pandas numpy
```

### Bước 4: Chạy Code

#### Cách 1: Chạy Script Chính (Khuyến Nghị)
```bash
cd src/load
python load.py
```

**Kết quả sẽ hiển thị:**
```
INFO: Đang xử lý file: data_20241105_143022.csv
INFO: Đã xử lý thành công data_20241105_143022.csv với 1500 records
INFO: Kết quả: {'status': 'completed', 'processed': 1, 'failed': 0, 'total': 1}
```

#### Cách 2: Sử dụng trong Code Python

```python
from load import StagingLoader

# Khởi tạo loader
loader = StagingLoader()

# Xử lý tất cả file mới
result = loader.process_latest_files()

print(f"Đã xử lý {result['processed']} file thành công")
print(f"Thất bại: {result['failed']} file")
```

### Bước 5: Kiểm Tra Kết Quả

#### Kiểm tra dữ liệu trong Supabase:
```sql
-- Xem dữ liệu trong staging
SELECT COUNT(*) FROM bronze.staging;

-- Xem log xử lý file
SELECT * FROM bronze.processed_files_log ORDER BY processed_at DESC;
```

#### Kiểm tra thư mục:
```
data/
├── raw/          # File mới sẽ xuất hiện ở đây
├── processed/    # File đã xử lý thành công
│   └── data_20241105_143022.csv
└── error/        # File lỗi (nếu có)
```

## 📊 Cách Hoạt Động Chi Tiết

### 1. Phát Hiện File Mới
- Script quét thư mục `data/raw/`
- Trích xuất timestamp từ tên file (định dạng: `data_YYYYMMDD_HHMMSS.csv`)
- So sánh với timestamp xử lý cuối cùng trong database
- Chỉ xử lý file có timestamp mới hơn

### 2. Xử Lý Dữ Liệu
- Đọc file CSV bằng pandas
- Làm sạch dữ liệu: loại bỏ NaN, infinity, giá trị không hợp lệ
- Chuyển đổi thành định dạng JSON an toàn

### 3. Upload Database
- Chia dữ liệu thành batch nhỏ (1000 records/batch)
- Upload lên bảng `bronze.staging`
- Ghi log kết quả vào `bronze.processed_files_log`

### 4. Tổ Chức File
- Thành công → Chuyển vào `processed/`
- Thất bại → Chuyển vào `error/`

## 🔧 Cấu Hình Nâng Cao

### Thay Đổi Thư Mục Dữ Liệu
```python
# Mặc định: data/raw/ (từ thư mục gốc project)
loader = StagingLoader()

# Tùy chỉnh đường dẫn
loader = StagingLoader(data_dir="/path/to/your/data/directory")
```

### Thay Đổi Batch Size
```python
# Mặc định: 1000 records/batch
success = loader.load_staging(data, batch_size=500)
```

### Kiểm Tra Kết Nối Supabase
```python
from supabase_class import SupabaseManager

supabase = SupabaseManager()
result = supabase.test_connection()
print(result)  # {"success": True, "message": "Kết nối thành công"}
```

## 🚨 Xử Lý Lỗi Thường Gặp

### Lỗi "Supabase URL và Key là bắt buộc"
- Kiểm tra file `.env` có tồn tại không
- Đảm bảo biến `SUPABASE_URL` và `SUPABASE_KEY` được set

### Lỗi "Thư mục data/raw không tồn tại"
- Tạo thư mục `data/raw/` trong thư mục gốc project
- Hoặc tùy chỉnh đường dẫn khi khởi tạo `StagingLoader`

### Lỗi "Không thể đọc file CSV"
- Kiểm tra file CSV có encoding UTF-8 không
- Đảm bảo file không bị corrupt hoặc đang được sử dụng bởi app khác

### Lỗi Upload Database
- Kiểm tra quyền truy cập database
- Đảm bảo bảng `bronze.staging` đã được tạo
- Kiểm tra connection Supabase

## 📝 Lưu Ý Quan Trọng

1. **Service Role Key**: Sử dụng `sb_secret_` key cho production, không dùng anon key
2. **File Naming**: Đặt tên file theo format `data_YYYYMMDD_HHMMSS.csv` để tự động trích xuất timestamp
3. **Data Cleaning**: Script tự động làm sạch NaN và giá trị không hợp lệ
4. **Batch Processing**: Tự động chia nhỏ dữ liệu lớn để tránh timeout
5. **Logging**: Tất cả hoạt động được ghi log chi tiết
6. **Idempotent**: Có thể chạy nhiều lần mà không tạo duplicate data

## 🎯 Ví Dụ Sử Dụng Thực Tế

```bash
# 1. Chuẩn bị file CSV trong data/raw/
# 2. Chạy script
cd src/load
python load.py

# 3. Kiểm tra kết quả
# - Dữ liệu xuất hiện trong bronze.staging
# - File được chuyển vào processed/
# - Log ghi trong processed_files_log
```

Thư mục `load` này là **cầu nối** giữa dữ liệu thô và database, đảm bảo tất cả dữ liệu bất động sản được tải lên một cách an toàn và có thể theo dõi.