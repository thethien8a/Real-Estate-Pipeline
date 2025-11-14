# BDS - Batdongsan.com.vn Real Estate Data Scraper

## 📖 Mô tả dự án

BDS là một dự án ETL (Extract, Transform, Load) pipeline được xây dựng bằng Python để thu thập dữ liệu bất động sản từ website lớn nhất Việt Nam: [batdongsan.com.vn](https://batdongsan.com.vn). Dự án tập trung vào việc scrape dữ liệu listings (căn hộ, nhà đất, dự án) một cách tự động, chống phát hiện bot, và lưu trữ dưới dạng CSV để phân tích thị trường, nghiên cứu đầu tư hoặc business intelligence.

### 🎯 Mục tiêu chính
- Thu thập dữ liệu chi tiết từ hàng nghìn bài đăng bất động sản.
- Hỗ trợ xử lý đồng thời (concurrent) để tăng tốc độ.
- Tránh bị chặn bởi anti-bot mechanisms của website.
- Cung cấp dữ liệu có cấu trúc (structured) với hơn 25 trường thông tin.

### 📊 Dữ liệu được thu thập
Mỗi bài đăng bao gồm:
- **Thông tin cơ bản**: Tiêu đề, địa chỉ, giá, diện tích.
- **Chi tiết**: Hướng nhà, ban công, pháp lý, nội thất, số phòng ngủ/tắm/tầng.
- **Dự án**: Tên dự án, tình trạng, chủ đầu tư.
- **Bài đăng**: Mã tin, ngày đăng/hết hạn, loại tin.
- **Metadata**: URL nguồn, thời gian crawl.

Dữ liệu được lưu vào `data/raw/` với định dạng timestamped CSV (ví dụ: `batdongsan_raw_20251028_045314.csv`).

## 🛠️ Công nghệ sử dụng
- **Ngôn ngữ**: Python 3.12
- **Web Scraping**: nodriver 0.47.0 (thư viện chống phát hiện bot, successor của undetected-chromedriver)
- **Xử lý dữ liệu**: pandas 2.3.3, numpy 2.3.4
- **Async**: asyncio (built-in) cho concurrent processing
- **Database**: Supabase (PostgREST API)
- **Lưu trữ**: CSV files + Supabase staging/silver layers
- **Logging**: Python logging module
- **CI/CD**: GitHub Actions (scheduled automation 4 lần/ngày)
- **Môi trường**: Virtual environment (venv)

## 🚀 Cài đặt

### 1. Clone hoặc tải dự án
```bash
# Giả sử bạn đã tải về thư mục BDS
cd C:\Users\Asus\Downloads\BDS
```

### 2. Tạo virtual environment
```bash
# Tạo venv
python -m venv venv

# Kích hoạt (Windows)
venv\Scripts\activate
```

### 3. Cài đặt dependencies
```bash
pip install -r requirements\requirements.txt
```

## 📊 Trạng thái hiện tại

### Tiến độ ETL Pipeline

| Module | Trạng thái | Chi tiết |
|--------|-----------|---------|
| **Extract** | ✅ Hoàn thành | Scraping dữ liệu từ batdongsan.com.vn hoạt động ổn định với 25+ fields. Được tự động hóa qua GitHub Actions (4 lần/ngày) |
| **Transform** | 🔄 Đang phát triển | Đang xây dựng logic chuyển đổi: dedup, upsert, trích xuất page number từ URL |
| **Load** | 🟡 Đang hoàn thiện | Load sang staging (Supabase) + Silver layer - cần test kỹ lưỡng |
| **Gold Layer** | ⏳ Kế hoạch | Chuẩn bị cho data warehouse layer |

### Dữ liệu mới nhất
- **Dữ liệu cuối cùng**: 07/11/2025 - 05:10 (lần chạy sáng)
- **Số lượng bản ghi**: ~80 listings/lần crawl
- **Thời gian lưu trữ**: CSV timestamped trong `data/processed/`
- **Automation**: Chạy tự động 4 lần/ngày (5h, 11h, 17h, 23h UTC+7)

### Công việc ưu tiên tiếp theo
1. ✅ Hoàn thiện transform logic (dedup + upsert)
2. 🔄 Fix & test load staging module (nodriver compatibility issue)
3. ⏳ Implement load gold layer
4. ⏳ Thêm unit tests + integration tests
5. ⏳ Tối ưu concurrency settings

---

## 📝 Sử dụng

### Chạy scraper chính
```bash
# Di chuyển đến module extract
cd src\extract

# Chạy chương trình
python crawl.py
```

- **Output**: Dữ liệu sẽ được lưu tự động vào `data/raw/` với timestamp.
- **Thời gian**: Tùy thuộc vào range page (mặc định 1-50), khoảng 10-30 phút cho 50 trang.

### Cấu hình tùy chỉnh
Chỉnh sửa `src/extract/config.py`:
- `START_PAGE` và `END_PAGE`: Phạm vi trang cần crawl (ví dụ: 1-100).
- `PAGE_SEMAPHORE_LIMIT`: Số main page đồng thời (mặc định 4).
- `SUBPAGE_SEMAPHORE_LIMIT`: Số subpage đồng thời (mặc định 10).

Ví dụ: Để crawl 100 trang đầu:
```python
END_PAGE = 100
```

## 📁 Cấu trúc dự án

```
BDS/
├── data/                  # Lưu trữ dữ liệu
│   ├── raw/              # Dữ liệu thô (CSV từ scraper)
│   ├── staging/          # Dữ liệu đã transform (placeholder)
│   └── warehouse/        # Dữ liệu cuối cùng (placeholder)
├── dags/                 # Airflow DAGs (nếu dùng orchestration, placeholder)
├── documentation/        # Tài liệu
│   ├── config/           # Cấu hình docs
│   ├── guides/           # Hướng dẫn sử dụng
│   └── schemas/          # Schema dữ liệu
├── learning/             # Tài liệu học tập/research
├── logs/                 # Log files (tự động tạo)
├── notebooks/            # Jupyter notebooks cho analysis
│   └── test.ipynb        # Notebook test (ví dụ)
├── requirements/         # Dependencies
│   └── requirements.txt  # Danh sách packages
├── .github/              # GitHub automation
│   └── workflows/
│       └── daily-etl.yml # Scheduled ETL job
├── src/                  # Source code chính
│   ├── __init__.py
│   ├── extract/          # Module extract dữ liệu
│   │   ├── __init__.py
│   │   ├── config.py     # Cấu hình crawling
│   │   ├── crawl.py      # Logic chính scraper
│   │   ├── utils.py      # Utility functions
│   │   └── README.md     # Docs cho extract module
│   ├── load/             # Module load dữ liệu
│   │   ├── __init__.py
│   │   ├── load_staging.py   # Load sang staging layer
│   │   ├── load_silver.py    # Load sang silver layer
│   │   ├── load_gold.py      # Load sang gold layer (WIP)
│   │   ├── supabase_class.py # Supabase integration
│   │   └── README.md         # Docs cho load module
│   ├── transform/        # Module transform dữ liệu
│   │   └── documentation/    # Transform logic documentation
├── tests/                # Tests toàn dự án (placeholder)
├── venv/                 # Virtual environment
├── LICENSE               # MIT License
└── README.md             # Tài liệu này
```

## ✨ Tính năng nổi bật

- **Concurrent Scraping**: Xử lý đồng thời 4 main pages + 10 subpages/page để tăng tốc.
- **Anti-Detection**: Sử dụng nodriver với stealth options (navigator manipulation, random delays).
- **Error Resilience**: Retry mechanism (reload page lên đến 3 lần), exception handling cho ProtocolException.
- **Structured Output**: Dữ liệu CSV với schema rõ ràng, dễ import vào Excel/Pandas.
- **Configurable**: Dễ dàng thay đổi range, concurrency qua config.py.
- **Logging**: Theo dõi tiến trình và lỗi chi tiết.


## 🤝 Contributing

1. Fork dự án.
2. Tạo branch: `git checkout -b feature/new-feature`.
3. Commit changes: `git commit -m 'Add new feature'`.
4. Push: `git push origin feature/new-feature`.
5. Tạo Pull Request.

### Thêm tính năng gợi ý
- Implement transform/load modules.
- Thêm database storage (PostgreSQL/SQLite).
- Tích hợp Airflow cho scheduling.
- Unit tests với pytest.

## 📄 License

Dự án được phân phối dưới [MIT License](LICENSE). Copyright (c) 2025 Nguyễn Thế Thiện.

## 📞 Liên hệ

- **Tác giả**: Nguyễn Thế Thiện, Nguyễn Hồng Nhung
- **Email**: [thethien04.work@gmail.com]
- **Issues**: Tạo issue trên GitHub nếu có bug hoặc feature request.

Cảm ơn bạn đã sử dụng BDS! Nếu cần hỗ trợ, hãy comment hoặc liên hệ. 🚀
