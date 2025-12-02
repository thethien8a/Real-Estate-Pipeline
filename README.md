# KIẾN TRÚC DỰ ÁN
<img width="4460" height="1624" alt="ETL PIPELINE" src="https://github.com/user-attachments/assets/d611021c-bd0e-4a6f-b379-663ab2e40055" />

# Dashboard 1
<img width="1324" height="742" alt="image" src="https://github.com/user-attachments/assets/cc6a5a94-7c0a-48a7-b2b0-eb08f9b169e8" />

# Dashboard 2
<img width="1198" height="672" alt="image" src="https://github.com/user-attachments/assets/71b5a659-b32e-4030-97c4-b62cdc179740" />

# CÁCH CHẠY PIPELINE Ở LOCAL

## 1. Tạo file .env ở thư mục gốc với nội dung như sau:
```env
SUPABASE_URL=<token đã cung cấp ở nhóm rồi>
SUPABASE_KEY=<token đã cung cấp ở nhóm rồi>
```

## 2. Khởi tạo môi trường venv
```bat
python -m venv venv
```

## 3. Kích hoạt môi trường venv
```bat
venv\Scripts\activate
```

## 4. Cài đặt các dependencies
```bat
pip install -r requirements/requirements.txt
```

## 5. File cấu hình tốc độ cào dữ liệu: nằm ở thư mục src/extract/config.py
### Một vài vấn đề cần quan tâm ở file này như sau:
1. `SUBPAGE_SEMAPHORE_LIMIT`: giới hạn số lượng subpage được xử lý đồng thời cho mỗi main page  
VD: `SUBPAGE_SEMAPHORE_LIMIT = 10` tức là sẽ có tối đa 10 subpage được xử lý đồng thời cho mỗi main page (LƯU Ý TỒN TÀI NGUYÊN MÁY TÍNH)
2. `START_PAGE` và `END_PAGE`: trang bắt đầu và trang kết thúc thu thập
VD: `START_PAGE = 0` và `END_PAGE = 1` tức là sẽ thu thập từ trang 0 đến trang 1
3. `SUBPAGE_CHUNK_SIZE`: số lượng subpage xuất ra file csv sau mỗi lần chạy
VD: `SUBPAGE_CHUNK_SIZE = 200` tức là sẽ xuất ra file csv sau mỗi lần 200 subpage được xử lý. Nghĩa là ví dụ có 1000 url thì xử lý xong từ url 0 đến url 200 sẽ xuất ra file csv, từ url 201 đến url 400 sẽ xuất ra file csv, và cứ thế tiếp tục đến hết

## 6. File cần quan tâm khi chạy pipeline
- `src/extract/crawl.py`: chạy crawl data từ website
- `src/load/load_staging.py`: chạy load data vào Supabase
- `src/transform/main.py`: chạy transform data từ Bronze sang Silver

## 7. 1 vài sửa đổi khi chạy ở local:
- ở src/extract/crawl.py  xóa t dòng: browser_executable_path="/opt/hostedtoolcache/setup-chrome/chrome/stable/x64/chrome"
- 
## 7. Cách chạy pipeline
```bat
B1: python src/extract/crawl.py
B2: python src/load/load_staging.py
B3: python src/transform/main.py
```

## 8. Xong, nếu gặp vấn đề gì báo tao
