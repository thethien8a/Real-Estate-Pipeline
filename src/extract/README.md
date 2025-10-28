# Batdongsan.com.vn Web Scraper

Chương trình cào dữ liệu bất động sản từ website batdongsan.com.vn sử dụng thư viện nodriver (successor của undetected-chromedriver) để tránh bị phát hiện.

## 📁 Cấu trúc thư mục extract/

### `__init__.py`
- **Chức năng**: Đánh dấu thư mục `extract/` là một Python package
- **Nội dung**: File trống, chỉ để Python nhận diện package

### `config.py`
- **Chức năng**: Chứa các cấu hình cho quá trình crawling
- **Các thành phần chính**:
  - `CrawlConfig` class: Định nghĩa các hằng số cấu hình
    - `PAGE_SEMAPHORE_LIMIT = 4`: Giới hạn 4 main page xử lý đồng thời
    - `SUBPAGE_SEMAPHORE_LIMIT = 10`: Giới hạn 10 subpage xử lý đồng thời cho mỗi main page
    - `START_PAGE = 1`: Trang bắt đầu thu thập
    - `END_PAGE = 50`: Trang kết thúc thu thập
  - `get_page_semaphore()`: Tạo semaphore cho main page
  - `get_subpage_semaphore()`: Tạo semaphore cho subpage

### `utils.py`
- **Chức năng**: Chứa các utility functions hỗ trợ quá trình extract dữ liệu
- **Các function chính**:

#### Text Extraction Functions:
- `text_from_element(element)`: Extract text từ DOM element
- `text_from_selector(page, selector)`: Extract text từ CSS selector

#### Data Extraction Functions:
- `extract_value_from_specs(page, label)`: Extract thông tin từ phần specs (giá, diện tích, hướng nhà, v.v.)
- `extract_value_from_project_card(page, icon_class)`: Extract thông tin từ project card (tình trạng dự án, chủ đầu tư)
- `extract_value_from_post_card(page, label)`: Extract thông tin từ post card (mã tin, ngày đăng, loại tin)

#### Data Storage Functions:
- `save_results_to_csv(results)`: Lưu kết quả crawling vào file CSV với timestamp

#### Error Handling Functions:
- `reload_page(page, reload_times=3)`: Reload trang khi gặp lỗi, thử lại tối đa 3 lần

### `crawl.py`
- **Chức năng**: File chính chứa logic crawling và orchestration
- **Các function chính**:

#### URL Extraction:
- `extract_subpage_urls(page)`: Extract danh sách URL các bài đăng từ trang danh sách

#### Data Extraction:
- `extract_data_from_page(page)`: Extract toàn bộ thông tin từ một trang chi tiết bài đăng

#### Scraping Functions:
- `scrape_subpage(url, subpage_semaphore, browser)`: Xử lý một trang chi tiết bài đăng
- `scrape_main_page(url, page_semaphore, subpage_semaphore)`: Xử lý một trang danh sách và tất cả subpage của nó

#### Main Function:
- `main()`: Hàm chính điều phối toàn bộ quá trình crawling

## 🚀 Cách sử dụng

### 1. Cài đặt dependencies
```bash
pip install -r requirements.txt
```

### 2. Chạy chương trình
```bash
cd src/extract
python crawl.py
```

### 3. Cấu hình (tùy chọn)
Chỉnh sửa file `config.py` để thay đổi:
- Số lượng page xử lý đồng thời
- Phạm vi trang cần crawl (START_PAGE, END_PAGE)
- Các thông số khác

## 📊 Dữ liệu được thu thập

Chương trình sẽ thu thập các thông tin sau từ mỗi bài đăng:

### Thông tin cơ bản:
- `title`: Tiêu đề bài đăng
- `address`: Địa chỉ
- `price`: Giá bán
- `area`: Diện tích

### Thông tin chi tiết:
- `house_direction`: Hướng nhà
- `balcony_direction`: Hướng ban công
- `facade`: Mặt tiền
- `legal`: Pháp lý
- `furniture`: Nội thất
- `number_bedroom`: Số phòng ngủ
- `number_bathroom`: Số phòng tắm
- `number_floor`: Số tầng
- `way_in`: Đường vào

### Thông tin dự án:
- `project_name`: Tên dự án
- `project_status`: Tình trạng dự án
- `project_investor`: Chủ đầu tư

### Thông tin bài đăng:
- `post_id`: Mã tin
- `post_start_time`: Ngày đăng
- `post_end_time`: Ngày hết hạn
- `post_type`: Loại tin

### Metadata:
- `source`: Nguồn (batdongsan.com.vn)
- `url`: URL của bài đăng
- `crawled_at`: Thời gian thu thập dữ liệu

## 💾 Định dạng output

Dữ liệu được lưu vào file CSV trong thư mục `data/raw/` với định dạng:
```
batdongsan_raw_YYYYMMDD_HHMMSS.csv
```

## ⚙️ Cơ chế hoạt động

1. **Parallel Processing**: Sử dụng asyncio và semaphore để xử lý đồng thời nhiều trang
2. **Error Handling**: Retry mechanism khi gặp lỗi, reload page khi cần thiết
3. **Anti-Detection**: Sử dụng nodriver với các tùy chọn chống phát hiện bot
4. **Data Persistence**: Tự động lưu dữ liệu với timestamp

## 🛠️ Tính năng nâng cao

- **Configurable**: Dễ dàng thay đổi cấu hình qua file config.py
- **Scalable**: Hỗ trợ xử lý hàng nghìn trang đồng thời
- **Robust**: Xử lý lỗi tự động, retry khi thất bại
- **Stealth**: Tránh bị phát hiện bởi anti-bot systems
- **Structured**: Dữ liệu được tổ chức rõ ràng theo schema

## 🔧 Troubleshooting

### Lỗi thường gặp:
1. **"Tab" has no attribute "wait_for"**: nodriver không có method này, đã được fix
2. **ProtocolException**: Xử lý tự động bằng reload mechanism
3. **Connection timeout**: Tăng timeout hoặc giảm số lượng concurrent requests

### Debug mode:
Thay đổi logging level trong code để xem chi tiết hơn:
```python
logging.basicConfig(level=logging.DEBUG)
```
