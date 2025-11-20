# Tài liệu chuyển đổi dữ liệu (Update 18/11/2025)

## 1. Chuyển đổi dữ liệu từ staging sang silver

### Xử lý chung:
- Loại bỏ hoàn toàn các dòng trùng lặp trong dữ liệu (với subcolumns là các cột trừ metadata hệ thống tự tạo)
- Áp dụng cơ chế upsert từ staging sang silver để đảm bảo dữ liệu không bị trùng lặp 
- Với những dữ liệu NULL cột số thì chuyển sang -1, những dữ liệu NULL cột string thì chuyển sang "không xác định"

---

### Các bước xử lý chi tiết:

#### 1. Cột `main_page_url`
- Lấy ra biến **"trang"** từ URL này
- Dựa vào cấu trúc: 
  - `https://batdongsan.com.vn/nha-dat-ban/p1` → trang 1
  - `https://batdongsan.com.vn/nha-dat-ban/p2` → trang 2

---

#### 2. Cột `address`

Cột này sẽ tách ra thành 4 cột: **Khu vực cụ thể | Thôn/Tổ dân phố | Quận/Huyện | Phường/Xã | Tỉnh/Thành phố**

**TRONG MỌI TRƯỜNG HỢP (đơn giản nhất là chỉ có 2 dấu phẩy):**
- Chữ sau phẩy cuối cùng sẽ luôn là **Tỉnh/Thành phố** (Hà Nội, Hồ Chí Minh, ...)
- Chữ sau phẩy trước cuối cùng sẽ luôn là **Quận/Huyện** (Quận 1, Quận 2, Đông Anh, ...)
- Chữ sau phẩy trước trước cuối cùng sẽ luôn là **Phường/Xã** (Lỗ Khê, Liên Hà, ...)

**XÉT RIÊNG:**

**a. Trường hợp có 4 dấu phẩy:**
- VD: "Nhà An Khê, Lỗ Khê, Liên Hà, Đông Anh, Hà Nội"
- Chữ sau phẩy đầu tiên sẽ là **Thôn/Tổ dân phố**
- Chữ đầu tiên sẽ là **Khu vực cụ thể**

**b. Trường hợp có 3 dấu phẩy:**
- **TH1:** "Đường Nguyễn Văn Linh, Liên Hà, Đông Anh, Hà Nội"
  - Do chữ đầu tiên chứa "Đường" → từ đầu tiên vào **Thôn/Tổ dân phố**
- **TH2:** "Vinhomes Golden City, Hòa Nghĩa, Dương Kinh, Hà Nội"
  - Không chứa "Đường" → từ đầu tiên vào **Khu vực cụ thể**

**c. Các trường hợp khác:**
- Đưa dữ liệu vào bảng lỗi `error_table`

**Xử lý sau khi tách:**
- Strip whitespace và title các cột

---

#### 3. Cột `price`

**Quan sát:** price có các trường hợp: Thỏa thuận, ... triệu/m², tỷ, triệu

**XỬ LÝ:** Tạo cột `million_per_m2` để lưu giá triệu đồng/m²
- Nếu có chữ "tỷ" → nhân với 1000 để được triệu đồng, sau đó chia cho diện tích
- Nếu có chữ "triệu" → chia cho diện tích để được triệu đồng/m²
- Thay "," thành "." để được số thập phân

**LƯU Ý:** Các trường hợp không phù hợp hoặc lỗi → đưa vào `error_table`

---

#### 4. Cột `area`

**Quan sát:** area có đơn vị m²

**XỬ LÝ:** Tạo cột `area_m2` để lưu giá trị diện tích m²
- Thay "." thành rỗng để được số nguyên
- Thay "," thành "." để được số thập phân
- Loại bỏ "m²"

**LƯU Ý:** Các trường hợp không phù hợp hoặc lỗi → đưa vào `error_table`

---

#### 5. Cột `house_direction` và `balcony_direction`
- Strip whitespace và title

---

#### 6. Cột `facade`
- Đổi "m" thành rỗng
- Strip whitespace
- Đổi "," thành "."

**LƯU Ý:** Các trường hợp không phù hợp hoặc lỗi → đưa vào `error_table`

---

#### 7. Cột `legal`

Tạo các cột:
- `have_red_book`: có số đỏ → 1, không → 0
- `have_pink_book`: có số hồng → 1, không → 0
- `have_sale_contract`: có hợp đồng mua bán → 1, không → 0

**LƯU Ý:** Các trường hợp không phù hợp hoặc lỗi → đưa vào `error_table`

---

#### 8. Cột `furniture`

Tạo 2 cột:
- `have_full_furniture`: có tất cả nội thất → 1, không → 0
- `furniture_type`: các giá trị như basic, premium

---

#### 9. Cột `number_bedroom`
- Xóa "phòng"
- Chuyển thành số nguyên

**LƯU Ý:** Các trường hợp không phù hợp hoặc lỗi → đưa vào `error_table`

---

#### 10. Cột `number_bathroom`
- Xóa "phòng"
- Chuyển thành số nguyên

**LƯU Ý:** Các trường hợp không phù hợp hoặc lỗi → đưa vào `error_table`

---

#### 11. Cột `number_floor`
- Xóa "tầng"
- Chuyển thành số nguyên

**LƯU Ý:** Các trường hợp không phù hợp hoặc lỗi → đưa vào `error_table`

---

#### 12. Cột `way_in`
- Xóa dấu nháy kép (")
- Xóa chữ "m"
- Đổi tên cột thành `way_in_m`

**LƯU Ý:** Các trường hợp không phù hợp hoặc lỗi → đưa vào `error_table`

---

#### 13. Cột `project_name`

Kiểm tra theo thứ tự từ trên xuống, match cái nào lấy cái đó:
<!-- 
Của Nhung
| Điều kiện | Kết quả |
|-----------|---------|
| "đã bàn giao" hoặc ("bàn giao" + năm ≤ 2024) | "đã bàn giao" |
| "đang bàn giao" | "đang bàn giao" |
| "sắp bàn giao" hoặc ("bàn giao" + năm ≥ 2025) | "sắp bàn giao" |
| "đang mở bán" hoặc "mở bán đợt" | "đang mở bán" |
| "sắp mở bán" | "sắp mở bán" |
| "cắt nóc" hoặc "đang xây dựng" | "đang xây dựng" |
| "dự kiến" | "dự kiến" |
| NULL | "không xác định | -->

Của Thiện
+ Với các TH dài dài khó xác định thì ta sẽ cho vào `error_table` để xly thủ công sau
+ NULL -> "không xác định 

**LƯU Ý:** Các trường hợp không phù hợp hoặc lỗi → đưa vào `error_table`

---

#### 14. Cột `project_investor`
- Giữ nguyên, đổi sang viết hoa tất cả các chữ đầu tiên cho đồng bộ
- Dữ liệu không biết → "không xác định"

---

#### 15. Cột `subpage_url`

**Các pattern URL:**
- `https://batdongsan.com.vn/ban-nha-biet-thu-...`
- `https://batdongsan.com.vn/ban-can-ho-chung-cu-...`
- `https://batdongsan.com.vn/ban-nha-rieng-...`
- `https://batdongsan.com.vn/ban-dat-xa-...`
- `https://batdongsan.com.vn/ban-dat-duong-...`
- `https://batdongsan.com.vn/ban-dat-nen-...`
- `https://batdongsan.com.vn/ban-condotel-...`
- `https://batdongsan.com.vn/ban-shophouse-...`
- `https://batdongsan.com.vn/ban-loai-bat-dong-san-khac-...`
- `https://batdongsan.com.vn/ban-nha-mat-pho-...`
- `https://batdongsan.com.vn/ban-dat-phuong-...`
- `https://batdongsan.com.vn/ban-trang-trai-...`

**Xử lý:**
- Tạo cột mới: **loại bất động sản**
- Các giá trị: Nhà biệt thự, Căn hộ chung cư, Nhà riêng, Đất xã, Đất đường, Đất nền, Condotel, Shophouse, Bất động sản khác, Nhà mặt phố, Đất phường, Trang trại

**LƯU Ý:** URL không phù hợp (Không chứa các pattern trên) → đưa vào `error_table`

---

## 2. Cấu trúc bảng clean_table trong Silver Layer

Dựa trên tài liệu chuyển đổi dữ liệu và cấu trúc staging table, bảng `clean_table` trong silver layer sẽ có các cột sau:

### Cột gốc từ staging (đã được làm sạch):
1. `main_page_url` - URL trang chính
2. `subpage_url` - URL trang chi tiết
3. `title` - Tiêu đề tin đăng
4. `price` - Giá gốc (đã làm sạch)
5. `area` - Diện tích gốc (đã làm sạch)
6. `house_direction` - Hướng nhà (đã chuẩn hóa)
7. `balcony_direction` - Hướng ban công (đã chuẩn hóa)
8. `facade` - Mặt tiền (đã làm sạch)
9. `legal` - Pháp lý (đã làm sạch)
10. `furniture` - Nội thất (đã làm sạch)
11. `number_bedroom` - Số phòng ngủ (đã chuyển thành số)
12. `number_bathroom` - Số phòng tắm (đã chuyển thành số)
13. `number_floor` - Số tầng (đã chuyển thành số)
14. `way_in_m` - Lối vào (đã đổi tên và làm sạch)
15. `project_name` - Tên dự án (đã làm sạch)
16. `project_status` - Trạng thái dự án (đã làm sạch)
17. `project_investor` - Chủ đầu tư (đã làm sạch)
18. `post_id` - ID tin đăng
19. `post_start_time` - Thời gian bắt đầu đăng
20. `post_end_time` - Thời gian kết thúc đăng
21. `post_type` - Loại tin đăng
22. `source` - Nguồn tin
23. `crawled_at` - Thời gian crawl dữ liệu

### Cột mới được tạo ra từ quá trình transform:
24. `page_number` - Số trang (tách từ main_page_url)
25. `khu_vuc_cụ_the` - Khu vực cụ thể (tách từ address)
26. `thon_to_dan_pho` - Thôn/Tổ dân phố (tách từ address)
27. `quan_huyen` - Quận/Huyện (tách từ address)
28. `phuong_xa` - Phường/Xã (tách từ address)
29. `tinh_thanh_pho` - Tỉnh/Thành phố (tách từ address)
30. `million_per_m2` - Giá triệu đồng/m² (tính từ price và area)
31. `area_m2` - Diện tích m² (đã chuyển đổi từ area)
32. `have_red_book` - Có sổ đỏ (1: có, 0: không)
33. `have_pink_book` - Có sổ hồng (1: có, 0: không)
34. `have_sale_contract` - Có hợp đồng mua bán (1: có, 0: không)
35. `have_full_furniture` - Có đầy đủ nội thất (1: có, 0: không)
36. `furniture_type` - Loại nội thất (basic, premium, etc.)
37. `loai_bat_dong_san` - Loại bất động sản (tách từ subpage_url)

### Cột metadata hệ thống:
38. `created_at` - Thời gian tạo record
39. `updated_at` - Thời gian cập nhật record
40. `is_active` - Trạng thái active (1: active, 0: inactive)

### Tổng cộng: 40 cột

**Lưu ý:**
- Các cột số có giá trị NULL sẽ được chuyển thành -1
- Các cột string có giá trị NULL sẽ được chuyển thành "không xác định"
- Dữ liệu trùng lặp sẽ được loại bỏ hoàn toàn
- Áp dụng cơ chế upsert từ staging sang silver để đảm bảo dữ liệu không bị trùng lặp

---

## 3. Cơ chế xử lý dữ liệu từ staging sang silver

### 3.1. Quy trình xử lý

#### Lần xử lý đầu tiên:
- Khi bảng `silver.clean_table` chưa có dữ liệu nào, ta sẽ xử lý 1 cách thủ công
- Toàn bộ dữ liệu từ staging sẽ được chuyển đổi và tải vào silver

#### Các lần xử lý tiếp theo (tự động):
1. **Lấy ngày cuối cùng xử lý:**
   - Đọc giá trị từ bảng `silver.last_transform_date`
   - Bảng này có duy nhất 1 cột `last_date` lưu ngày cuối cùng trong staging đã được xử lý

2. **Lấy dữ liệu mới:**
   - Sử dụng ngày cuối cùng từ `last_transform_date` để lọc dữ liệu mới từ bảng `bronze.staging`
   - Chỉ lấy những records có `created_at` > ngày cuối cùng đã xử lý

3. **Xử lý và tải dữ liệu:**
   - Transform dữ liệu theo quy tắc đã định nghĩa
   - Tải dữ liệu đã làm sạch vào `silver.clean_table`
   - Lưu dữ liệu lỗi vào `silver.error_table`
   - Cập nhật ngày xử lý mới nhất vào `silver.last_transform_date`

---

## 4. Cấu trúc các bảng bổ trợ trong Silver Layer

### 4.1. Bảng `last_transform_date`

**Mục đích:** Lưu trữ ngày cuối cùng đã xử lý dữ liệu từ staging

**Cấu trúc:**
| Tên cột | Kiểu dữ liệu | Mô tả |
|---------|-------------|-------|
| last_date | TIMESTAMP | Ngày cuối cùng trong staging đã được xử lý |

**Đặc điểm:**
- Bảng này chỉ chứa 1 dòng dữ liệu
- Được cập nhật sau mỗi lần xử lý thành công

### 4.2. Bảng `error_table`

**Mục đích:** Lưu trữ các bản ghi bị lỗi trong quá trình transform

**Cấu trúc:**

**Cột gốc từ staging (23 cột):**
1. `main_page_url` - URL trang chính
2. `subpage_url` - URL trang chi tiết
3. `title` - Tiêu đề tin đăng
4. `address` - Địa chỉ gốc
5. `price` - Giá gốc
6. `area` - Diện tích gốc
7. `house_direction` - Hướng nhà
8. `balcony_direction` - Hướng ban công
9. `facade` - Mặt tiền
10. `legal` - Pháp lý
11. `furniture` - Nội thất
12. `number_bedroom` - Số phòng ngủ
13. `number_bathroom` - Số phòng tắm
14. `number_floor` - Số tầng
15. `way_in` - Lối vào
16. `project_name` - Tên dự án
17. `project_status` - Trạng thái dự án
18. `project_investor` - Chủ đầu tư
19. `post_id` - ID tin đăng
20. `post_start_time` - Thời gian bắt đầu đăng
21. `post_end_time` - Thời gian kết thúc đăng
22. `post_type` - Loại tin đăng
23. `source` - Nguồn tin

**Cột bổ trợ (4 cột):**
24. `crawled_at` - Thời gian crawl dữ liệu
25. `error_cols` - TEXT - Lưu các cột mà dữ liệu bị lỗi (ví dụ: "price,area,facade")
26. `retry_status` - VARCHAR(20) - Trạng thái xử lý lại:
   - `pending`: Chờ xử lý lại (mặc định)
   - `processing`: Đang xử lý lại
   - `resolved`: Đã xử lý thành công
   - `failed`: Xử lý lại thất bại
27. `error_message` - TEXT - Mô tả chi tiết lỗi xảy ra
28. `created_at` - TIMESTAMP - Thời gian record lỗi được tạo

**Tổng cộng: 29 cột**

**Luồng xử lý lỗi:**
1. Khi phát hiện lỗi trong quá trình transform, record sẽ được chuyển vào `error_table`
2. `error_cols` sẽ lưu danh sách các cột bị lỗi
3. `retry_status` mặc định là `pending`
4. Có thể xử lý lại các record lỗi thủ công hoặc tự động
5. Khi xử lý lại thành công, cập nhật `retry_status` thành `resolved`
