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
- Nếu có chữ "Thỏa thuận" → giá trị `None`
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
-Chia làm các loại giá trị: 1;2;3;4;5; 6-10; >10
12. **Cột way_in**
- Chia làm các loại: 0-10m; 11-20m; 21-30m; 31-40m; 41-50m; >50m
13. **Cột project_name**
Chia làm các loại: Kiểm tra theo thứ tự từ trên xuống, match cái nào lấy cái đó:
- "đã bàn giao" → "đã bàn giao"
   (hoặc "bàn giao" + năm ≤ 2024)

- "đang bàn giao" → "đang bàn giao"

- "sắp bàn giao" → "sắp bàn giao"
   (hoặc "bàn giao" + năm ≥ 2025)

- "đang mở bán" → "đang mở bán"
   (hoặc "mở bán đợt")

- "sắp mở bán" → "sắp mở bán"

- "cắt nóc" hoặc "đang xây dựng" → "đang xây dựng"

- "dự kiến" → "dự kiến"

- Còn lại → "đang cập nhật"
14. **Cột project_investor"
Giữ nguyên, đổi sang viết hoa tất cả các chữ đầu tiên cho đồng bộ
dữ liệu k biết thì unknown
15. **Các cột còn lại"
Không cần transform


