# Tài liệu chuyển đổi dữ liệu

## 1. Chuyển đổi dữ liệu từ staging sang silver

### Xử lý chung:
- Loại bỏ hoàn toàn các dòng trùng lặp trong dữ liệu (với subcolumns là các cột trừ metadata hệ thống tự tạo)
- Áp dụng cơ chế upsert từ staging sang silver để đảm bảo dữ liệu không bị trùng lặp 

### Các bước xử lý chi tiết:

1. **Cột main_page_url:**
   - thì ta sẽ lấy ra 1 biến là "trang" từ url này  (dựa vào cấu trúc https://batdongsan.com.vn/nha-dat-ban/p1 -> trang 1, https://batdongsan.com.vn/nha-dat-ban/p2 -> trang 2, ...)

2. **Cột address:**

   Cột này sẽ tách ra thành 4 cột: Khu vực cụ thể | Thôn/Tổ dân phố | Quận/Huyện | Phường/Xã | Tỉnh/Thành phố

   **TRONG MỌI TRƯỜNG HỢP (đơn giản nhất là chỉ có 2 dấu phẩy):**
   + Chữ sau phẩy cuối cùng sẽ luôn là Tỉnh/Thành phố (Hà Nội, Hồ Chí Minh, ...)
   + Chữ sau phẩy trước cuối cùng sẽ luôn là Quận/Huyện (Quận 1, Quận 2, Đông Anh, ...)
   + Chữ sau phẩy trước trước cuối cùng sẽ luôn là Phường/Xã (Lỗ Khê, Liên Hà, ...)

   **XÉT RIÊNG:**  
   a. Với trường hợp có 4 dấu phẩy ở trong address: (VD: "Nhà An Khê, Lỗ Khê, Liên Hà, Đông Anh, Hà Nội") thì:
      + Chữ sau phẩy đầu tiên sẽ là Thôn/Tổ dân phố
      + Chữ đầu tiên sẽ là: Khu vực cụ thể

   b. Với trường hợp có 3 dấu phẩy:
      TH1: Với trường hợp ví dụ: Đường Nguyễn Văn Linh, Liên Hà, Đông Anh, Hà Nội thì do chữ đầu tiên "Đường Nguyễn Văn Linh" chứa chữ "Đường" nên từ đầu tiên vào "Thôn/Tổ dân phố"
      TH2: Với trường hợp: Vinhomes Golden City, Hòa Nghĩa, Dương Kinh, Hà Nội thì không chứa "Đường" nên ta sẽ xét cái chữ đầu tiên vào "Khu vực cụ thể"

   c. Với các trường hợp khác ở trên (hoặc xử lý bị lỗi): ta đưa dữ liệu đó vào bảng lỗi "error_table"

    Với các cột sau khi được tách ra, ta cần xử lý sạch sẽ cho đúng định dạng: Strip whitespace và title chúng

3. **Cột price:**
    Quan sát: price có các trường hợp như: Thỏa thuận, ... triệu/m^2, tỷ, triệu (Ở thời điểm hiện tại)

    **XỬ LÝ:** Ta sẽ tạo 1 cột có tên là "million_per_m2" để lưu giá triệu đồng/m^2
    + Nếu price có chữ "Thỏa thuận" thì ta sẽ để giá trị là None
    + Nếu price có chữ "tỷ": ta lấy giá trị số đó nhân với 1000 để được giá trị triệu đồng. Rồi sau đó chia cho diện tích để được giá trị triệu đồng/m^2
    + Nếu price có chữ "triệu": ta lấy giá trị số đó chia cho diện tích để được giá trị triệu đồng/m^2
    + Thay các "," thành "." để được giá trị số thập phân
    
    **LƯU Ý:** Với các trường hợp không phải ở trên (hoặc xử lý bị lỗi), ta sẽ cho giá trị đó vào bảng lỗi "error_table"

4. **Cột area:**
    Quan sát: area có chỉ m^2 (ít nhất ở thời điểm hiện tại)

    **XỬ LÝ:** Ta sẽ tạo 1 cột có tên là "area_m2" để lưu giá trị diện tích m^2
    + Thay các "." thành rỗng để được giá trị số nguyên 
    + Thay các "," thành "." để được giá trị số thập phân
    + Sau đó loại bỏ các m^2 đi

    **LƯU Ý:** Với các trường hợp không phải ở trên (hoặc xử lý bị lỗi), ta sẽ cho giá trị đó vào bảng lỗi "error_table"

5. **Cột house_direction:** và **Cột balcony_direction:**: Chỉ cần strip whitespace và title chúng

6. **Cột facade:**:
    + Đổi "m" thành rỗng
    + Strip whitespace
    + Đổi "," thành "."

    **LƯU Ý:** Với các trường hợp không phải ở trên (hoặc xử lý bị lỗi), ta sẽ cho giá trị đó vào bảng lỗi "error_table"

7. **Cột legal:**:
    - Từ cột này tạo các cột có tên là: 
        + have_red_book: nếu có số đỏ thì 1 còn không thì là 0
        + have_pink_book: nếu có số hồng thì 1 còn không thì là 0
        + have_sale_contract: nếu có hợp đồng mua bán thì 1 còn không thì là 0

    **LƯU Ý:** Với các trường hợp không phải ở trên (hoặc xử lý bị lỗi), ta sẽ cho giá trị đó vào bảng lỗi "error_table"

8. **Cột furniture:**: Sẽ được tạo thành 2 cột:
    - have_full_furniture: nếu có tất cả nội thất thì 1 còn không thì là 0
    - furniture_type: với các giá trị như: basic, premium
9. **Cột number_bedroom**
- Xóa "phòng"
- Chia làm các loại giá trị: 1;2;3;4;5;  6-10; >10
10. **Cột number_bathroom**
-Xóa "phòng"
-Chia làm các loại giá trị: 1;2;3;4;5; 6-10; >10
11. **Cột number_floor**
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
14. **Cột project_invenhật**
Giữ nguyên, đổi sang viết hoa tất cả các chữ đầu tiên cho đồng bộ
dữ liệu k biết thì unknown
15. **Các cột còn lại**
Không cần transform


