# 📊 Quality Check Module — Real Estate Pipeline

Module **Quality Check** được thiết kế để kiểm tra và đánh giá chất lượng dữ liệu bất động sản sau khi crawl, trước khi đưa vào hệ thống phân tích và xử lý.

Thư mục này bao gồm:

* 🧮 Các **hàm làm sạch dữ liệu** trong Supabase (PostgreSQL)
* 👀 Một **view tổng hợp lỗi theo ngày**: `qc_daily_overview`
* 📈 File Python tạo **Dashboard kiểm tra dữ liệu** (HTML): `dash_quality.py`
* 📝 Hướng dẫn chạy môi trường và generate dashboard

---

## 🧩 1. Các hàm làm sạch dữ liệu (Supabase Functions)

Hai hàm dưới đây được tạo trong **Supabase PostgreSQL**. View QC sẽ sử dụng chúng để đánh giá dữ liệu.

---

### 🔹 1.1 `clean_area(raw TEXT)`

**Mục đích:** Chuẩn hóa dữ liệu diện tích (m²), chuyển các chuỗi đầu vào về dạng số (DOUBLE PRECISION).

**Quy trình xử lý:**

* Trả về NULL nếu input rỗng
* Chuyển chuỗi thành lowercase
* Loại bỏ ký tự không phải số (`0-9`, `.`, `,`)
* Đổi dấu `,` → `.`
* Trả về số thực
* Nếu parse thất bại → NULL

**Ví dụ:**

| raw input | output |
| --------- | ------ |
| "47,7 m²" | 47.7   |
| "100m2"   | 100    |
| "---"     | NULL   |

**Mã nguồn:**

```sql
CREATE OR REPLACE FUNCTION clean_area(raw TEXT)
RETURNS DOUBLE PRECISION AS $$
DECLARE
    num TEXT;
BEGIN
    IF raw IS NULL OR raw = '' THEN 
        RETURN NULL;
    END IF;

    raw := lower(raw);
    num := regexp_replace(raw, '[^0-9,\.]', '', 'g');
    num := replace(num, ',', '.');

    RETURN num::double precision;

EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
```

---

### 🔹 1.2 `clean_price(raw TEXT)`

**Mục đích:** Chuẩn hóa giá bất động sản, xử lý cả dạng text như `"3.2 tỷ"`, `"700 triệu"`, `"30 tr"`.

**Quy trình:**

* Bỏ dấu ngăn cách nghìn
* Chỉ giữ ký tự số và `. ,`
* Chuyển `,` → `.`
* Ép về dạng số
* Nhận diện đơn vị:

  * `tỷ` → × 1,000,000,000
  * `triệu`, `tr` → × 1,000,000

**Ví dụ:**

| raw input   | output     |
| ----------- | ---------- |
| "3.2 tỷ"    | 3200000000 |
| "700 triệu" | 700000000  |
| "30 tr"     | 30000000   |

**Mã nguồn:**

```sql
CREATE OR REPLACE FUNCTION clean_price(raw TEXT)
RETURNS DOUBLE PRECISION AS $$
DECLARE
    num TEXT;
    number_value DOUBLE PRECISION;
BEGIN
    IF raw IS NULL OR raw = '' THEN 
        RETURN NULL;
    END IF;

    raw := lower(raw);
    raw := replace(raw, '.', '');

    num := regexp_replace(raw, '[^0-9,\.]', '', 'g');
    num := replace(num, ',', '.');

    number_value := num::double precision;

    IF raw LIKE '%tỷ%' THEN
        number_value := number_value * 1000000000;
    ELSIF raw LIKE '%tr%' OR raw LIKE '%triệu%' THEN
        number_value := number_value * 1000000;
    END IF;

    RETURN number_value;

EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
```

---

## 🧮 2. View kiểm tra chất lượng dữ liệu

View `bronze.qc_daily_overview` tổng hợp **lỗi dữ liệu theo từng ngày crawl**.

### Nội dung view bao gồm 3 nhóm chính:

---

### 🔴 A. Missing Required Fields

Kiểm tra các trường bắt buộc bị thiếu:

* `missing_title`
* `missing_address`
* `missing_price`
* `missing_area`
* `missing_legal`
* `missing_furniture`
* `missing_number_bedroom`, `missing_number_bathroom`
* …

---

### ⚠️ B. Invalid Formatting

Dựa trên 2 hàm làm sạch:

* `invalid_price_format`
* `invalid_area_format`

---

### 🔍 C. Suspicious Outliers

**Giá bất thường:**

* `< 200 triệu`
* `> 200 tỷ`

**Diện tích bất thường:**

* `< 10 m²`
* `> 2000 m²`

---

### 📌 Mã view (rút gọn – giữ nguyên logic)

```sql
CREATE OR REPLACE VIEW bronze.qc_daily_overview AS
SELECT 
    DATE(crawled_at) AS crawl_date,
    COUNT(*) AS total_records,

    -- Missing fields
    SUM(CASE WHEN title IS NULL OR title = '' THEN 1 END) AS missing_title,
    ...

    -- Invalid formats
    SUM(CASE WHEN clean_price(price) IS NULL THEN 1 END) AS invalid_price_format,
    SUM(CASE WHEN clean_area(area) IS NULL THEN 1 END) AS invalid_area_format,

    -- Suspicious data
    SUM(CASE WHEN clean_price(price) < 200000000 THEN 1 END) AS suspicious_low_price,
    SUM(CASE WHEN clean_price(price) > 200000000000 THEN 1 END) AS suspicious_high_price,
    ...
FROM bronze.staging
GROUP BY DATE(crawled_at)
ORDER BY crawl_date DESC;
```

---

## 📈 3. Dashboard kiểm tra dữ liệu (Python)

File: **`dash_quality.py`**

Chức năng:

* Kết nối Supabase (đọc `.env`)
* Lấy dữ liệu từ view `qc_daily_overview`
* Tạo biểu đồ Plotly
* Xuất ra **dashboard_quality.html**

---

## ▶️ 4. Hướng dẫn chạy Quality Dashboard

### **Bước 1 — Kích hoạt môi trường ảo**

```bash
cd Real-Estate-Pipeline
venv\Scripts\activate
```

---

### **Bước 2 — Cài đặt thư viện**

```bash
pip install -r requirements/requirements.txt
```

---

### **Bước 3 — Tạo file `.env`**

Trong thư mục `quality_check/` tạo file:

```
.env
```

Nội dung:

```
SUPABASE_URL=***
SUPABASE_KEY=***
```

---

### **Bước 4 — Chạy script**

```bash
python check_quality/dash_quality.py
```

---

### **Bước 5 — Kiểm tra output**

File HTML được sinh ra tại:

```
check_quality/dashboard_quality.html
```

Mở bằng **Chrome** hoặc bất kỳ trình duyệt nào.

---

## 📁 5. Cấu trúc thư mục gợi ý

```
quality_check/
│
├── dash_quality.py
├── README.md
├── .env               (private, không commit)
└── dashboard_quality.html   (generated)
```

---

## 🔐 6. Quyền truy cập view (nếu bị lỗi permission)

```sql
GRANT USAGE ON SCHEMA bronze TO service_role;

GRANT SELECT ON ALL TABLES IN SCHEMA bronze TO service_role;

ALTER DEFAULT PRIVILEGES IN SCHEMA bronze
GRANT SELECT ON TABLES TO service_role;
```
