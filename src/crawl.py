import time, random, csv, sys
from urllib.parse import urljoin, urlencode
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

BASE = "https://batdongsan.com.vn"
START = "/nha-dat-ban-ha-noi?vrs=1"   # trang đầu Hà Nội

def parse_price(text):
    import re
    if not text: return None
    t = text.lower().replace(" ", "")
    t = t.replace(".", "").replace("m²", "").replace("/m²", "")
    t = t.replace("m2", "")
    t = t.replace("giáthỏa thuận", "").replace("thoathuận", "")
    t = t.replace("≈", "")
    t = t.replace(",", ".")
    m_ty  = re.search(r'([\d\.]+)tỷ', t)
    m_tr  = re.search(r'([\d\.]+)tr', t)
    m_trv = re.search(r'([\d\.]+)tr/m', t)
    if m_ty:
        return float(m_ty.group(1)) * 1e9
    if m_tr and not m_trv:
        return float(m_tr.group(1)) * 1e6
    m_num = re.search(r'^\d+(\.\d+)?$', t)
    if m_num:
        return float(m_num.group(0))
    return None

def parse_area(text):
    import re
    if not text: return None
    t = text.lower().replace(" ", "")
    t = t.replace(",", ".")
    m = re.search(r'([\d\.]+)', t)
    return float(m.group(1)) if m else None

def parse_card(card):
    data = {}
    # --- TIÊU ĐỀ ---
    title_tag = card.select_one("h3.re__card-title span.pr-title.js__card-title[product-title]") \
        or card.select_one("span.pr-title.js__card-title[product-title]") \
        or card.select_one("h3.re__card-title, h2.re__card-title, .re__card-title, h1, h3")
    data["title"] = title_tag.get_text(" ", strip=True) if title_tag else None

    # --- CỤM THÔNG SỐ ---
    price_tag = card.select_one("span.re__card-config-price")
    area_tag  = card.select_one("span.re__card-config-area")
    unit_tag  = card.select_one("span.re__card-config-price_per_m2")

    price_txt = price_tag.get_text(" ", strip=True) if price_tag else None
    area_txt  = area_tag.get_text(" ", strip=True) if area_tag else None
    unit_txt  = unit_tag.get_text(" ", strip=True) if unit_tag else None

    data["price_text"]        = price_txt
    data["area_text"]         = area_txt
    data["price_per_m2_text"] = unit_txt

    data["price_vnd"] = parse_price(price_txt)
    data["area_m2"]   = parse_area(area_txt)

    # --- VỊ TRÍ ---
    loc_tag = card.select_one("div.re__card-location span:not(.re__card-config-dot)") \
        or card.select_one(".re__card-location, .re__location")
    data["location"] = loc_tag.get_text(" ", strip=True) if loc_tag else None

    # --- LINK CHI TIẾT ---
    a = card.select_one('a[href*="/tin-"], a[href^="/ban-"], a[href^="/cho-thue-"]') \
        or card.select_one("a[href]")
    if a and a.has_attr("href"):
        data["detail_url"] = urljoin(BASE, a["href"].split("?")[0])

    # --- MÔ TẢ NGẮN ---
    desc = card.select_one("div.re__card-description")
    data["short_description"] = desc.get_text(" ", strip=True) if desc else None

    return data

def parse_detail_selenium(driver, url):
    # Vào trang chi tiết lấy thêm thông tin
    driver.get(url)
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.re__detail-content.js__pr-description, .re__detail-content"))
        )
    except Exception:
        return {}

    html = driver.page_source
    soup = BeautifulSoup(html, "lxml")
    d = {}
    desc = soup.select_one("div.re__detail-content.js__pr-description") \
        or soup.select_one(".re__detail-content")
    d["description"] = desc.get_text(" ", strip=True) if desc else None

    def get_spec_by_icon(icon_class, label):
        item = soup.select_one(f".re__pr-specs-content-item:has(.{icon_class})")
        if item:
            val = item.select_one(".re__pr-specs-content-item-value")
            if val:
                d[label] = val.get_text(strip=True)

    get_spec_by_icon("re__icon-document", "Pháp lý")
    get_spec_by_icon("re__icon-apartment", "Số tầng")
    get_spec_by_icon("re__icon-bedroom", "Số phòng ngủ")
    get_spec_by_icon("re__icon-bath", "Số phòng tắm, vệ sinh")
    get_spec_by_icon("re__icon-front-view", "Hướng nhà")
    get_spec_by_icon("re__icon-private-house", "Hướng ban công")
    get_spec_by_icon("re__icon-road", "Đường vào")
    get_spec_by_icon("re__icon-home", "Mặt tiền")
    get_spec_by_icon("re__icon-interior", "Nội thất")
    return d

def crawl_listing_selenium(driver, start_path, max_pages=2, delay=(1.5, 3.5)):
    results = []
    seen = set()
    page = 1
    while page <= max_pages:
        if page == 1:
            url = urljoin(BASE, start_path)
        else:
            sep = "&" if "?" in start_path else "?"
            url = urljoin(BASE, f"{start_path}{sep}{urlencode({'p': page})}")

        print(f"[Page {page}] {url}")
        driver.get(url)
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.re__card-info-content"))
            )
        except Exception:
            print("  -> hết dữ liệu (không còn thẻ tin).")
            break

        html = driver.page_source
        with open("debug.html", "w", encoding="utf-8") as f:
            f.write(html)
        soup = BeautifulSoup(html, "lxml")
        cards = soup.select('div.re__card-info-content')
        if not cards:
            print("  -> hết dữ liệu (không còn thẻ tin).")
            break

        for c in cards:
            row = parse_card(c)
            du = row.get("detail_url")
            if not du or du in seen:
                continue
            seen.add(du)
            # Crawl chi tiết từng tin
            extra = parse_detail_selenium(driver, du)
            row.update(extra)
            results.append(row)
            time.sleep(random.uniform(*delay))

        page += 1
        time.sleep(random.uniform(*delay))
    return results

if __name__ == "__main__":
    max_pages = 2
    if len(sys.argv) >= 2:
        try:
            max_pages = int(sys.argv[1])
        except:
            pass

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=options)

    data = crawl_listing_selenium(driver, START, max_pages=max_pages)
    print(f"Crawled {len(data)} items")

    if data:
        for i, row in enumerate(data, 1):
            print(f"\n--- Tin số {i} ---")
            for k, v in row.items():
                print(f"{k}: {v}")
    else:
        print("No data crawled.")

    driver.quit()
