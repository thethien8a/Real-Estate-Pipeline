import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchWindowException
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import time
import logging
import random
import os

BASE  = "https://batdongsan.com.vn"
START = "/nha-dat-ban-ha-noi"

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)

def init_driver(headless=False):
    """Khởi tạo driver - version đơn giản"""
    options = uc.ChromeOptions()
    
    # Các option cơ bản
    if headless:
        options.add_argument("--headless=new")
    
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    try:
        # Khởi tạo driver đơn giản
        driver = uc.Chrome(options=options)
        driver.set_page_load_timeout(60)
        
        logging.info("Driver initialized successfully")
        return driver
        
    except Exception as e:
        logging.error("Error initializing driver: %s", str(e))
        raise


def parse_card(card):
    """Parse thông tin từ 1 card bất động sản"""
    data = {}

    # Title
    title = (
        card.select_one('h3.re__card-title')
        or card.select_one('span.pr-title')
        or card.select_one('.js__card-title')
    )
    data["title"] = title.get_text(" ", strip=True) if title else None

    # Link chi tiết
    link = (
        card.select_one("a.re__card-title") 
        or card.select_one("h3.re__card-title a")
        or card.find_parent("a")
        or card.find("a", href=True)
    )
    
    if link and link.get("href"):
        href = link.get("href")
        data["detail_url"] = BASE + href if href.startswith("/") else href
    else:
        data["detail_url"] = None

    # Giá
    price = card.select_one("span.re__card-config-price")
    data["price_text"] = price.get_text(" ", strip=True) if price else None

    # Diện tích
    area = card.select_one("span.re__card-config-area")
    data["area_text"] = area.get_text(" ", strip=True) if area else None

    # Đơn giá
    unit = card.select_one("span.re__card-config-price_per_m2")
    data["price_per_m2_text"] = unit.get_text(" ", strip=True) if unit else None

    # Địa chỉ
    loc = card.select_one("div.re__card-location")
    data["location"] = loc.get_text(" ", strip=True) if loc else None

    return data


def parse_detail_page(driver, url):
    """Parse chi tiết từ trang detail"""
    d = {}
    
    try:
        if not safe_get_page(driver, url, max_retries=2):
            logging.warning("Cannot load detail page: %s", url)
            return d
        
        # Đợi trang load
        time.sleep(3)
        
        # Scroll để load hết nội dung
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
        time.sleep(1)
        
        html = driver.page_source
        soup = BeautifulSoup(html, "lxml")
        
        # --- MÔ TẢ ---
        desc = soup.select_one("div.re__detail-content.js__pr-description")
        d["description"] = desc.get_text(" ", strip=True) if desc else None

        # --- THÔNG TIN NGẮN (Ngày đăng, Ngày hết hạn, Loại tin, Mã tin) ---
        short_info_container = soup.select_one("div.re__pr-short-info.re__pr-config.js__pr-config")
        
        if short_info_container:
            info_items = short_info_container.select("div.re__pr-short-info-item.js__pr-config-item")
            
            for item in info_items:
                title_elem = item.select_one("span.title")
                value_elem = item.select_one("span.value")
                
                if title_elem and value_elem:
                    title_text = title_elem.get_text(strip=True)
                    value_text = value_elem.get_text(strip=True)
                    
                    # Map trực tiếp theo tên
                    if title_text == "Ngày đăng":
                        d["Ngày đăng"] = value_text
                    elif title_text == "Ngày hết hạn":
                        d["Ngày hết hạn"] = value_text
                    elif title_text == "Loại tin":
                        d["Loại tin"] = value_text
                    elif title_text == "Mã tin":
                        d["Mã tin"] = value_text

        # --- PHÁP LÝ ---
        item = soup.select_one(".re__pr-specs-content-item:has(.re__icon-document)")
        if item:
            val = item.select_one(".re__pr-specs-content-item-value")
            d["Pháp lý"] = val.get_text(strip=True) if val else None

        # --- SỐ TẦNG ---
        item = soup.select_one(".re__pr-specs-content-item:has(.re__icon-apartment)")
        if item:
            val = item.select_one(".re__pr-specs-content-item-value")
            d["Số tầng"] = val.get_text(strip=True) if val else None

        # --- SỐ PHÒNG NGỦ ---
        item = soup.select_one(".re__pr-specs-content-item:has(.re__icon-bedroom)")
        if item:
            val = item.select_one(".re__pr-specs-content-item-value")
            d["Số phòng ngủ"] = val.get_text(strip=True) if val else None

        # --- SỐ PHÒNG TẮM / VỆ SINH ---
        item = soup.select_one(".re__pr-specs-content-item:has(.re__icon-bath)")
        if item:
            val = item.select_one(".re__pr-specs-content-item-value")
            d["Số phòng tắm, vệ sinh"] = val.get_text(strip=True) if val else None

        # --- HƯỚNG NHÀ ---
        item = soup.select_one(".re__pr-specs-content-item:has(.re__icon-front-view)")
        if item:
            val = item.select_one(".re__pr-specs-content-item-value")
            d["Hướng nhà"] = val.get_text(strip=True) if val else None

        # --- HƯỚNG BAN CÔNG ---
        item = soup.select_one(".re__pr-specs-content-item:has(.re__icon-private-house)")
        if item:
            val = item.select_one(".re__pr-specs-content-item-value")
            d["Hướng ban công"] = val.get_text(strip=True) if val else None

        # --- ĐƯỜNG VÀO ---
        item = soup.select_one(".re__pr-specs-content-item:has(.re__icon-road)")
        if item:
            val = item.select_one(".re__pr-specs-content-item-value")
            d["Đường vào"] = val.get_text(strip=True) if val else None

        # --- MẶT TIỀN ---
        item = soup.select_one(".re__pr-specs-content-item:has(.re__icon-home)")
        if item:
            val = item.select_one(".re__pr-specs-content-item-value")
            d["Mặt tiền"] = val.get_text(strip=True) if val else None

        # --- NỘI THẤT ---
        item = soup.select_one(".re__pr-specs-content-item:has(.re__icon-interior)")
        if item:
            val = item.select_one(".re__pr-specs-content-item-value")
            d["Nội thất"] = val.get_text(strip=True) if val else None
        
        logging.info("✓ Parsed detail page successfully")
        
    except Exception as e:
        logging.warning("Error parsing detail page %s: %s", url, str(e))
    
    return d


def safe_get_page(driver, url, max_retries=3):
    """Load trang với retry logic"""
    for attempt in range(max_retries):
        try:
            logging.info("Loading page (attempt %d/%d): %s", attempt + 1, max_retries, url)
            driver.get(url)
            time.sleep(3)  # Đợi trang load
            
            # Kiểm tra xem browser còn mở không
            driver.current_url
            return True
            
        except NoSuchWindowException:
            logging.error("Browser window closed unexpectedly")
            return False
            
        except Exception as e:
            logging.warning("Error loading page (attempt %d): %s", attempt + 1, str(e))
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                return False
    
    return False


def crawl_pages(driver, start_path, start_page=1, end_page=1, crawl_details=True):
    """Crawl nhiều trang từ start_page đến end_page"""
    all_data = []
    crawl_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Thời gian bắt đầu crawl

    for page in range(start_page, end_page + 1):
        url = BASE + start_path + (f"/p{page}" if page > 1 else "")
        
        # Load trang
        if not safe_get_page(driver, url):
            logging.error("Failed to load page %s", page)
            break
        
        try:
            # Scroll để trigger lazy load
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(2)
            
            # Lấy HTML
            html = driver.page_source
            soup = BeautifulSoup(html, "lxml")
            
            # Thử nhiều selector
            selectors = [
                "div.re__card-info-content",
                "div.re__card-full",
                "div[class*='re__card']",
                ".js__card",
                "div.product-item"
            ]
            
            cards = []
            for selector in selectors:
                cards = soup.select(selector)
                if cards:
                    logging.info("Found %d cards with selector: %s", len(cards), selector)
                    break
            
            if not cards:
                logging.warning("No cards found at page %s", page)
                
                # Debug: Lưu HTML
                debug_file = f"debug_page_{page}.html"
                with open(debug_file, "w", encoding="utf-8") as f:
                    f.write(html)
                logging.info("Saved HTML to %s for debugging", debug_file)
                
                # In một số thông tin debug
                logging.info("Page title: %s", soup.title.string if soup.title else "No title")
                logging.info("Body length: %d characters", len(html))
                
                # Tìm xem có div nào không
                all_divs = soup.find_all("div", limit=10)
                logging.info("Found %d div tags (showing first 10 classes):", len(all_divs))
                for i, div in enumerate(all_divs[:10], 1):
                    logging.info("  Div %d: class=%s", i, div.get("class"))
                
                break

            # Parse từng card
            for idx, card in enumerate(cards, 1):
                try:
                    row = parse_card(card)
                    
                    if idx <= 3:  # Log 3 card đầu
                        logging.info("Card %d: %s", idx, row.get("title", "No title")[:60])
                    
                    # Crawl detail nếu có URL và được yêu cầu
                    if crawl_details and row.get("detail_url"):
                        logging.info("Crawling detail %d/%d...", idx, len(cards))
                        detail_data = parse_detail_page(driver, row["detail_url"])
                        row.update(detail_data)
                        time.sleep(random.uniform(2, 4))  # Delay giữa các detail
                    
                    row["Thời gian crawl"] = crawl_time  # Thêm thời gian crawl
                    all_data.append(row)
                    
                except Exception as e:
                    logging.warning("Error parsing card %d: %s", idx, str(e))
                    continue
            
            logging.info("Parsed %d cards from page %s", len(cards), page)
            time.sleep(random.uniform(2, 4))
            
        except Exception as e:
            logging.error("Error processing page %s: %s", page, str(e))
            break
            
    return all_data


def main():
    driver = None
    
    try:
        logging.info("Starting crawler...")
        driver = init_driver(headless=False)
        
        # Test connection
        logging.info("Testing connection to website...")
        if not safe_get_page(driver, BASE):
            logging.error("Cannot connect to website")
            return
        
        logging.info("Connection successful! Starting crawl...")
        time.sleep(2)
        
        # Crawl (crawl_details=True để lấy chi tiết)
        # Thiết lập trang bắt đầu và kết thúc
        START_PAGE = 20  # Trang bắt đầu
        END_PAGE = 30    # Trang kết thúc
        
        logging.info("Will crawl from page %d to page %d", START_PAGE, END_PAGE)
        data = crawl_pages(driver, START, start_page=START_PAGE, end_page=END_PAGE, crawl_details=True)
        
        if len(data) > 0:
            df = pd.DataFrame(data)
            logging.info("✓ Successfully crawled %d items", len(df))
            
            # Đổi tên cột detail_url thành Link tin
            if "detail_url" in df.columns:
                df.rename(columns={"detail_url": "Link tin"}, inplace=True)
            
            # Sắp xếp lại thứ tự cột - các cột quan trọng ở đầu
            priority_cols = ["Thời gian crawl", "Link tin", "Mã tin", "Ngày đăng", "Ngày hết hạn", "Loại tin", "title"]
            other_cols = [col for col in df.columns if col not in priority_cols]
            
            # Chỉ lấy các cột priority tồn tại trong df
            existing_priority = [col for col in priority_cols if col in df.columns]
            final_cols = existing_priority + other_cols
            df = df[final_cols]
            
            # Lưu ra CSV
            output_file = f"batdongsan_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(output_file, index=False, encoding="utf-8-sig")
            logging.info("✓ Saved to %s", output_file)
            
            # Hiển thị
            pd.set_option("display.max_columns", None)
            pd.set_option("display.max_rows", None)
            pd.set_option("display.width", None)
            pd.set_option("display.max_colwidth", 80)
            
            print("\n" + "="*150)
            print(f"ĐÃ CRAWL ĐƯỢC {len(df)} BẤT ĐỘNG SẢN")
            print("="*150)
            print(df.to_string(index=False))
            print("="*150)
            print(f"\n✓ Đã lưu vào file: {output_file}")
            
        else:
            logging.warning("✗ No data crawled!")
            logging.warning("Please check:")
            logging.warning("  1. debug_page_1.html file")
            logging.warning("  2. Browser window for any errors")
            logging.warning("  3. Website might have changed structure")
            
    except KeyboardInterrupt:
        logging.info("Interrupted by user")
        
    except Exception as e:
        logging.error("Fatal error: %s", str(e), exc_info=True)
        
    finally:
        if driver:
            try:
                logging.info("Closing browser...")
                driver.quit()
                time.sleep(1)
            except:
                pass


if __name__ == "__main__":
    main()