import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchWindowException, WebDriverException
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import time
import logging
import random
import requests # Dùng cho cào Detail nhanh hơn
from concurrent.futures import ThreadPoolExecutor # Dùng cho cào List và Detail song song
from functools import partial

# ==============================================================================
# 1. CẤU HÌNH CHUNG
# ==============================================================================

BASE = "https://batdongsan.com.vn"
START = "/nha-dat-ban-ha-noi"
MAX_WORKERS = 4 # Số luồng tối đa để cào List Pages

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] (Thread:%(threadName)s) %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler()]
)

# ==============================================================================
# 2. CÁC HÀM CƠ BẢN (DRIVER, UTILITY, PARSE)
# ==============================================================================

def init_driver(headless=True, attempt=0):
    """Khởi tạo driver an toàn, phù hợp cho môi trường đa luồng."""
    options = uc.ChromeOptions()
    
    if headless:
        options.add_argument("--headless=new")
    
    # Core arguments
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-web-security")
    options.add_argument("--disable-site-isolation-trials")
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    options.add_argument('--log-level=3')
    options.add_argument('--silent')
    
    # Preferences - Tắt load ảnh để tăng tốc
    prefs = {
        "profile.managed_default_content_settings.images": 2, 
        "profile.default_content_setting_values.notifications": 2,
    }
    
    try:
        options.add_experimental_option("prefs", prefs)
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
    except:
        logging.debug("Experimental options not fully applied.")
    
    try:
        if attempt > 0:
            time.sleep(random.uniform(2, 4))
        
        driver = uc.Chrome(
            options=options,
            # QUAN TRỌNG: use_subprocess=False để tránh lỗi daemonic processes
            use_subprocess=False, 
            version_main=None
        )
        
        driver.set_page_load_timeout(45)
        driver.implicitly_wait(10)
        
        return driver
        
    except Exception as e:
        if attempt < 2:
            logging.warning(f"Driver init failed (attempt {attempt+1}), retrying...")
            time.sleep(random.uniform(3, 6))
            return init_driver(headless, attempt + 1)
        else:
            logging.error(f"Failed to initialize driver: {e}")
            raise


def human_like_delay(min_sec=1, max_sec=3):
    """Delay ngẫu nhiên giữa các hành động."""
    time.sleep(random.uniform(min_sec, max_sec))


def parse_card(card):
    """Parse thông tin cơ bản từ 1 card bất động sản (sử dụng BeautifulSoup)."""
    data = {}
    
    # Logic parse title, link, price, area, location... (giữ nguyên)
    title = card.select_one('h3.re__card-title') or card.select_one('span.pr-title') or card.select_one('.js__card-title')
    data["title"] = title.get_text(" ", strip=True) if title else None
    
    link = card.select_one("a.re__card-title") or card.select_one("h3.re__card-title a") or card.find_parent("a") or card.find("a", href=True)
    if link and link.get("href"):
        href = link.get("href")
        data["detail_url"] = BASE + href if href.startswith("/") else href
    else:
        data["detail_url"] = None

    data["price_text"] = (card.select_one("span.re__card-config-price") or type('obj', (object,), {'get_text': lambda *args, **kwargs: None})()).get_text(" ", strip=True)
    data["area_text"] = (card.select_one("span.re__card-config-area") or type('obj', (object,), {'get_text': lambda *args, **kwargs: None})()).get_text(" ", strip=True)
    data["price_per_m2_text"] = (card.select_one("span.re__card-config-price_per_m2") or type('obj', (object,), {'get_text': lambda *args, **kwargs: None})()).get_text(" ", strip=True)
    data["location"] = (card.select_one("div.re__card-location") or type('obj', (object,), {'get_text': lambda *args, **kwargs: None})()).get_text(" ", strip=True)

    return data


def fetch_detail_requests(url):
    """Lấy chi tiết bằng requests và BeautifulSoup (rất nhanh)."""
    d = {}
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "lxml")
            
            desc = soup.select_one("div.re__detail-content.js__pr-description")
            d["description"] = desc.get_text(" ", strip=True) if desc else None

            short_info = soup.select_one("div.re__pr-short-info.re__pr-config.js__pr-config")
            if short_info:
                items = short_info.select("div.re__pr-short-info-item.js__pr-config-item")
                for item in items:
                    title_elem = item.select_one("span.title")
                    value_elem = item.select_one("span.value")
                    if title_elem and value_elem:
                        d[title_elem.get_text(strip=True)] = value_elem.get_text(strip=True)

            specs_map = {
                ".re__icon-document": "Pháp lý", ".re__icon-apartment": "Số tầng", ".re__icon-bedroom": "Số phòng ngủ", 
                ".re__icon-bath": "Số phòng tắm, vệ sinh", ".re__icon-front-view": "Hướng nhà", 
                ".re__icon-private-house": "Hướng ban công", ".re__icon-road": "Đường vào", 
                ".re__icon-home": "Mặt tiền", ".re__icon-interior": "Nội thất",
            }
            
            for icon_class, field_name in specs_map.items():
                item = soup.select_one(f".re__pr-specs-content-item:has({icon_class})")
                if item:
                    val = item.select_one(".re__pr-specs-content-item-value")
                    d[field_name] = val.get_text(strip=True) if val else None
            
        else:
            logging.warning(f"Requests failed (Status {response.status_code}) for {url}")

    except Exception as e:
        logging.debug(f"Requests error for {url}: {e}")
    
    return url, d


def safe_get_page(driver, url, max_retries=3):
    """Load trang list với retry và kiểm tra card."""
    for attempt in range(max_retries):
        try:
            driver.get(url)
            
            # Chờ page load
            try:
                WebDriverWait(driver, 15).until(lambda d: d.execute_script('return document.readyState') == 'complete')
            except: pass
            
            # Scroll và chờ load
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight/3);")
            except: pass
            human_like_delay(1, 2)
            
            # Kiểm tra xem card đã load chưa
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.re__card-info-content, div.re__card-full"))
            )
            return True
            
        except TimeoutException:
            page_source = driver.page_source.lower()
            if 'captcha' in page_source or 'blocked' in page_source:
                logging.warning(f"Detected blocking on attempt {attempt+1}")
                if attempt < max_retries - 1:
                    human_like_delay(5, 10)
                    continue
            return False
            
        except Exception as e:
            logging.warning(f"Error loading page (attempt {attempt+1}): {e}")
            if attempt < max_retries - 1:
                human_like_delay(2, 4)
    return False

# ==============================================================================
# 3. CÀO ĐA LUỒNG (CONCURRENT THREADING)
# ==============================================================================

def crawl_single_page_task(page_num, start_path, crawl_details):
    """Task cào 1 trang list và tất cả các chi tiết của nó (trong cùng 1 luồng)."""
    driver = None
    page_data = []
    crawl_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        # Delay để phân tán thời gian khởi tạo driver
        human_like_delay(page_num * 0.3, page_num * 0.5) 
        
        driver = init_driver(headless=True)
        url = BASE + start_path + (f"/p{page_num}" if page_num > 1 else "")
        
        logging.info(f"[P{page_num}] Starting list page crawl...")
        
        if not safe_get_page(driver, url):
            logging.error(f"[P{page_num}] Failed to load list page")
            return []
        
        html = driver.page_source
        soup = BeautifulSoup(html, "lxml")
        
        # Lấy cards
        selectors = ["div.re__card-info-content", "div.re__card-full", "div[class*='re__card']"]
        cards = []
        for selector in selectors:
            cards = soup.select(selector)
            if cards: break
        
        if not cards:
            logging.warning(f"[P{page_num}] No cards found.")
            return []
        
        logging.info(f"[P{page_num}] Found {len(cards)} cards. Extracting base info...")
        
        # 1. Trích xuất thông tin cơ bản
        list_data_with_url = []
        for card in cards:
            row = parse_card(card)
            row["Thời gian crawl"] = crawl_time
            row["Trang"] = page_num
            list_data_with_url.append(row)
        
        # 2. Cào chi tiết (Requests + Threading (nội bộ))
        if crawl_details and list_data_with_url:
            logging.info(f"[P{page_num}] Starting detail fetch for {len(list_data_with_url)} items...")
            detail_urls = [row["detail_url"] for row in list_data_with_url if row.get("detail_url")]
            detail_results = {}
            
            # ThreadPoolExecutor nhỏ để cào chi tiết song song (nội bộ)
            with ThreadPoolExecutor(max_workers=MAX_WORKERS * 2) as executor: 
                futures = [executor.submit(fetch_detail_requests, url) for url in detail_urls]
                
                for future in futures:
                    url, detail_data = future.result()
                    detail_results[url] = detail_data
            
            # Hợp nhất dữ liệu
            for row in list_data_with_url:
                detail_url = row.get("detail_url")
                if detail_url and detail_url in detail_results:
                    row.update(detail_results[detail_url])
                page_data.append(row)
                
            logging.info(f"[P{page_num}] ✓ Done: {len(page_data)} items (Detail merged)")
            
        else:
             page_data = list_data_with_url
             logging.info(f"[P{page_num}] ✓ Done: {len(page_data)} items (List only)")
        
    except Exception as e:
        logging.error(f"[P{page_num}] Critical error: {e}")
    finally:
        if driver:
            try:
                driver.quit()
                time.sleep(0.5)
            except: pass
    
    return page_data


def crawl_pages_concurrent(start_path, start_page=1, end_page=10, crawl_details=True, max_workers=MAX_WORKERS):
    """Chức năng chính sử dụng ThreadPoolExecutor để chạy List Pages song song."""
    logging.info(f"=== CONCURRENT THREADING: Pages {start_page}-{end_page}, {max_workers} workers ===")
    
    pages = range(start_page, end_page + 1)
    all_data = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        func = partial(crawl_single_page_task, start_path=start_path, crawl_details=crawl_details)
        
        # Gửi các task cào từng trang list
        future_to_page = {executor.submit(func, page_num): page_num for page_num in pages}
        
        for future in future_to_page:
            page_num = future_to_page[future]
            try:
                page_data = future.result()
                all_data.extend(page_data)
                logging.info(f"[P{page_num}] Result gathered: {len(page_data)} items")
            except Exception as exc:
                logging.error(f"[P{page_num}] Thread generated an exception: {exc}")
    
    return all_data

# ==============================================================================
# 4. HÀM MAIN
# ==============================================================================

def main():
    try:
        logging.info("="*80)
        logging.info("BATDONGSAN.COM.VN CRAWLER - ULTIMATE OPTIMIZED VERSION")
        logging.info("="*80)
        
        # ========== CẤU HÌNH ==========
        START_PAGE = 1
        END_PAGE = 3   # Nên giữ nhỏ khi debug
        CRAWL_DETAILS = True      
        MODE = "CONCURRENT"      # LUÔN DÙNG CONCURRENT/THREADING
        USE_DELTA_LAKE = True     # Bật/tắt lưu vào Delta Lake
        # ==============================
        
        if MODE == "CONCURRENT":
            data = crawl_pages_concurrent(
                START, 
                start_page=START_PAGE, 
                end_page=END_PAGE, 
                crawl_details=CRAWL_DETAILS,
                max_workers=MAX_WORKERS
            )
        else:
            logging.error("Vui lòng đặt MODE='CONCURRENT' để có hiệu suất tối đa.")
            data = []
            
        # Kết quả
        if len(data) > 0:
            df = pd.DataFrame(data)
            logging.info("="*80)
            logging.info(f"✓ SUCCESS: {len(df)} ITEMS")
            logging.info("="*80)
            
            if "detail_url" in df.columns: df.rename(columns={"detail_url": "Link tin"}, inplace=True)
            priority_cols = ["Trang", "Thời gian crawl", "Link tin", "Mã tin", "Ngày đăng", "Ngày hết hạn", "Loại tin", "title"]
            existing_priority = [col for col in priority_cols if col in df.columns]
            other_cols = [col for col in df.columns if col not in existing_priority]
            df = df[existing_priority + other_cols]
            
            if "Trang" in df.columns: df = df.sort_values("Trang")
            
            # Lưu CSV (backup)
            output_file = f"batdongsan_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(output_file, index=False, encoding="utf-8-sig")
            logging.info(f"✓ Saved CSV backup: {output_file}")
            
            # ===== LƯU VÀO DELTA LAKE =====
            if USE_DELTA_LAKE:
                try:
                    from src.load.delta_lake_manager import quick_save_raw
                    logging.info("Saving to Delta Lake...")
                    quick_save_raw(df)
                    logging.info("✓ Saved to Delta Lake (Raw layer)")
                except Exception as e:
                    logging.warning(f"⚠ Delta Lake save failed: {e}")
                    logging.warning("  (Data still saved to CSV)")
            # ==============================
            
            print("\n" + "="*100)
            print(f"✓ CRAWLED {len(df)} BẤT ĐỘNG SẢN (Trang {START_PAGE}-{END_PAGE})")
            print("="*100)
            
            if "Trang" in df.columns:
                print(f"\n📊 Thống kê số lượng theo trang:")
                print(df.groupby("Trang").size())
            
            print("\n" + "="*100)
            print("📋 Preview 10 records:")
            print("="*100)
            pd.set_option("display.max_columns", None)
            pd.set_option("display.max_colwidth", 60)
            print(df.head(10).to_string(index=False))
            print("="*100)
            print(f"\n💾 Saved: {output_file}")
            
        else:
            logging.warning("❌ No data!")
            
    except KeyboardInterrupt:
        logging.info("\n⚠ Stopped by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
    finally:
        logging.info("="*80)
        logging.info("FINISHED")
        logging.info("="*80)


if __name__ == "__main__":
    main()