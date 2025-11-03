import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
import nodriver as uc
from utils import (
    extract_value_from_specs,
    text_from_selector,
    extract_value_from_project_card,
    extract_value_from_post_card,
    save_results_to_csv,
    wait_for_content_load,
)
from typing import Optional
from nodriver.core.connection import ProtocolException
from config import get_page_semaphore, get_subpage_semaphore, CrawlConfig


# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_URL = "https://batdongsan.com.vn"
START = "/nha-dat-ban/"

RAW_DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"


async def extract_subpage_urls(page):
    # Sử dụng JavaScript để lấy tất cả các phần tử a có class js__product-link-for-product-id
    js_code = """
    Array.from(document.querySelectorAll('a.js__product-link-for-product-id'))
         .map(element => element.href)
         .filter(href => href && href !== '');
    """
    href_list = await page.evaluate(js_code)
    
    # Lọc các URL tương đối và chuyển đổi thành URL tuyệt đối nếu cần
    subpage_urls = []
    for href in href_list:
        href = href.get("value")
        if href.startswith('/'):  
            subpage_urls.append(BASE_URL + href)
        elif href.startswith(BASE_URL):  
            subpage_urls.append(href)
    
    return subpage_urls


async def extract_data_from_page(page):
    """Extract data từ page sau khi đã load hoàn toàn và scroll"""

    logger.info(f"Bắt đầu extract data từ: {page.url}")

    # Chờ page load hoàn toàn và trigger lazy loading
    await wait_for_content_load(page)

    item = {}

    item['title'] = await text_from_selector(page, "h1[class='re__pr-title pr-title js__pr-title']")
    item['address'] = await text_from_selector(page, "span[class='re__pr-short-description js__pr-address']")

    item['price'] = await extract_value_from_specs(page, "Khoảng giá")
    item['area'] = await extract_value_from_specs(page, "Diện tích")

    item['house_direction'] = await extract_value_from_specs(page, "Hướng nhà")
    item['balcony_direction'] = await extract_value_from_specs(page, "Hướng ban công")
    item['facade'] = await extract_value_from_specs(page, "Mặt tiền")
    item['legal'] = await extract_value_from_specs(page, "Pháp lý")
    item['furniture'] = await extract_value_from_specs(page, "Nội thất")
    item['number_bedroom'] = await extract_value_from_specs(page, "Số phòng ngủ")
    item['number_bathroom'] = await extract_value_from_specs(page, "Số phòng tắm, vệ sinh")
    item['number_floor'] = await extract_value_from_specs(page, "Số tầng")
    item['way_in'] = await extract_value_from_specs(page, "Đường vào")

    item['project_name'] = await text_from_selector(page, "div[class='re__project-title']")
    item['project_status'] = await extract_value_from_project_card(page, "re__icon-info-circle--sm")
    item['project_investor'] = await extract_value_from_project_card(page, "re__icon-office--sm")
    
    item['post_id'] = await extract_value_from_post_card(page, "Mã tin")
    item['post_start_time'] = await extract_value_from_post_card(page, "Ngày đăng")
    item['post_end_time'] = await extract_value_from_post_card(page, "Ngày hết hạn")
    item['post_type'] = await extract_value_from_post_card(page, "Loại tin")
    
    item["source"] = "batdongsan.com.vn"
    item["url"] = page.url
    item["crawled_at"] = datetime.now(timezone.utc).isoformat()

    return item
    

async def scrape_subpage(url: str, subpage_semaphore: asyncio.Semaphore, browser):
    """
    Hàm xử lý một subpage
    """
    async with subpage_semaphore:  # Giới hạn 10 subpage đồng thời cho mỗi main page
        logger.info(f"  Đang xử lý subpage: {url}")
        item: Optional[dict] = None
        subpage = None
        try:
            subpage = await browser.get(url, new_tab=True)
            try:
                item = await extract_data_from_page(subpage)
            except ProtocolException as proto_error:
                logger.warning(f"ProtocolException tại {url}: {proto_error}. Thử reload...")
                try:
                    await subpage.reload()
                    await asyncio.sleep(5)
                    item = await extract_data_from_page(subpage)
                except Exception as retry_error:
                    logger.warning(f"Reload vẫn lỗi với {url}: {retry_error}")
                    item = {
                        "url": url,
                        "source": "batdongsan.com.vn",
                        "error": "protocol_exception"
                    }
            except Exception as error:
                logger.warning(f"Lỗi không mong đợi tại {url}: {error}")
                item = {
                    "url": url,
                    "source": "batdongsan.com.vn",
                    "error": "unexpected_exception"
                }
        finally:
            if subpage:
                try:
                    await subpage.close()
                except Exception as close_error:
                    logger.debug(f"Không thể đóng subpage {url}: {close_error}")

        if not item:
            item = {
                "url": url,
                "source": "batdongsan.com.vn",
                "error": "empty_item"
            }

        logger.info(f"  Đã hoàn thành subpage: {url}")
        return item

async def scrape_main_page(url: str, page_semaphore: asyncio.Semaphore, subpage_semaphore: asyncio.Semaphore):
    """
    Hàm xử lý một main page và các subpage của nó
    """
    async with page_semaphore:  
        logger.info(f"Đang xử lý main page: {url}")
        
        # Khởi tạo browser và mở page với các tùy chọn chống phát hiện
        browser = await uc.start(
            headless=True,
            browser_args=[
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',
                '--disable-extensions',
                '--disable-plugins',
                '--window-size=136,768',
                '--lang=vi-VN',
                '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                '--disable-blink-features=AutomationControlled',
                '--disable-features=VizDisplayCompositor',
                '--enable-automation',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding',
                '--disable-ipc-flooding-protection',
                '--disable-background-networking',
                '--disable-default-apps',
                '--disable-extensions',
                '--disable-plugins',
                '--disable-sync',
                '--no-first-run',
                '--disable-bundled-ppapi-flash',
                '--disable-accelerated-2d-canvas',
                '--disable-accelerated-jpeg-decoding',
                '--disable-dev-shm-usage',
                '--disable-web-security',
                '--allow-running-insecure-content',
                '--disable-features=TranslateUI',
                '--remote-debugging-port=9222',
                '--disable-features=VizDisplayCompositor'
            ]
        )
        
        page = await browser.get(url)
        
        # Thêm đoạn mã giả mạo các thuộc tính đặc trưng của bot
        await page.evaluate(
            """
            // Xóa webdriver property
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            
            // Tạo plugin array giả
            Object.defineProperty(navigator, 'plugins', {
                get: () => {
                    const pluginArray = [
                        { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format' },
                        { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: '' },
                        { name: 'Native Client', filename: 'internal-nacl-plugin', description: '' }
                    ];
                    pluginArray.length = 3;
                    return pluginArray;
                },
            });
            
            // Tạo ngôn ngữ giả
            Object.defineProperty(navigator, 'languages', {
                get: () => ['vi-VN', 'vi', 'en-US', 'en'],
            });
            
            // Thêm thuộc tính missing
            Object.defineProperty(navigator, 'vendor', {
                get: () => 'Google Inc.',
            });
            
            // Thêm thuộc tính webdriver giả
            Object.defineProperty(navigator, 'userAgent', {
                get: () => 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            });
            
            // Thêm window.chrome giả
            if (window.chrome) {
                window.chrome.runtime = {
                    connect: () => {},
                    sendMessage: () => {}
                };
            } else {
                Object.defineProperty(window, 'chrome', {
                    value: {
                        runtime: {
                            connect: () => {},
                            sendMessage: () => {}
                        }
                    },
                    writable: true
                });
            }
            
            // Giả lập webgl
            const originalWebgl = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(parameter) {
                if (parameter === 37445) return 'Intel Inc.';
                if (parameter === 37446) return 'Intel Iris OpenGL Engine';
                return originalWebgl.call(this, parameter);
            };
            
            // Giả lập canvas
            const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
            HTMLCanvasElement.prototype.toDataURL = function() {
                // Thêm nhiễu ngẫu nhiên vào canvas để tránh phát hiện
                return originalToDataURL.apply(this, arguments);
            };
            """
        )
        
        # Chờ ngẫu nhiên để mô phỏng hành vi người dùng thật
        await asyncio.sleep(5)
        
        # Lấy danh sách subpage từ main page này
        subpage_urls = await extract_subpage_urls(page)
        
        logger.info(f"Tìm thấy {len(subpage_urls)} subpage từ {url}")
        
        if len(subpage_urls) > 0:
            # Cào các subpage với giới hạn 10 subpage đồng thời
            subpage_tasks = []
            for subpage_url in subpage_urls:
                task = scrape_subpage(subpage_url, subpage_semaphore, browser)
                subpage_tasks.append(task)
            subpage_results_raw = await asyncio.gather(*subpage_tasks, return_exceptions=True)

            subpage_results = []
            for result in subpage_results_raw:
                if isinstance(result, Exception):
                    logger.warning(f"Subpage task exception: {result}")
                    subpage_results.append({
                        "url": url,
                        "source": "batdongsan.com.vn",
                        "error": "subpage_task_exception"
                    })
                else:
                    subpage_results.append(result)
        else:
            subpage_results = []
        
        # Đóng browser khi hoàn thành
        browser.stop()
        
    logger.info(f"Đã hoàn thành main page: {url} với {len(subpage_results)} subpage")
    return {
            "main_page_url": url,
            "subpage_count": len(subpage_results),
            "subpage_data": subpage_results
        }

async def main():
    """
    Hàm chính để chạy toàn bộ quá trình cào dữ liệu
    """
    logger.info("Bắt đầu quá trình cào dữ liệu")
    
    # Giới hạn số page cào đồng thời
    page_semaphore = get_page_semaphore()
    
    # Giới hạn số subpage cào đồng thời
    subpage_semaphore = get_subpage_semaphore()
    
    # Trang bắt đầu thu thập
    start_page = CrawlConfig.START_PAGE
    
    # Trang kết thúc thu thập
    end_page = CrawlConfig.END_PAGE
    
    main_urls = [f"{BASE_URL}{START}p{i}" for i in range(start_page, end_page + 1)]
    
    logger.info(f"Đang xử lý {len(main_urls)} main page: {main_urls}")
    
    # Tạo task cho mỗi main page
    tasks = []
    for url in main_urls:
        task = scrape_main_page(url, page_semaphore, subpage_semaphore)
        tasks.append(task)

    # Chạy tất cả các task với giới hạn 4 main page đồng thời
    all_results_raw = await asyncio.gather(*tasks, return_exceptions=True)
    all_results = []
    for result in all_results_raw:
        if isinstance(result, Exception):
            logger.warning(f"Main page task exception: {result}")
        else:
            all_results.append(result)

    logger.info(f"Đã hoàn thành cào dữ liệu từ {len(all_results)} main page")

    
    save_results_to_csv(all_results)


if __name__ == "__main__":
    # Sử dụng nodriver.loop() thay vì asyncio.run()
    results = uc.loop().run_until_complete(main())