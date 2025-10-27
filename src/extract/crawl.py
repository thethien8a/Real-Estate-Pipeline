import asyncio
import csv
import logging
from datetime import datetime
from pathlib import Path

import nodriver as uc
from typing import List

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
    subpage_elements = await page.select_all("a.js__product-link-for-product-id")
    subpage_urls = []
    for element in subpage_elements:
        href = await element.get_attribute("href")
        if href:
            subpage_urls.append(BASE_URL + href)
    return subpage_urls

async def extract_data_from_page(page):
    await page.get_content()
    
    # Tổng quan bất động sản
    ## Tên bất động sản
    title_element = await page.query_selector("h1[class='cre__pr-title pr-title js__pr-title']")
    title = title_element.text_content()
    
    # Địa chỉ bất động sản
    address_element = await page.query_selector("span[class='pre__pr-short-description js__pr-address']")
    address = address_element.text_content()
    
    # Chi tiết bất động sản
    ## Khoảng giá
    price_element = await page.query_selector("span[class='cre__pr-price-text js__pr-price-text']")

async def scrape_subpage(url: str, subpage_semaphore: asyncio.Semaphore, browser):
    """
    Hàm xử lý một subpage
    """
    async with subpage_semaphore:  # Giới hạn 10 subpage đồng thời cho mỗi main page
        logger.info(f"  Đang xử lý subpage: {url}")
        
        # Mở tab mới cho subpage
        subpage = await browser.get(url, new_tab=True)
        
        # Thực hiện cào dữ liệu từ subpage
        data = await extract_data_from_page(subpage)
        
        # Đóng tab subpage
        await subpage.close()
        
        logger.info(f"  Đã hoàn thành subpage: {url}")
        return data

async def scrape_main_page(url: str, page_semaphore: asyncio.Semaphore, subpage_semaphore: asyncio.Semaphore):
    """
    Hàm xử lý một main page và các subpage của nó
    """
    async with page_semaphore:  # Giữ một slot trong 4 slot cho phép
        logger.info(f"Đang xử lý main page: {url}")
        
        # Khởi tạo browser và mở page
        browser = await uc.start(headless=True)
        page = await browser.get(url)
        
        await page.get_content()
        
        # Lấy danh sách subpage từ main page này
        subpage_urls = await extract_subpage_urls(page)
        
        logger.info(f"Tìm thấy {len(subpage_urls)} subpage từ {url}")
        
        if len(subpage_urls) > 0:
            # Cào các subpage với giới hạn 10 subpage đồng thời
            subpage_tasks = []
            for subpage_url in subpage_urls:
                task = scrape_subpage(subpage_url, subpage_semaphore, browser)
                subpage_tasks.append(task)
            
            # Chờ tất cả subpage hoàn thành
            subpage_results = await asyncio.gather(*subpage_tasks)
        else:
            subpage_results = []
        
        # Đóng browser khi hoàn thành
        await browser.stop()
        
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
    
    page_semaphore = asyncio.Semaphore(4)
    # Semaphore cho subpage - giới hạn 10 subpage đồng thời cho mỗi main page
    subpage_semaphore = asyncio.Semaphore(10)
    
    # Số trang cần thu thập
    end_page = 1
    
    main_urls = [f"{BASE_URL}{START}p{i}" for i in range(1, end_page + 1)]
    
    logger.info(f"Đang xử lý {len(main_urls)} main page: {main_urls}")
    
    # Tạo task cho mỗi main page
    tasks = []
    for url in main_urls:
        task = scrape_main_page(url, page_semaphore, subpage_semaphore)
        tasks.append(task)
    
    # Chạy tất cả các task với giới hạn 4 main page đồng thời
    all_results = await asyncio.gather(*tasks)

    logger.info(f"Đã hoàn thành cào dữ liệu từ {len(all_results)} main page")

    save_results_to_csv(all_results)

    # Trả về kết quả (tùy chọn, có thể lưu vào file hoặc xử lý thêm)
    return all_results


def save_results_to_csv(results):
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_path = RAW_DATA_DIR / f"batdongsan_raw_{timestamp}.csv"

    fieldnames = [
        "main_page_url",
        "subpage_url",
        "title",
        "content_length",
    ]

    rows = []
    for main_page in results:
        subpages = main_page.get("subpage_data", [])
        for subpage in subpages:
            rows.append(
                {
                    "main_page_url": main_page.get("main_page_url"),
                    "subpage_url": subpage.get("url"),
                    "title": subpage.get("title"),
                    "content_length": subpage.get("content_length"),
                }
            )

    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"Đã lưu dữ liệu raw vào: {output_path}")

if __name__ == "__main__":
    # Sử dụng nodriver.loop() thay vì asyncio.run()
    results = uc.loop().run_until_complete(main())
    
    # Ghi log một số thông tin tổng kết
    total_subpages = sum(result['subpage_count'] for result in results)
    logger.info(f"Tổng số subpage đã cào: {total_subpages}")
