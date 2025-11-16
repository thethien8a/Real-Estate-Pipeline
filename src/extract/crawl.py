import asyncio
import logging
import os
import shutil
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
from typing import Optional, List, Tuple
from nodriver.core.connection import ProtocolException
from config import get_subpage_semaphore, CrawlConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_URL = "https://batdongsan.com.vn"
START = "/nha-dat-ban/"


# def detect_chrome_path() -> str:
#     """
#     Best-effort detection of the Chrome executable across CI runners and local environments.
#     Preference order:
#     1. CHROME_PATH env (absolute path or command on PATH)
#     2. Common binary names on PATH
#     3. Known absolute install locations (Linux + Windows)
#     """
#     preferred_commands = [
#         os.getenv("CHROME_PATH"),
#         "google-chrome-stable",
#         "google-chrome",
#         "chromium-browser",
#     ]
#     preferred_paths = [
#         "/usr/bin/google-chrome-stable",
#         "/usr/bin/google-chrome",
#         "/usr/local/bin/google-chrome",
#         "/opt/google/chrome/google-chrome",
#     ]

#     for candidate in preferred_commands:
#         if not candidate:
#             continue
#         resolved = shutil.which(candidate)
#         if resolved:
#             return resolved
#         if Path(candidate).is_file():
#             return candidate

#     for candidate in preferred_paths:
#         if Path(candidate).is_file():
#             return candidate

#     raise FileNotFoundError(
#         "Không tìm thấy Chrome. Hãy cài đặt google-chrome-stable trong CI hoặc đặt CHROME_PATH."
#     )

async def start_browser():
    try:
        # chrome_path = detect_chrome_path()
        # logger.info("Using Chrome executable at %s", chrome_path)
        browser = await uc.start(
            headless=True,
            sandbox=False,
            browser_executable_path="/opt/hostedtoolcache/setup-chrome/chrome/stable/x64/chrome", # Lấy cái này ở log github actions nhá
            browser_args=CrawlConfig.BROWSER_ARGS
        )
        if not getattr(browser, "connection", None):
            raise RuntimeError("Browser started but connection is None")
        logger.info("Browser started successfully")
        return browser
    except Exception as exc:
        logger.error(f"Failed to start browser: {exc}")
        raise



async def apply_stealth_and_wait(page):
    await page.evaluate(CrawlConfig.STEALTH_EVASION_SCRIPT)
    await asyncio.sleep(5)


def build_main_page_payload(main_page_results: dict):
    payload = []
    for main_url, subpages in main_page_results.items():
        payload.append(
            {
                "main_page_url": main_url,
                "subpage_count": len(subpages),
                "subpage_data": subpages,
            }
        )
    return payload


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
    

async def scrape_subpage(main_page_url: str, url: str, subpage_semaphore: asyncio.Semaphore, browser):
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

        item["main_page_url"] = main_page_url
        logger.info(f"  Đã hoàn thành subpage: {url}")
        return item


async def collect_subpage_urls(browser, main_url: str) -> List[str]:
    """
    Mở main page và thu thập toàn bộ subpage URL
    """
    logger.info(f"Đang thu thập subpage từ main page: {main_url}")
    page = await browser.get(main_url, new_tab=True)
    try:
        await apply_stealth_and_wait(page)
        subpage_urls = await extract_subpage_urls(page)
        logger.info(f"Tìm thấy {len(subpage_urls)} subpage từ {main_url}")
        return subpage_urls
    except Exception as exc:
        logger.warning(f"Không thể thu thập subpage từ {main_url}: {exc}")
        return []
    finally:
        try:
            await page.close()
        except Exception as close_error:
            logger.debug(f"Không thể đóng main page {main_url}: {close_error}")

async def main():
    """
    Hàm chính để chạy toàn bộ quá trình cào dữ liệu
    """
    logger.info("Bắt đầu quá trình cào dữ liệu")

    subpage_semaphore = get_subpage_semaphore()

    start_page = CrawlConfig.START_PAGE
    end_page = CrawlConfig.END_PAGE

    main_urls = [f"{BASE_URL}{START}p{i}" for i in range(start_page, end_page + 1)]
    if not main_urls:
        logger.warning("Không có main page nào để xử lý")
        return []

    logger.info(f"Đang xử lý {len(main_urls)} main page: {main_urls}")

    browser = await start_browser()
    main_page_results = {url: [] for url in main_urls}
    try:
        all_subpage_refs: List[Tuple[str, str]] = []
        for url in main_urls:
            subpage_urls = await collect_subpage_urls(browser, url)
            if not subpage_urls:
                logger.info(f"Không tìm thấy subpage nào cho {url}")
            for subpage_url in subpage_urls:
                all_subpage_refs.append((url, subpage_url))

        logger.info(f"Tổng cộng {len(all_subpage_refs)} subpage sẽ được xử lý")

        if not all_subpage_refs:
            logger.warning("Không tìm thấy subpage nào để cào.")
            final_payload = build_main_page_payload(main_page_results)
            return final_payload

        subpage_tasks = [
            scrape_subpage(main_url, sub_url, subpage_semaphore, browser)
            for main_url, sub_url in all_subpage_refs
        ]

        subpage_results_raw = await asyncio.gather(*subpage_tasks, return_exceptions=True)
        for result in subpage_results_raw:
            if isinstance(result, Exception):
                logger.warning(f"Subpage task exception: {result}")
            else:
                parent_url = result.get("main_page_url")
                if parent_url in main_page_results:
                    main_page_results[parent_url].append(result)
                else:
                    logger.debug(f"Không tìm thấy main_page_url cho subpage: {result}")

        final_payload = build_main_page_payload(main_page_results)
        save_results_to_csv(final_payload)
        logger.info("Đã hoàn thành cào dữ liệu với mô hình batch subpage")
        return final_payload
    finally:
        try:
            await browser.stop()
        except Exception as stop_error:
            logger.debug(f"Browser stop error: {stop_error}")


if __name__ == "__main__":
    results = uc.loop().run_until_complete(main())