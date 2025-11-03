from typing import Optional
import asyncio
import logging
import csv
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def scroll_page_slowly(
    page,
    steps: int = 6,
    step_distance: int = 600,
    delay: float = 0.6,
) -> None:
    """Scroll xuống từ từ để kích hoạt lazy-loading cho đến hết trang."""

    previous_height = 0
    max_attempts = steps
    attempt = 0
    
    while attempt < max_attempts:
        try:
            # Lấy chiều cao hiện tại của trang
            current_height = await page.evaluate("document.body.scrollHeight")
            
            # Nếu chiều cao không thay đổi, nghĩa là đã đến cuối trang
            if current_height == previous_height:
                logger.debug("Reached end of page")
                break
            
            previous_height = current_height
            
            # Scroll xuống
            try:
                await page.scroll_down(step_distance)
            except Exception as scroll_error:
                logger.debug(f"scroll_down fallback evaluate: {scroll_error}")
                try:
                    await page.evaluate(f"window.scrollBy(0, {step_distance});")
                except Exception as eval_error:
                    logger.debug(f"window.scrollBy failed: {eval_error}")
                    break
            
            await asyncio.sleep(delay)
            attempt += 1
            
        except Exception as error:
            logger.debug(f"Error during scrolling: {error}")
            break

    try:
        await page.evaluate("window.scrollTo(0, 0);")
    except Exception as error:
        logger.debug(f"Could not scroll back to top: {error}")



async def wait_for_content_load(
    page,
    scroll_steps: int = 6,
    scroll_delay: float = 0.6,
) -> None:
    """Đợi trang load hoàn toàn và kích hoạt lazy loading."""

    try:
        # Cuộn trang từ từ để kích hoạt lazy loading
        await scroll_page_slowly(page, steps=scroll_steps, delay=scroll_delay)
    except Exception as error:
        logger.debug(f"scroll_page_slowly failed: {error}")

    # Chờ cho phần tử có class 're__main-content' xuất hiện bằng JavaScript
    try:
        await page.evaluate("""
            () => new Promise((resolve) => {
                const element = document.querySelector('.re__main-content');
                if (element) {
                    resolve();
                } else {
                    const observer = new MutationObserver((mutations) => {
                        mutations.forEach((mutation) => {
                            mutation.addedNodes.forEach((node) => {
                                if (node.nodeType === 1 && 
                                    (node.classList && node.classList.contains('re__main-content')) ||
                                    (node.querySelector && node.querySelector('.re__main-content'))) {
                                    observer.disconnect();
                                    resolve();
                                }
                            });
                        });
                    });
                    observer.observe(document.body, {
                        childList: true,
                        subtree: true
                    });
                }
            })
        """, await_promise=True)
    except Exception as error:
        logger.debug(f"Waiting for re__main-content failed: {error}")

    await asyncio.sleep(3.0)


async def text_from_selector(page, selector: str, attempts: int = 3, delay: float = 3) -> Optional[str]:
    """
    Extract text from selector using JavaScript to avoid RemoteObject issues.
    Includes retry logic with delay for asynchronously loaded content.
    """
    for attempt in range(attempts):
        try:
            # Use JavaScript to extract text directly in browser
            js_code = f"""
            (function() {{
                const selector = {repr(selector)};
                const element = document.querySelector(selector);
                
                if (element) {{
                    const text = element.textContent || element.innerText || '';
                    const trimmed = text.trim();
                    return trimmed || null;
                }}
                
                return null;
            }})();
            """
            
            result = await page.evaluate(js_code, return_by_value=True)
            
            # Handle RemoteObject fallback
            if hasattr(result, 'value'):
                result = result.value
            
            if result:
                return str(result)
                
        except Exception as e:
            logger.debug(f"Attempt {attempt+1} failed for selector '{selector}': {e}")
            await page.reload()
            
        if attempt < attempts - 1:
            await page.reload()
            await asyncio.sleep(delay)

    logger.warning(f"Not found selector '{selector}' after {attempts} attempts for page {page.url}")
    return None


async def extract_value_from_specs(page, label: str, default: str = "") -> str:
    """
    Extract value from specs section using JavaScript to avoid CBOR stack limit errors.
    This approach processes data in browser and returns only the needed value.
    """
    try:
        # Build JavaScript code with embedded parameters
        # nodriver only accepts JavaScript string, not function with parameters
        # IMPORTANT: Must return string, not undefined/null to avoid RemoteObject
        js_code = f"""
        (function() {{
            const label = {repr(label)};
            const defaultValue = {repr(default)};
            const specItems = document.querySelectorAll('div.re__pr-specs-content-item');
            
            for (const item of specItems) {{
                const titleElement = item.querySelector('span.re__pr-specs-content-item-title');
                if (titleElement) {{
                    const titleText = titleElement.textContent.trim();
                    if (titleText && titleText.toLowerCase().includes(label.toLowerCase())) {{
                        const valueElement = item.querySelector('span.re__pr-specs-content-item-value');
                        if (valueElement) {{
                            const value = valueElement.textContent.trim();
                            return value ? value : defaultValue;
                        }}
                    }}
                }}
            }}
            
            return defaultValue;
        }})()
        """
        
        result = await page.evaluate(js_code, return_by_value=True)
        # Ensure we return string, not RemoteObject or None
        if result is None or (hasattr(result, 'type_') and result.type_ == 'string'):
            return default
        return str(result) if result else default
        
    except Exception as e:
        logger.warning(f"Not found specs section for '{label}': {e} for page {page.url}")
        return default


async def extract_value_from_project_card(page, icon_class: str, default: str = "", max_retries: int = 3) -> str:
    """
    Extract value from project card using JavaScript to avoid CBOR stack limit errors.
    Includes retry logic with page reload if default value is returned.
    """
    for attempt in range(max_retries):
        try:
            js_code = f"""
            (function() {{
                const iconClass = {repr(icon_class)};
                const defaultValue = {repr(default)};
                const items = document.querySelectorAll('span.re__prj-card-config-value');
                
                for (const item of items) {{
                    const icon = item.querySelector('i.' + iconClass);
                    if (icon) {{
                        const valueElement = item.querySelector('span.re__long-text');
                        if (valueElement) {{
                            const value = valueElement.textContent.trim();
                            return value ? value : defaultValue;
                        }}
                    }}
                }}
                
                return defaultValue;
            }})()
            """
            
            result = await page.evaluate(js_code, return_by_value=True)
            # Ensure we return string, not RemoteObject or None
            if result is None or (hasattr(result, 'type_') and result.type_ == 'string'):
                result_str = default
            else:
                result_str = str(result) if result else default
            
            # If we got a non-default value, return it
            if result_str != default:
                return result_str
            
            # If we got default value and have retries left, reload and retry
            if attempt < max_retries - 1:
                logger.debug(f"Got default value for icon '{icon_class}', reloading page (attempt {attempt + 1}/{max_retries})")
                await page.reload()
                await wait_for_content_load(page)
            else:
                logger.warning(f"Cannot load project card items '{icon_class}' after {max_retries} attempts for page {page.url}")
                return default
            
        except Exception as e:
            if attempt < max_retries - 1:
                logger.debug(f"Error extracting project card value '{icon_class}': {e}, retrying (attempt {attempt + 1}/{max_retries})")
                await page.reload()
                await wait_for_content_load(page)
            else:
                logger.warning(f"Cannot load project card items '{icon_class}': {e} for page {page.url}")
                return default
    
    return default


async def extract_value_from_post_card(page, label: str, default: str = "", max_retries: int = 3) -> str:
    """
    Extract value from post card using JavaScript to avoid CBOR stack limit errors.
    Includes retry logic for cases where value might load asynchronously.
    """
    for attempt in range(max_retries):
        try:
            js_code = f"""
            (function() {{
                const label = {repr(label)};
                const items = document.querySelectorAll('div.re__pr-short-info-item.js__pr-config-item');
                
                for (const item of items) {{
                    const titleElement = item.querySelector('span.title');
                    if (titleElement) {{
                        const titleText = titleElement.textContent.trim();
                        if (titleText && titleText.toLowerCase().includes(label.toLowerCase())) {{
                            const valueElement = item.querySelector('span.value');
                            if (valueElement) {{
                                const valueText = valueElement.textContent.trim();
                                if (valueText) {{
                                    return valueText;
                                }}
                            }}
                        }}
                    }}
                }}
                
                return ""; // Return empty string instead of null to avoid RemoteObject
            }})()
            """
            
            result = await page.evaluate(js_code, return_by_value=True)
            
            # Ensure we handle RemoteObject properly
            if result is None or (hasattr(result, 'type_') and result.type_ == 'string'):
                result_str = default
            else:
                result_str = str(result) if result else ""
            
            if result_str:
                return result_str
            
            # If value is empty and we have retries left, wait and retry
            if attempt < max_retries - 1:
                logger.debug(f"Empty value for '{label}', retrying (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(0.5)
            else:
                return default
                
        except Exception as e:
            logger.warning(f"Not found post card items '{label}': {e} for page {page.url} (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                await asyncio.sleep(0.5)
                continue
            return default

    return default

def save_results_to_csv(results):
    RAW_DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_path = RAW_DATA_DIR / f"batdongsan_raw_{timestamp}.csv"

    fieldnames = [
        "main_page_url",
        "subpage_url",
        "title",
        "address",
        "price",
        "area",
        "house_direction",
        "balcony_direction",
        "facade",
        "legal",
        "furniture",
        "number_bedroom",
        "number_bathroom",
        "number_floor",
        "way_in",
        "project_name",
        "project_status",
        "project_investor",
        "post_id",
        "post_start_time",
        "post_end_time",
        "post_type",
        "source",
        "crawled_at"
    ]

    rows = []
    for main_page in results:
        subpages = main_page.get("subpage_data", [])
        for subpage in subpages:
            row = {
                "main_page_url": main_page.get("main_page_url", ""),
                "subpage_url": subpage.get("url", ""),
                "title": subpage.get("title", ""),
                "address": subpage.get("address", ""),
                "price": subpage.get("price", ""),
                "area": subpage.get("area", ""),
                "house_direction": subpage.get("house_direction", ""),
                "balcony_direction": subpage.get("balcony_direction", ""),
                "facade": subpage.get("facade", ""),
                "legal": subpage.get("legal", ""),
                "furniture": subpage.get("furniture", ""),
                "number_bedroom": subpage.get("number_bedroom", ""),
                "number_bathroom": subpage.get("number_bathroom", ""),
                "number_floor": subpage.get("number_floor", ""),
                "way_in": subpage.get("way_in", ""),
                "project_name": subpage.get("project_name", ""),
                "project_status": subpage.get("project_status", ""),
                "project_investor": subpage.get("project_investor", ""),
                "post_id": subpage.get("post_id", ""),
                "post_start_time": subpage.get("post_start_time", ""),
                "post_end_time": subpage.get("post_end_time", ""),
                "post_type": subpage.get("post_type", ""),
                "source": subpage.get("source", ""),
                "crawled_at": subpage.get("crawled_at", "")
            }
            rows.append(row)

    with output_path.open("w", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"Đã lưu dữ liệu raw vào: {output_path}")
