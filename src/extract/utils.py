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


async def wait_for_selector(page, selector: str, *, attempts: int = 3, delay: float = 1.0, log_label: Optional[str] = None):
    label = log_label or selector
    for attempt in range(1, attempts + 1):
        try:
            element = await page.query_selector(selector)
            if element:
                return element
        except Exception as error:
            logger.debug(f"Attempt {attempt} failed for selector '{label}': {error}")
        if attempt < attempts:
            await asyncio.sleep(delay)
    logger.warning(f"Failed to locate selector '{label}' after {attempts} attempts")
    return None


def text_from_element(element) -> Optional[str]:
    if element is None:
        return None

    try:
        text = element.text
    except Exception:
        return None

    if text is None:
        return None

    stripped = text.strip()
    return stripped or None


async def text_from_selector(page, selector: str, attempts: int = 3, delay: float = 0.5) -> Optional[str]:
    element = await wait_for_selector(page, selector, attempts=attempts, delay=delay)
    return text_from_element(element)


async def extract_value_from_specs(page, label: str, default: str = "") -> str:
    await wait_for_selector(page, "div.re__pr-specs-content-item")
    try:
        spec_items = await page.select_all("div.re__pr-specs-content-item")
    except Exception as e:
        logger.warning(f"Cannot load specs section for '{label}': {e}")
        return default

    for item in spec_items:
        try:
            title_element = await item.query_selector("span.re__pr-specs-content-item-title")
            title_text = text_from_element(title_element)
            if title_text and label.lower() in title_text.lower():
                value_element = await item.query_selector("span.re__pr-specs-content-item-value")
                value_text = text_from_element(value_element)
                return value_text or default
        except Exception as inner_error:
            logger.debug(f"Skip spec item for '{label}' due to error: {inner_error}")
            continue

    return default


async def extract_value_from_project_card(page, icon_class: str, default: str = "") -> str:
    await wait_for_selector(page, "span.re__prj-card-config-value")
    try:
        items = await page.select_all("span.re__prj-card-config-value")
    except Exception as e:
        logger.warning(f"Cannot load project card items '{icon_class}': {e}")
        return default

    for item in items:
        try:
            icon = await item.query_selector(f"i.{icon_class}")
            if icon:
                value_element = await item.query_selector("span.re__long-text")
                value_text = text_from_element(value_element)
                return value_text or default
        except Exception as inner_error:
            logger.debug(f"Skip project card item '{icon_class}' due to error: {inner_error}")
            continue

    return default


async def extract_value_from_post_card(page, label: str, default: str = "") -> str:
    await wait_for_selector(page, "div.re__pr-short-info-item.js__pr-config-item")
    try:
        items = await page.select_all("div.re__pr-short-info-item.js__pr-config-item")
    except Exception as e:
        logger.warning(f"Cannot load post card items '{label}': {e}")
        return default

    for item in items:
        try:
            title_element = await item.query_selector("span.title")
            title_text = text_from_element(title_element)
            if title_text and label.lower() in title_text.lower():
                value_element = await item.query_selector("span.value")
                value_text = text_from_element(value_element)
                return value_text or default
        except Exception as inner_error:
            logger.debug(f"Skip post card item '{label}' due to error: {inner_error}")
            continue

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

    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f"Đã lưu dữ liệu raw vào: {output_path}")