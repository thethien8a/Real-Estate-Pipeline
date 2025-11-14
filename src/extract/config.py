import asyncio


class CrawlConfig:
    # Semaphore cho main page - giới hạn số lượng main page được xử lý đồng thời
    PAGE_SEMAPHORE_LIMIT = 5
    
    # Semaphore cho subpage - giới hạn số lượng subpage được xử lý đồng thời cho mỗi main page
    SUBPAGE_SEMAPHORE_LIMIT = 10
    
    # Trang bắt đầu thu thập
    START_PAGE = 20
    
    # Trang kết thúc thu thập
    END_PAGE = 24


def get_page_semaphore():
    """Trả về một instance của Semaphore cho main page."""
    return asyncio.Semaphore(CrawlConfig.PAGE_SEMAPHORE_LIMIT)


def get_subpage_semaphore():
    """Trả về một instance của Semaphore cho subpage."""
    return asyncio.Semaphore(CrawlConfig.SUBPAGE_SEMAPHORE_LIMIT)