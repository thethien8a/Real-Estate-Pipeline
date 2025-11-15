import asyncio
import os
import tempfile
from pathlib import Path


class CrawlConfig:
    
    # Semaphore cho subpage - giới hạn số lượng subpage được xử lý đồng thời cho mỗi main page
    SUBPAGE_SEMAPHORE_LIMIT = 20
    
    # Trang bắt đầu thu thập
    START_PAGE = 1
    
    # Trang kết thúc thu thập
    END_PAGE = 3

    
    BROWSER_EXECUTABLE = "/usr/bin/google-chrome-stable"
    BROWSER_ARGS = [
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-setuid-sandbox",
        "--window-size=1366,768",
        "--lang=vi-VN",
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "--disable-software-rasterizer",
        "--no-first-run",
    ]

    USER_DATA_DIR = os.getenv("CHROME_USER_DATA_DIR")
    TEMP_PROFILE_BASE_DIR = Path(tempfile.gettempdir())
    TEMP_PROFILE_PREFIX = "nodriver-profile-"

    STEALTH_EVASION_SCRIPT = """
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
        return originalToDataURL.apply(this, arguments);
    };
    """

def get_subpage_semaphore():
    """Trả về một instance của Semaphore cho subpage."""
    return asyncio.Semaphore(CrawlConfig.SUBPAGE_SEMAPHORE_LIMIT)