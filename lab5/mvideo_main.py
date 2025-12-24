# lesson5_mvideo_main.py
import os
import re
from datetime import datetime
from urllib.parse import urljoin

import scrapy
from pymongo import MongoClient, ASCENDING
from scrapy_playwright.page import PageMethod

BASE_URL = "https://www.mvideo.ru/"

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "parsing_hw")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION_MVIDEO", "mvideo_trending")

PRICE_RE = re.compile(r"(\d[\d\s\xa0]*?)\s*руб", re.IGNORECASE)

def _to_int_price(s: str) -> int | None:
    if not s:
        return None
    s = s.replace("\xa0", " ").replace(" ", "")
    try:
        return int(s)
    except ValueError:
        return None

def extract_prices_from_html(html_text: str) -> list[int]:
    """
    Достаём цены по вхождениям 'NNN руб' из HTML.
    Обычно первая — текущая, вторая (если есть) — старая/перечёркнутая.
    """
    found = []
    for m in PRICE_RE.finditer(html_text):
        p = _to_int_price(m.group(1))
        if p is not None:
            found.append(p)
    # немного чистим дубли
    uniq = []
    for x in found:
        if not uniq or uniq[-1] != x:
            uniq.append(x)
    return uniq

class MongoPipeline:
    def open_spider(self, spider):
        self.client = MongoClient(MONGO_URI)
        self.col = self.client[MONGO_DB][MONGO_COLLECTION]
        self.col.create_index([("url", ASCENDING)], unique=True)
        self.col.create_index([("product_id", ASCENDING)])

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        item = dict(item)
        self.col.update_one(
            {"url": item["url"]},
            {"$set": item, "$setOnInsert": {"created_at": datetime.utcnow()}},
            upsert=True,
        )
        return item

class MvideoTrendingSpider(scrapy.Spider):
    name = "mvideo_trending"

    custom_settings = {
        # scrapy-playwright конфиг (официально используется DOWNLOAD_HANDLERS + AsyncioSelectorReactor) :contentReference[oaicite:1]{index=1}
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},

        "ITEM_PIPELINES": {__name__ + ".MongoPipeline": 300},
        "LOG_LEVEL": "INFO",
        "ROBOTSTXT_OBEY": True,
        "DOWNLOAD_TIMEOUT": 60,
    }

    def start_requests(self):
        yield scrapy.Request(
            BASE_URL,
            meta={
                "playwright": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_load_state", "networkidle"),
                    # прокрутка вниз, чтобы догрузились карусели/блоки
                    PageMethod("evaluate", "window.scrollTo(0, document.body.scrollHeight)"),
                    PageMethod("wait_for_timeout", 1200),
                    PageMethod("evaluate", "window.scrollTo(0, document.body.scrollHeight)"),
                    PageMethod("wait_for_timeout", 1200),
                    # ждём, что появятся ссылки на товары
                    PageMethod("wait_for_selector", 'a[href*="/products/"]', timeout=20000),
                ],
            },
            callback=self.parse_home,
        )

    def parse_home(self, response):
        # Ищем секцию, где есть заголовок "В тренде"
        section = response.xpath(
            '//*[self::section or self::div]'
            '[.//*[self::h1 or self::h2 or self::h3][contains(normalize-space(.), "В тренде")]]'
        )

        if section:
            root = section[0]
            self.logger.info('Found section "В тренде"')
        else:
            # fallback: если верстка поменялась/не успело догрузиться
            root = response
            self.logger.warning('Section "В тренде" not found, fallback to whole page')

        hrefs = root.xpath('.//a[contains(@href, "/products/")]/@href').getall()
        urls = []
        seen = set()
        for h in hrefs:
            u = urljoin(BASE_URL, h)
            if u in seen:
                continue
            seen.add(u)
            urls.append(u)

        # чтобы было что показать преподу быстро — ограничим разумным числом
        for u in urls[:30]:
            yield scrapy.Request(u, callback=self.parse_product)

    def parse_product(self, response):
        title = (response.xpath("normalize-space(//h1)") or "").get()
        url = response.url

        # product_id обычно в конце URL после последнего дефиса
        m = re.search(r"-([0-9]{6,})/?$", url)
        product_id = m.group(1) if m else None

        prices = extract_prices_from_html(response.text)
        current_price = prices[0] if len(prices) >= 1 else None
        old_price = prices[1] if len(prices) >= 2 else None

        yield {
            "source": "mvideo.ru",
            "collection": "В тренде",
            "title": title,
            "url": url,
            "product_id": product_id,
            "price_current_rub": current_price,
            "price_old_rub": old_price,
            "scraped_at": datetime.utcnow(),
            "project_tag": "lesson5_scrapy_mvideo_trending",
        }

if __name__ == "__main__":
    # удобный запуск одной командой:
    # python lesson5_mvideo_main.py
    from scrapy.cmdline import execute
    execute(["scrapy", "runspider", __file__])
