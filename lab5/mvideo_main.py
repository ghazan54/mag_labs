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


def abort_request(request) -> bool:
    """
    Ускоряем загрузку: не тянем картинки/шрифты/медиа.
    """
    return request.resource_type in {"image", "font", "media"}


def _to_int_price(s: str) -> int | None:
    if not s:
        return None
    s = s.replace("\xa0", " ").replace(" ", "")
    try:
        return int(s)
    except ValueError:
        return None


def extract_prices_from_html(html_text: str) -> list[int]:
    found = []
    for m in PRICE_RE.finditer(html_text):
        p = _to_int_price(m.group(1))
        if p is not None:
            found.append(p)
    
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
        self.col.create_index([("scraped_at", ASCENDING)])

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        item = dict(item)
        # upsert по url, чтобы при повторах не плодить дубли
        self.col.update_one(
            {"url": item["url"]},
            {"$set": item, "$setOnInsert": {"created_at": datetime.utcnow()}},
            upsert=True,
        )
        return item


class MvideoTrendingSpider(scrapy.Spider):
    name = "mvideo_trending"

    custom_settings = {
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},

        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 60000,
        "PLAYWRIGHT_DEFAULT_TIMEOUT": 60000,

        "PLAYWRIGHT_ABORT_REQUEST": abort_request,

        "ITEM_PIPELINES": {__name__ + ".MongoPipeline": 300},
        "LOG_LEVEL": "INFO",
        "ROBOTSTXT_OBEY": True,
        "DOWNLOAD_TIMEOUT": 90,
        "RETRY_TIMES": 2,
    }

    async def start(self):
        yield scrapy.Request(
            BASE_URL,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_load_state", "domcontentloaded"),
                    PageMethod("wait_for_timeout", 1500),

                    PageMethod("evaluate", "window.scrollTo(0, document.body.scrollHeight)"),
                    PageMethod("wait_for_timeout", 1500),
                    PageMethod("evaluate", "window.scrollTo(0, document.body.scrollHeight)"),
                    PageMethod("wait_for_timeout", 1500),

                    PageMethod("wait_for_selector", 'a[href*="/products/"]', timeout=60000),
                ],
            },
            callback=self.parse_home,
            errback=self.errback_close_page,
            dont_filter=True,
        )

    async def errback_close_page(self, failure):
        """
        Если Playwright упал, обязательно закрываем page, иначе будут утечки.
        """
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
        self.logger.error("Request failed: %r", failure)

    async def parse_home(self, response):
        page = response.meta.get("playwright_page")
        if page:
            await page.close()

        section = response.xpath(
            '//*[self::section or self::div]'
            '[.//*[self::h1 or self::h2 or self::h3][contains(normalize-space(.), "В тренде")]]'
        )
        root = section[0] if section else response
        if section:
            self.logger.info('Found section "В тренде"')
        else:
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

        self.logger.info("Product links extracted: %d", len(urls))

        # Ограничим число товаров для ДЗ
        for u in urls[:30]:
            yield scrapy.Request(u, callback=self.parse_product)

    def parse_product(self, response):
        title = response.xpath("normalize-space(//h1)").get() or ""
        url = response.url

        # product_id часто в конце после дефиса
        m = re.search(r"-([0-9]{6,})/?$", url)
        product_id = m.group(1) if m else None

        prices = extract_prices_from_html(response.text)
        current_price = prices[0] if len(prices) >= 1 else None
        old_price = prices[1] if len(prices) >= 2 else None

        yield {
            "source": "mvideo.ru",
            "collection": "В тренде",
            "title": title.strip(),
            "url": url,
            "product_id": product_id,
            "price_current_rub": current_price,
            "price_old_rub": old_price,
            "scraped_at": datetime.utcnow(),
            "project_tag": "scrapy_mvideo_trending",
        }


if __name__ == "__main__":
    # удобный запуск одним файлом
    from scrapy.cmdline import execute
    execute(["scrapy", "runspider", __file__])
