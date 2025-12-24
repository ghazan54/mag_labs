import re
from datetime import datetime
from urllib.parse import urljoin

import scrapy
from scrapy_splash import SplashRequest

from books.items import BookItem


BASE = "https://www.labirint.ru"
START_URL = "https://www.labirint.ru/rating/"


def to_int(num_str: str) -> int | None:
    if not num_str:
        return None
    s = num_str.replace("\xa0", " ").replace(" ", "").strip()
    try:
        return int(s)
    except ValueError:
        return None


def extract_prices(text: str) -> tuple[int | None, int | None]:
    """
    На страницах Labirint обычно встречается: "5 935" и "14 838" рядом.
    Считаем:
      price_discount = первая (со скидкой)
      price_base     = вторая (без скидки)
    """
    nums = re.findall(r"(\d[\d\s\xa0]{1,10})", text or "")
    nums = [to_int(x) for x in nums]
    nums = [x for x in nums if x is not None and x > 0]
    if len(nums) >= 2:
        return nums[1], nums[0]  # base, discount
    if len(nums) == 1:
        return None, nums[0]
    return None, None


def extract_rating(text: str) -> float | None:
    """
    На странице часто встречается вроде: "Рейтинг 5(5 оценок)" или "4.4"
    """
    if not text:
        return None
    m = re.search(r"([0-5](?:[.,]\d)?)", text)
    if not m:
        return None
    return float(m.group(1).replace(",", "."))


class LabirintBooksSpider(scrapy.Spider):
    name = "labirint_books"

    # небольшой Lua-скрипт: просто отрендерить страницу
    splash_args = {
        "wait": 1.0,
        "timeout": 60,
        "images": 0,
        "resource_timeout": 15,
    }

    def start_requests(self):
        yield SplashRequest(
            url=START_URL,
            callback=self.parse_list,
            endpoint="render.html",
            args=self.splash_args,
        )

    def parse_list(self, response: scrapy.http.Response):
        # ссылки на книги
        hrefs = response.xpath('//a[contains(@href, "/books/")]/@href').getall()
        seen = set()
        urls = []
        for h in hrefs:
            u = urljoin(BASE, h)
            if u in seen:
                continue
            seen.add(u)
            urls.append(u)

        # чтобы быстро показать преподу — ограничим
        for u in urls[:50]:
            yield SplashRequest(
                url=u,
                callback=self.parse_book,
                endpoint="render.html",
                args=self.splash_args,
            )

    def parse_book(self, response: scrapy.http.Response):
        item = BookItem()
        item["source"] = "labirint.ru"
        item["url"] = response.url
        item["scraped_at"] = datetime.utcnow()
        item["project_tag"] = "lesson6_scrapy_splash_books"

        # Название
        item["title"] = (response.xpath("normalize-space(//h1)").get() or "").strip()

        # Авторы: на Labirint обычно ссылки вида /authors/ID/
        authors = response.xpath('//a[contains(@href, "/authors/")]/text()').getall()
        authors = [a.strip() for a in authors if a and a.strip()]
        item["authors"] = list(dict.fromkeys(authors))  # уникальные, сохраняя порядок

        # Рейтинг: пробуем вытащить из видимой зоны рядом со словом "Рейтинг"
        rating_block = response.xpath(
            'normalize-space(//*[contains(., "Рейтинг")][1])'
        ).get()
        item["rating"] = extract_rating(rating_block)

        # Цены: берём кусок текста около скидки/цены (часто вверху страницы)
        # fallback: если не нашли — ищем любые числа в тексте страницы (хуже, но работает)
        price_zone = response.xpath(
            'normalize-space((//*[contains(., "Скидка") or contains(., "%") or contains(., "Вы сэкономите")])[1])'
        ).get()
        if not price_zone:
            price_zone = response.xpath("normalize-space(//body)").get()

        base, discount = extract_prices(price_zone)
        item["price_base"] = base
        item["price_discount"] = discount

        yield item
