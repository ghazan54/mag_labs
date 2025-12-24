# scrape_news_mongo.py
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from lxml import html

from mongo_utils import get_collection


BASE_URL = "https://lenta.ru/"
SOURCE_NAME = "lenta.ru"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; NewsParserHomework/1.0)"
}
TIMEOUT = 15


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso_datetime(dt_str: str) -> str | None:
    """
    Приводим к ISO-строке, если возможно.
    Lenta часто отдаёт time datetime="2025-12-24T10:15:00+03:00" или "...Z".
    """
    if not dt_str:
        return None
    s = dt_str.strip()

    # поддержка Z
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        # datetime.fromisoformat понимает +03:00
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return None


def fetch(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r.text


def extract_mainpage_items(main_html: str) -> list[dict]:
    """
    Достаём заголовок и ссылку с главной. XPath используется везде.
    Верстка может меняться, поэтому берём несколько XPath-кандидатов.
    """
    tree = html.fromstring(main_html)

    # Кандидаты на ссылки (берём те, что ведут на статьи /news/)
    # 1) якоря с href содержащим "/news/"
    link_nodes = tree.xpath('//a[contains(@href, "/news/")]')

    items = []
    seen_links = set()

    for a in link_nodes:
        href = a.get("href")
        if not href:
            continue

        link = urljoin(BASE_URL, href)

        # У Lenta много служебных ссылок, чуть фильтруем
        if "/news/" not in link:
            continue

        # Текст заголовка: либо текст внутри <a>, либо дочерних узлов
        title = " ".join(a.xpath(".//text()")).strip()
        title = " ".join(title.split())

        if not title:
            continue
        if link in seen_links:
            continue

        seen_links.add(link)
        items.append({"title": title, "link": link})

    # ограничим, чтобы не бомбить сайт (достаточно для ДЗ)
    return items[:30]


def extract_published_at(article_html: str) -> str | None:
    """
    Дата публикации — с страницы новости.
    Пробуем несколько XPath:
    - <time datetime="...">
    - meta property="article:published_time"
    - meta itemprop="datePublished"
    """
    tree = html.fromstring(article_html)

    # 1) time[@datetime]
    dt = tree.xpath("string(//time/@datetime)").strip()
    iso = parse_iso_datetime(dt)
    if iso:
        return iso

    # 2) og/article:published_time
    dt = tree.xpath('string(//meta[@property="article:published_time"]/@content)').strip()
    iso = parse_iso_datetime(dt)
    if iso:
        return iso

    # 3) schema.org datePublished
    dt = tree.xpath('string(//meta[@itemprop="datePublished"]/@content)').strip()
    iso = parse_iso_datetime(dt)
    if iso:
        return iso

    return None


def upsert_news(col, source: str, title: str, link: str, published_at: str | None) -> bool:
    """
    Вставляем только если новости ещё не было (уникальность по link).
    Возвращаем True если вставили, False если уже существовала.
    """
    doc = {
        "source": source,
        "title": title,
        "link": link,
        "published_at": published_at,
        "scraped_at": now_iso(),
    }

    # upsert через $setOnInsert — не перетираем старые записи
    res = col.update_one(
        {"link": link},
        {"$setOnInsert": doc},
        upsert=True
    )
    return res.upserted_id is not None


def main():
    col = get_collection()  # при желании передайте mongo_uri/db/collection

    main_html = fetch(BASE_URL)
    items = extract_mainpage_items(main_html)

    inserted = 0
    skipped = 0

    for i, it in enumerate(items, start=1):
        link = it["link"]
        title = it["title"]

        # берём published_at со страницы статьи
        published_at = None
        try:
            article_html = fetch(link)
            published_at = extract_published_at(article_html)
        except Exception:
            # не валим весь прогон из-за одной страницы
            published_at = None

        ok = upsert_news(col, SOURCE_NAME, title, link, published_at)
        if ok:
            inserted += 1
            print(f"[{i}] + inserted: {title}")
        else:
            skipped += 1
            print(f"[{i}] = exists:   {title}")

        # небольшой троттлинг (вежливо к сайту)
        time.sleep(0.2)

    print("\nDone.")
    print(f"Inserted: {inserted}")
    print(f"Skipped:  {skipped}")
    print(f"Total in DB: {col.count_documents({})}")


if __name__ == "__main__":
    main()
