import scrapy


class BookItem(scrapy.Item):
    source = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    authors = scrapy.Field()          # list[str]
    price_base = scrapy.Field()       # int | None
    price_discount = scrapy.Field()   # int | None
    rating = scrapy.Field()           # float | None
    scraped_at = scrapy.Field()
    project_tag = scrapy.Field()
