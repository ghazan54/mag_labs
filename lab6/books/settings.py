BOT_NAME = "books"

SPIDER_MODULES = ["books.spiders"]
NEWSPIDER_MODULE = "books.spiders"

ROBOTSTXT_OBEY = True
LOG_LEVEL = "INFO"

# Mongo pipeline
ITEM_PIPELINES = {
    "books.pipelines.MongoPipeline": 300
}

# -------- Splash интеграция (scrapy-splash) --------
SPLASH_URL = "http://localhost:8050"

DOWNLOADER_MIDDLEWARES = {
    "scrapy_splash.SplashCookiesMiddleware": 723,
    "scrapy_splash.SplashMiddleware": 725,
    "scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware": 810,
}

SPIDER_MIDDLEWARES = {
    "scrapy_splash.SplashDeduplicateArgsMiddleware": 100,
}

DUPEFILTER_CLASS = "scrapy_splash.SplashAwareDupeFilter"
HTTPCACHE_STORAGE = "scrapy_splash.SplashAwareFSCacheStorage"

DOWNLOAD_TIMEOUT = 90
RETRY_TIMES = 2
