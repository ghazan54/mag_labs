import os
from pymongo import MongoClient, ASCENDING


class MongoPipeline:
    def __init__(self):
        self.mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
        self.mongo_db = os.getenv("MONGO_DB", "parsing_hw")
        self.mongo_collection = os.getenv("MONGO_COLLECTION_BOOKS", "books_labirint")

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def open_spider(self, spider):
        self.client = MongoClient(self.mongo_uri)
        self.col = self.client[self.mongo_db][self.mongo_collection]
        self.col.create_index([("url", ASCENDING)], unique=True)

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        doc = dict(item)
        # upsert по URL, чтобы не дублировать
        self.col.update_one(
            {"url": doc["url"]},
            {"$set": doc},
            upsert=True
        )
        return item
