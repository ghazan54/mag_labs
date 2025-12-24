# mongo_utils.py
from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection

DEFAULT_MONGO_URI = "mongodb://localhost:27017"
DEFAULT_DB_NAME = "news_db"
DEFAULT_COLLECTION = "news"


def get_collection(
    mongo_uri: str = DEFAULT_MONGO_URI,
    db_name: str = DEFAULT_DB_NAME,
    collection_name: str = DEFAULT_COLLECTION,
) -> Collection:
    client = MongoClient(mongo_uri)
    db = client[db_name]
    col = db[collection_name]

    col.create_index([("link", ASCENDING)], unique=True, name="uniq_link")

    col.create_index([("source", ASCENDING)], name="idx_source")
    col.create_index([("published_at", ASCENDING)], name="idx_published_at")

    return col
