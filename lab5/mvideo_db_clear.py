# lesson5_mvideo_db_clear.py
import os
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "parsing_hw")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION_MVIDEO", "mvideo_trending")

def main():
    client = MongoClient(MONGO_URI)
    col = client[MONGO_DB][MONGO_COLLECTION]

    res = col.delete_many({"project_tag": "lesson5_scrapy_mvideo_trending"})
    print(f"Deleted: {res.deleted_count} documents from {MONGO_DB}.{MONGO_COLLECTION}")

if __name__ == "__main__":
    main()
