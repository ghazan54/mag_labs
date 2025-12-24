import os
from pymongo import MongoClient, DESCENDING

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "parsing_hw")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION_MVIDEO", "mvideo_trending")

def main():
    client = MongoClient(MONGO_URI)
    col = client[MONGO_DB][MONGO_COLLECTION]

    total = col.count_documents({})
    print(f"Collection: {MONGO_DB}.{MONGO_COLLECTION}")
    print(f"Total docs: {total}\n")

    print("Last 10 docs:")
    for doc in col.find({}, {"_id": 0}).sort("scraped_at", DESCENDING).limit(10):
        print("-" * 60)
        print(f"title:   {doc.get('title')}")
        print(f"url:     {doc.get('url')}")
        print(f"price:   {doc.get('price_current_rub')} (old: {doc.get('price_old_rub')})")
        print(f"scraped: {doc.get('scraped_at')}")

if __name__ == "__main__":
    main()
