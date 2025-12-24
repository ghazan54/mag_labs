import os
from pymongo import MongoClient, DESCENDING

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "parsing_hw")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION_BOOKS", "books_labirint")

def main():
    client = MongoClient(MONGO_URI)
    col = client[MONGO_DB][MONGO_COLLECTION]

    print(f"Collection: {MONGO_DB}.{MONGO_COLLECTION}")
    print("Total docs:", col.count_documents({}))

    print("\nLast 10:")
    for doc in col.find({}, {"_id": 0}).sort("scraped_at", DESCENDING).limit(10):
        print("-" * 60)
        print("title:", doc.get("title"))
        print("authors:", doc.get("authors"))
        print("base:", doc.get("price_base"), "discount:", doc.get("price_discount"))
        print("rating:", doc.get("rating"))
        print("url:", doc.get("url"))

if __name__ == "__main__":
    main()
