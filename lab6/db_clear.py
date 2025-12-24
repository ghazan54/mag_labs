import os
from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "parsing_hw")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION_BOOKS", "books_labirint")

def main():
    client = MongoClient(MONGO_URI)
    col = client[MONGO_DB][MONGO_COLLECTION]

    res = col.delete_many({"project_tag": "lesson6_scrapy_splash_books"})
    print("Deleted:", res.deleted_count)
    print("Now total:", col.count_documents({}))

if __name__ == "__main__":
    main()
