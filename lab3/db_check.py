# db_check.py
from mongo_utils import get_collection


def main():
    col = get_collection()

    total = col.count_documents({})
    print(f"Collection: {col.full_name}")
    print(f"Total docs: {total}\n")

    print("Indexes:")
    for idx in col.list_indexes():
        print(f" - {idx.get('name')}: {idx.get('key')} (unique={idx.get('unique', False)})")

    print("\nLast 10 docs:")
    cursor = col.find(
        {},
        {"_id": 0, "source": 1, "title": 1, "link": 1, "published_at": 1, "scraped_at": 1},
    ).sort("scraped_at", -1).limit(10)

    for n, doc in enumerate(cursor, start=1):
        print(f"\n#{n}")
        print(f"source:       {doc.get('source')}")
        print(f"title:        {doc.get('title')}")
        print(f"link:         {doc.get('link')}")
        print(f"published_at: {doc.get('published_at')}")
        print(f"scraped_at:   {doc.get('scraped_at')}")


if __name__ == "__main__":
    main()
