# db_clear.py
import sys
from mongo_utils import get_collection


def main():
    col = get_collection()

    # Использование:
    #   python db_clear.py                -> удалить всё
    #   python db_clear.py lenta.ru       -> удалить только по source=lenta.ru
    if len(sys.argv) == 1:
        res = col.delete_many({})
        print(f"Deleted ALL docs: {res.deleted_count}")
    else:
        source = sys.argv[1]
        res = col.delete_many({"source": source})
        print(f"Deleted docs for source='{source}': {res.deleted_count}")

    print(f"Total after очистки: {col.count_documents({})}")


if __name__ == "__main__":
    main()
