# made by Martin "Granc3k" Šimon
import json
from pymongo import MongoClient
import random


def load_data_to_mongo(json_file, db_name, collection_name):
    # Připojení k MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client[db_name]
    collection = db[collection_name]

    # Načtení dat z JSON souboru
    with open(json_file, "r", encoding="utf-8") as file:
        data = json.load(file)

    # Vložení dat do MongoDB kolekce
    collection.insert_many(data)

    return collection


def load_data_from_mongo(db_name, collection_name):
    client = MongoClient("mongodb://localhost:27017/")
    db = client[db_name]
    collection = db[collection_name]

    return collection


def get_random_article(collection):
    random_article = collection.aggregate([{"$sample": {"size": 1}}])
    for article in random_article:
        print("Náhodný článek:", article)


def get_total_articles(collection):
    total_articles = collection.count_documents({})
    print(f"Celkový počet článků: {total_articles}")


def get_avg_photos_per_article(collection):
    avg_photos = collection.aggregate(
        [
            {"$match": {"photo_count": {"$exists": True, "$type": "int"}}},
            {"$group": {"_id": None, "average_photos": {"$avg": "$photo_count"}}},
        ]
    )
    for result in avg_photos:
        print(f"Průměrný počet fotek na článek: {result['average_photos']:.2f}")


def get_articles_with_more_than_100_comments(collection):
    high_comment_articles = collection.count_documents({"comment_count": {"$gt": 100}})
    print(f"Počet článků s více než 100 komentáři: {high_comment_articles}")


def get_articles_by_category_in_2022(collection):
    category_count_2022 = collection.aggregate(
        [
            {"$match": {"time": {"$regex": "^2022"}}},
            {"$group": {"_id": "$category", "count": {"$sum": 1}}},
        ]
    )
    print("Počet článků v roce 2022 podle kategorií:")
    for result in category_count_2022:
        print(f"Kategorie: {result['_id']} - Počet článků: {result['count']}")


def main():
    json_file = "./cv02/data/filtered_data.json"
    db_name = "mongodb"
    collection_name = "articles"

    # collection = load_data_to_mongo(json_file, db_name, collection_name)
    collection = load_data_from_mongo(db_name, collection_name)

    get_random_article(collection)  # Vypíše jeden náhodný článek
    get_total_articles(collection)  # Vypíše celkový počet článků
    get_avg_photos_per_article(collection)  # Vypíše průměrný počet fotek na článek
    get_articles_with_more_than_100_comments(
        collection
    )  # Vypíše počet článků s více než 100 komentáři
    get_articles_by_category_in_2022(
        collection
    )  # Vypíše počet článků pro každou kategorii z roku 2022


if __name__ == "__main__":
    main()
