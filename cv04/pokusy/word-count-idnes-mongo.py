from pyspark import SparkConf, SparkContext
import re
from pymongo import MongoClient

def main():
    # Připojení k MongoDB
    client = MongoClient('mongodb://mongodb-container:27017/')
    db = client['<mongodb-container>']
    collection = db['articles']

    # Nastavení Spark kontextu pro lokální běh
    conf = SparkConf() \
        .setMaster("spark://36d524166078:7077") \
        .setAppName("IDNESWordCount") \
        .set("spark.driver.memory", "4g") \
        .set("spark.executor.memory", "4g")

    sc = SparkContext(conf=conf)

    # Načtení článků z MongoDB
    articles = collection.find({}, {"content": 1})

    # Zpracování textu
    input_rdd = sc.parallelize([article['content'] for article in articles])
    words = input_rdd.flatMap(lambda x: normalize_text(x).split()) \
                     .filter(lambda x: len(x) >= 6)
    word_counts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    sorted_word_counts = word_counts.map(lambda x: (x[1], x[0])).sortByKey(False)

    # Výpis výsledků
    results = sorted_word_counts.take(20)
    for count, word in results:
        print(f"{word}: {count}")

def normalize_text(text):
    return re.sub(r'[^\w\s]', '', text.lower())

if __name__ == "__main__":
    main()
