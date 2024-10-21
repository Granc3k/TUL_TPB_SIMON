from pyspark import SparkConf, SparkContext
import re
import json

# Nastavení Spark konfigurace se zvýšenou pamětí a CPU jádry
conf = SparkConf() \
    .setMaster("spark://49207e712981:7077") \
    .setAppName("WordCount")
sc = SparkContext(conf = conf)

# Funkce pro normalizaci textu
def normalizeText(text):
    lowercased = text.lower()
    cleaned = re.sub(r'[^\w\s]', '', lowercased)  # odstranění interpunkce
    return cleaned

# Načtení JSON souboru do proměnné data
with open('/files/data/idnes_articles.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Vytvoření seznamu obsahu článků, kontrola, že 'content' existuje
articles_texts = [ ' '.join(article['content']) for article in data if 'content' in article]

# Převedení textů na RDD (rozparalelizování na úrovni článků)
content_rdd = sc.parallelize(articles_texts)

# Normalizace textu, rozdělení na slova a filtrace podle délky (min. 6 znaků)
words = content_rdd.flatMap(lambda text: normalizeText(text).split())
long_words = words.filter(lambda word: len(word) >= 6)

# Spočítání výskytů jednotlivých slov
wordCounts = long_words.map(lambda word: (word, 1)).reduceByKey(lambda x, y: x + y)

# Seřazení podle počtu výskytů (největší počet jako první)
sortedWordCounts = wordCounts.map(lambda x: (x[1], x[0])).sortByKey(False)

# Vybrání 20 nejčastějších slov
top_20_words = sortedWordCounts.take(20)

# Výpis výsledků
print("Nejčastější slova:")
for count, word in top_20_words:
    print(f"{word}: {count}")
