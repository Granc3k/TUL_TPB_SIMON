from pyspark import SparkConf, SparkContext
import re

conf = SparkConf() \
    .setMaster("spark://85b8a6a7d728:7077") \
    .setAppName("WordCount") \
    .set("spark.executor.memory", "4g") \
    .set("spark.driver.memory", "4g") \
    .set("spark.rpc.message.maxSize", "2046")
sc = SparkContext(conf = conf)

def normalizeText(text):
    lowercased = text.lower()
    cleaned = re.sub(r'[^\w\s]', '', lowercased)
    return cleaned

print("Loading data...")
input = sc.textFile("/files/data/idnes_articles_text.txt")
print("Data loaded.")
print("---------------")
print("Processing data...")

words = input.flatMap(lambda x: normalizeText(x).split())

filteredWords = words.filter(lambda x: len(x) >= 6)

wordCounts = filteredWords.map(lambda x: (x, 1))

wordCountsReduced = wordCounts.reduceByKey(lambda x, y: x + y)

sortedWordCounts = wordCountsReduced.map(lambda x: (x[1], x[0])).sortByKey(False)

results = sortedWordCounts.take(20)

for count, word in results:
    print(f"{word}: {count}")
