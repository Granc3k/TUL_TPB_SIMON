from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("spark://85b8a6a7d728:7077").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalizeText(text):
    lowercased = text.lower()
    cleaned = re.sub(r'[^\w\s]', '', lowercased)
    return cleaned

input = sc.textFile("/files/data/book.txt")

words = input.flatMap(lambda x: normalizeText(x).split())

wordCounts = words.map(lambda x: (x, 1))

wordCountsReduced = wordCounts.reduceByKey(lambda x, y: x + y)

sortedWordCounts = wordCountsReduced.map(lambda x: (x[1], x[0])).sortByKey(False)

results = sortedWordCounts.take(20)

for count, word in results:
    print(f"{word}: {count}")
