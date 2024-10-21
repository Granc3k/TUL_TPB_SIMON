from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("spark://2db24b67c774:7077").setAppName("RatingsHistogram")
# conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("/files/data/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
