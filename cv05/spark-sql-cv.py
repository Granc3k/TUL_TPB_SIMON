# made by Martin "Granc3k" Šimon
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

master = "spark://f562857efac5:7077"

spark = SparkSession.builder.master(master).appName("SparkSQL").getOrCreate()

people = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("/files/data/fakefriends-header.csv")
)

print("Here is our inferred schema:")
people.printSchema()

# Calc prumernych pratel - sort podle prumerneho poctu
avg_friends_by_age = (
    people.groupBy("age")
    .agg(avg("friends").alias("average_friends"))
    .orderBy("average_friends", ascending=False)
)
avg_friends_by_age.show()

spark.stop()
