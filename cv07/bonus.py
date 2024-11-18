# made by Martin "Granc3k" Šimon
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, regexp_replace, lower

# settings pro SparkSession
spark = SparkSession.builder.appName("WordCountFromFiles").getOrCreate()

# Stream čtení dat z adresáře
lines = (
    spark.readStream.format("text")
    .option("path", "/files/data/bonus/")
    .option("maxFilesPerTrigger", 1)
    .load()
)

# Processing textov streamu
# 1. Transfer textu na malá písmena
# 2. Del interpunkce
# 3. Split textu na singl slova
words = lines.select(
    explode(
        split(lower(regexp_replace(col("value"), "[^a-zA-Z0-9\\s]", "")), "\\s+")
    ).alias("word")
)

# Count četnosti slov
word_counts = words.groupBy("word").count()

# Sort slov podle četnosti
sorted_word_counts = word_counts.orderBy(col("count").desc())

# Print výsledků na konzoli
query = (
    sorted_word_counts.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
