# made by Martin "Granc3k" Šimon

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    split,
    window,
    regexp_replace,
    lower,
    current_timestamp,
)

# Settings pro SparkSession
spark = SparkSession.builder.appName("WordCountWithSlidingWindow").getOrCreate()

# Stream čtení dat z TCP socketu
lines = (
    spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

# Process textového streamu
words = lines.select(
    explode(
        split(lower(regexp_replace(col("value"), "[^a-zA-Z0-9\\s]", "")), "\\s+")
    ).alias("word"),
    current_timestamp().alias("timestamp"),  # Appendnutí akt. času
)

# Count četnosti slov s posuvným oknem
word_counts = words.groupBy(
    window(
        col("timestamp"), "30 seconds", "15 seconds"
    ),  # Okno 30 sekund s posunem 15 sekund
    col("word"),
).count()

# Sort podle začátku okna a následně podle četnosti
sorted_word_counts = word_counts.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("word"),
    col("count"),
).orderBy(col("window_start"), col("count").desc())

# Print výsledků na konzoli
query = (
    sorted_word_counts.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
