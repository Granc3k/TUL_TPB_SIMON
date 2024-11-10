# made by Martin "Granc3k" Šimon
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, size, min

spark = SparkSession.builder.appName("MostObscureSuperheroes").getOrCreate()

# Load dat
graph = spark.read.text("/files/data/marvel-graph.txt")

# Extrakce superhrdinů ID and connexe
connections = graph.withColumn("id", split(col("value"), " ")[0]).withColumn(
    "connections", split(col("value"), " ").alias("connections")
)

# Calc počtu spojení
connections_count = connections.withColumn(
    "connections_count", size(col("connections")) - 1
)

# Load jmen superhrdinů
names = (
    spark.read.option("sep", " ")
    .csv("/files/data/marvel-names.txt")
    .withColumnRenamed("_c0", "id")
    .withColumnRenamed("_c1", "name")
)

# Joinnutí conexí a jmen
superheroes = connections_count.join(names, "id")

# Findne se minimum
min_connections = superheroes.agg(min("connections_count")).first()[0]

# filter na minimum
most_obscure_superheroes = superheroes.filter(
    col("connections_count") == min_connections
)

# Shownou se výsledky
most_obscure_superheroes.select("name", "connections_count").show()

spark.stop()
