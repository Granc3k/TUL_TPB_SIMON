from pyspark.sql import SparkSession
from pyspark.sql.functions import round, sum

spark = SparkSession.builder.appName("TotalSpentByCustomer").getOrCreate()

# Load zakazniku a jejich objednavek
orders = spark.read.option("header", "false").option("inferSchema", "true").csv("/files/data/customer-orders.csv")

# Rename sloupcu
orders = orders.withColumnRenamed("_c0", "customer_id").withColumnRenamed("_c2", "amount")

# Calc celkove castky u kazdyho zakaznika
total_spent = orders.groupBy("customer_id").agg(round(sum("amount"), 2).alias("total_spent"))

# Sort
sorted_total_spent = total_spent.orderBy("total_spent", ascending=False)
sorted_total_spent.show()

spark.stop()