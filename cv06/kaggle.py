from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder \
    .appName("FlightDelayPrediction") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "8g") \
    .config("spark.memory.fraction", "0.8") \
    .getOrCreate()

# Load dat
df_2002 = spark.read.csv("/files/data/2002.csv", header=True, inferSchema=True)
df_2003 = spark.read.csv("/files/data/2003.csv", header=True, inferSchema=True)
df_2004 = spark.read.csv("/files/data/2004.csv", header=True, inferSchema=True)

# Union dat
df = df_2002.union(df_2003).union(df_2004)

# Select relevantních sloupců
columns = ["Year", "Month", "DayofMonth", "DayOfWeek", "CRSDepTime", "CRSArrTime", "UniqueCarrier", 
           "CRSElapsedTime", "Origin", "Dest", "Distance", "Cancelled", "ArrDelay"]
df = df.select(columns)

# Fix hodnot 
df = df.filter((col("Cancelled") == 0) & (col("ArrDelay").isNotNull()))

# Příprava labelu
df = df.withColumn("label", when(col("ArrDelay") > 0, 1).otherwise(0).cast(DoubleType()))

# Retype sloupce CRSElapsedTime na num
df = df.withColumn("CRSElapsedTime", col("CRSElapsedTime").cast(IntegerType()))

# Příprava input příznaků
feature_columns = ["Year", "Month", "DayofMonth", "DayOfWeek", "CRSDepTime", "CRSArrTime", "UniqueCarrier", 
                   "CRSElapsedTime", "Origin", "Dest", "Distance"]

# Retype string sloupců na num
indexers = [StringIndexer(inputCol=column, outputCol=column+"_index", handleInvalid="skip").fit(df) for column in ["UniqueCarrier", "Origin", "Dest"]]
pipeline = Pipeline(stages=indexers)
df = pipeline.fit(df).transform(df)

# Rozdělení dat na chunky||partitions
df = df.repartition(100)

# Vytvoření feature vectoru
assembler = VectorAssembler(inputCols=[col+"_index" if col in ["UniqueCarrier", "Origin", "Dest"] else col for col in feature_columns], outputCol="features", handleInvalid="skip")
df = assembler.transform(df)

# Split dat na tren a test sadu
train_df, test_df = df.randomSplit([0.9, 0.1], seed=42)

# Tren modelu logistické regrese
lr = LogisticRegression(featuresCol="features", labelCol="label")
lr_model = lr.fit(train_df)

# Vyhodnocení modelu
predictions = lr_model.transform(test_df)
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print(f"Logistic Regression Accuracy: {accuracy}")

# Pokud accuracy < 0.55, zkusíme jiný klasifikátor (random forest accuracy by měla být vyšší)
if accuracy < 0.55:
    rf = RandomForestClassifier(featuresCol="features", labelCol="label")
    rf_model = rf.fit(train_df)
    predictions = rf_model.transform(test_df)
    accuracy = evaluator.evaluate(predictions)
    print(f"Random Forest Accuracy: {accuracy}")

spark.stop()