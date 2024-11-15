from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor
import pandas as pd
import numpy as np

spark = SparkSession.builder.appName("RealEstateModel").getOrCreate()

# Load dat
data = spark.read.csv("/files/data/realestate.csv", header=True, inferSchema=True)

# Výběr příznaků a cílové proměnné
assembler = VectorAssembler(inputCols=["HouseAge", "DistanceToMRT", "NumberConvenienceStores"], 
                            outputCol="features")
data = assembler.transform(data).select("PriceOfUnitArea", "features")

# Rozdělení na tren a test sadu
train_data, test_data = data.randomSplit([0.9, 0.1], seed=1234)

# Trenál modelu
dt = DecisionTreeRegressor(labelCol="PriceOfUnitArea", featuresCol="features")
model = dt.fit(train_data)

# Predikce na test datech
predictions = model.transform(test_data)

# Export predikcí do CSVčka
predictions.select("prediction", "PriceOfUnitArea").toPandas().to_csv("/files/data/predictions.csv", index=False)

# Load výsledků do pandasu
df = pd.read_csv("/files/data/predictions.csv")

# Výpočet rozdílu mezi predikovanými a reálnými hodnotami
df['difference'] = np.abs(df['prediction'] - df['PriceOfUnitArea'])

# Print vsech test dat
print(df)

# Výpočet avg pred erroru
average_difference = df['difference'].mean()
print(f"Average Prediction Error: {average_difference}")