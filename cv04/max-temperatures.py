# made by Martin "Granc3k" Šimon
from pyspark import SparkConf, SparkContext

master = "spark://aab3dd1bb876:7077"

conf = SparkConf().setMaster(master).setAppName("MaxTemperatures")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(",")
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1
    return (stationID, entryType, temperature)


lines = sc.textFile("/files/data/1800.csv")
parsedLines = lines.map(parseLine)


maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])

stationTemps = maxTemps.map(lambda x: (x[0], x[2]))

maxTempsByStation = stationTemps.reduceByKey(lambda x, y: max(x, y))

results = maxTempsByStation.collect()

for result in results:
    print(result[0] + "\t{:.2f}C".format(result[1]))
