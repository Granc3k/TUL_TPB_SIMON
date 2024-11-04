from pyspark import SparkConf, SparkContext
master = "spark://aab3dd1bb876:7077"

conf = SparkConf().setMaster(master).setAppName("TotalSpentByCustomer")
sc = SparkContext(conf=conf)

input = sc.textFile("/files/data/customer-orders.csv")

def parseLine(line):
    parts = line.split(",")  
    customer_id = parts[0]   
    price = float(parts[2])
    return (customer_id, price)

# Zpracování vstupních dat na RDD
orders = input.map(parseLine)

# Spočítání celkové částky pro každého zákazníka
totalSpent = orders.reduceByKey(lambda x, y: x + y)

# Seřazení podle celkové částky (od nejvyššího po nejnižší)
sortedTotalSpent = totalSpent.map(lambda x: (x[1], x[0])).sortByKey(False)

# Výpis 20 zákazníků s nejvyššími útratami
results = sortedTotalSpent.take(20)

for total, customer_id in results:
    print(f"Customer ID: {customer_id}, Total Spent: {total}")


sc.stop()
