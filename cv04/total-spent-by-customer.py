from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("spark://172.18.0.5:7077").setAppName("TotalSpentByCustomer")
sc = SparkContext(conf=conf)

input = sc.textFile("/files/data/customer-orders.csv")

def parseLine(line):
    parts = line.split(",")  # Rozdělení řádku podle čárky
    customer_id = parts[0]   # ID zákazníka
    price = float(parts[2])   # Cena jako float
    return (customer_id, price)

# Zpracování vstupních dat
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
