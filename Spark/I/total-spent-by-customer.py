from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("spark://365d184c51f1:7077").setAppName("MinTemperatures")
# conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)


def parseLine(line):
    fields = line.split(',')
    custID = int(fields[0])
    prodID = int(fields[1])
    price = float(fields[2])
    return (custID, prodID, price)

lines = sc.textFile("/files/customer-orders.csv")
parsedLines = lines.map(parseLine)

totalOrders = parsedLines.map(lambda x: (x[0], 1)).reduceByKey(lambda x,y : x + y).sortByKey()

totalSpent = parsedLines.map(lambda x: (x[0], x[2])).reduceByKey(lambda x,y : x + y).sortByKey()

totalSpentSorted = parsedLines.map(lambda x: (x[0], x[2])).reduceByKey(lambda x,y : x + y)\
                   .map(lambda x: (x[1], x[0])).sortByKey(False)


result = totalOrders.take(5)
print()
print("Pocet objednavek pro kazdeho zakaznika")
for res in result:
    print("CustID:", res[0], "Pocet objednavek:", res[1])

result = totalSpent.take(5)
print()
print("Celkova utracena castka pro kazdeho zakaznika")
for res in result:
    print("CustID:", res[0], "Utracena castka: {:.2f}$".format(res[1]))


result = totalSpentSorted.take(5)
print()
print("Celkova utracena castka pro kazdeho zakaznika podle celkove utracene castky")
for res in result:
    print("CustID:", res[1], "Utracena castka: {:.2f}$".format(res[0]))