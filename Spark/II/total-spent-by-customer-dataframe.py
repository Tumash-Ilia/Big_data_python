from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, round
spark = SparkSession.builder.master("spark://6ddab77f0471:7077").appName("SparkSQL").getOrCreate()


def mapper(line):
    fields = line.split(",")
    return Row(CustID=int(fields[0]), ProdID=int(fields[1]), Price=float(fields[2]))


lines = spark.sparkContext.textFile("/files/customer-orders.csv")
customers = lines.map(mapper)

schemaCustomers = spark.createDataFrame(customers).cache()
schemaCustomers.createOrReplaceTempView("customers")

#schemaCustomers.printSchema()
"""
Shlukovani podle ID zakaznika
Vypocet celkove vyssi objednavek
"""
print("Pocet objednavek pro kazdeho zakaznika")
schemaCustomers.groupBy("CustID").count().orderBy("CustID").show()


"""
Shlukovani podle ID zakaznika
Vypocet celkove utracene caskty pro kazdeho 
Setrideni podle utracene castky
"""
print()
print("Celkova utracena castka pro kazdeho zakaznika")
schemaCustomers.groupBy("CustID").agg(round(sum("Price"),2).alias("total_spent")).orderBy("total_spent").show(100)


spark.stop()
