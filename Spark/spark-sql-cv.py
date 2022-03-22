from pyspark.sql import SparkSession
from pyspark.sql import functions as func
spark = SparkSession.builder.master("spark://6ddab77f0471:7077").appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("/files/fakefriends-header.csv")


print("Prumerny pocet kamaradu podle veku")
#people.groupBy("age").mean("friends").orderBy("age").show()
"""
Shlukovani podle veku
Vypocet prumenru poctu a zaokrouhleni na 2 des mista
"""

people.groupBy("age").agg(func.round(func.mean("friends"), 2).alias('friends_avg')).orderBy("age").show()

spark.stop()

