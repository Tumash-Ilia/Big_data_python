from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("/files/marvel-names.txt")

lines = spark.read.text("/files/marvel-graph.txt")

connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))


print("Hrdiny kteri maji 1 propoieni")
# Najdu ty, kteri maji 1 propjeni
fewPopular = connections.filter(connections.connections == 1) #.show(connections.count())
# Z DF names najdu id ktere jsou v DF fewPopular
mostPopularName = fewPopular.join(names, ['id'], 'left').select("name").show(fewPopular.count())

print("Hrdiny kteri maji nejmene propoieni")
#Hledam min podle poctu spojeni v setr. DF
min = connections.sort(func.col("connections")).first()[1]
# Najdu ty, kteri maji 1 propjeni
fewPopular = connections.filter(connections.connections == min) #.show(connections.count())
# Z DF names najdu id ktere jsou v DF fewPopular
mostPopularName = fewPopular.join(names, ['id'], 'left').select("name").show(fewPopular.count())

spark.stop()



