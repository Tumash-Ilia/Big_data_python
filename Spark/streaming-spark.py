from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, window,col
from pyspark.sql.functions import split, lower, regexp_replace, current_timestamp, desc


spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()


lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

#Mala pismena a odstraneni interpunkce
words = lines.select(explode(split(regexp_replace(lower(lines.value), r'[^\w\s]', ''), " ")).alias("word"))
#Pridani sloupce s casem
words = words.withColumn("timestamp", current_timestamp())
# words.printSchema()
wordCounts = words \
    .groupBy(
        window(col('timestamp'), "10 seconds", "5 seconds"), #Posuvne okno
        words.word) \
    .count()\
    .orderBy("window", desc("count")) #Setrideni podle casu a poctu slov


#wordCounts = words.groupBy("word").count().orderBy("count",  ascending=False)

query = wordCounts.writeStream.outputMode("complete").format("console").option("truncate", "false").start()
query.awaitTermination()

spark.stop()