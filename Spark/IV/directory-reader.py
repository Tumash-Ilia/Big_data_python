from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split, lower, regexp_replace, desc


spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

#lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

lines = spark.readStream.format("text").option("maxFilesPerTrigger", 1).load("/files/stream")

#Mala pismena a odstraneni interpunkce
words = lines.select(explode(split(regexp_replace(lower(lines.value), r'[^\w\s]', ''), " ")).alias("word"))
wordCounts = words \
    .groupBy(
        words.word) \
    .count()\
    .orderBy(desc("count")) #Setrideni podle poctu slov


#wordCounts = words.groupBy("word").count().orderBy("count",  ascending=False)

query = wordCounts.writeStream.outputMode("complete").format("console").option("truncate", "false").start()
query.awaitTermination()

spark.stop()