import re
import string

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("spark://365d184c51f1:7077").setAppName("WordCount")
# conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("/files/book.txt")
#Normalizace slov mala pismena, -interpunkce
words = input.flatMap(lambda x: x.lower().translate(str.maketrans(dict.fromkeys(string.punctuation))).split())
#setridim slova podle poctu vyskytu
wordCounts = words.map(lambda x: (x,1)).reduceByKey(lambda x,y : x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey(False)
results = wordCountsSorted.take(20)

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
         print(word.decode() + "\t\t" +count)

