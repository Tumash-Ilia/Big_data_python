from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("spark://365d184c51f1:7077").setAppName("MinTemperatures")
# conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1
    return (stationID, entryType, temperature)

lines = sc.textFile("/files/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
# results = minTemps.collect();
# for result in results:
#     print(result)
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
results = minTemps.collect()

for result in results:
    print(result[0] + "\t{:.2f}C".format(result[1]))
