from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PropertyValue").getOrCreate()


dataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv("/files/realestate.csv")


# dataFrame.printSchema()
# dataFrame.show()
#df = dataFrame.select(["HouseAge","DistanceToMRT", "NumberConvenienceStores",])

# Pro pouziti vice vstupnich priznaku pouziju assembler
assembler = VectorAssembler(inputCols=["HouseAge","DistanceToMRT", "NumberConvenienceStores"]
                            ,outputCol = "features")

data = assembler.transform(dataFrame).select("features", "PriceOfUnitArea").drop()
#Rozdelim trenovanaci data na 90/10
train_data, test_data = data.randomSplit([0.9,0.1])

#Vytvorim rozhodovaci strom
decisionTree = DecisionTreeRegressor(featuresCol="features", labelCol="PriceOfUnitArea")

#Trenovani
fit_model = decisionTree.fit(train_data)

#Testovani
result = fit_model.transform(test_data)

result.show()

spark.stop()