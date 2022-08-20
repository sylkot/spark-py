from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("CustomerSpent").getOrCreate()

schema = StructType([ \
                     StructField("customerID", IntegerType(), True), \
                     StructField("itemID", IntegerType(), True), \
                     StructField("amount", FloatType(), True)])

df = spark.read.schema(schema).csv(r"...\data\customer-orders.csv")
df.printSchema()

result = df.groupBy("customerID").agg(func.round(func.sum("amount"),2).alias("total_amount")).sort("total_amount")

result.show()

spark.stop()
