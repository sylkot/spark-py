from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostObscureSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv(r"...\data\Marvel-names.txt")

lines = spark.read.text(r"...\data\Marvel-graph.txt")

connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

leastConnection = connections.agg(func.min("connections")).first()[0]

leastPopular = connections.filter(func.col("connections")==leastConnection)

leastPopularNames = leastPopular.join(names, "id")

leastPopularNames.show()

spark.stop()
