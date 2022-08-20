from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv(r"...\data\fakefriends-header.csv")

peopleDS = people.select(people.age, people.friends)

peopleDS.groupBy("age").avg("friends").show()

spark.stop()