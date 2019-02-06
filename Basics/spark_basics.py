from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Basics').getOrCreate()
df = spark.read.json('Input/people.json')
df.show()