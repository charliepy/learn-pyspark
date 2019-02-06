import findspark
findspark.init('/home/charlie/spark-2.1.0-bin-hadoop2.7')
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Basics').getOrCreate()
df = spark.read.json('Input/people.json')
df.show()
