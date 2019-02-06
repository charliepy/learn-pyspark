import findspark
findspark.init('/home/charlie/spark-2.1.0-bin-hadoop2.7')
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Basics').getOrCreate()
df = spark.read.json('Input/people.json')

# Shows DataFrame
df.show()

# Shows Columns and DataTypes
df.printSchema()
