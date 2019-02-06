import findspark
findspark.init('/home/charlie/spark-2.1.0-bin-hadoop2.7')
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('ops').getOrCreate()

# Unlike read.json you can inferschema with read.csv
# header makes first line as the names of columns
df = spark.read.csv('Input/appl_stock.csv', inferSchema=True, header=True)

# Use a SQL like filter to show specific rows
df.filter('Close < 500').show()

# Select specific columns from query
df.filter('Close < 500').select(['Open', 'Close']).show()

# DataFrame filter
# Note the bit operator &
# You can write filter all in one line, keep track of ()
close_less_than_500 = df['Close'] < 500
open_greater_than_200 = df['Open'] > 200
df.filter(close_less_than_500 & open_greater_than_200).select(['Open', 'Close']).show()

# Collect a list of rows
# Turn a row into a dict
result = df.filter(close_less_than_500).collect()
row = result[0].asDict()
row['Volume']
