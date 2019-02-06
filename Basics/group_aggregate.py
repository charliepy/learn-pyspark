from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, avg, stddev, format_number

spark = SparkSession.builder.appName('aggs').getOrCreate()
df = spark.read.csv('Input/sales_info.csv', inferSchema=True, header=True)

# Groupby the company column
# Calculate the mean which returns a new DataFrame
# Look at pyspark.sql.functions module for more functions
df.groupby('Company').mean().show()

# Pass in a dict for aggregations
agg = {'Sales': 'max'}
df.agg(agg).show()

# Group and aggregate data
group_col = ['Company']
df.groupby(group_col).agg(agg).show()

# Apply functions to a select query
# Give alias to column name
df.select(countDistinct('Sales').alias('Avg Sales')).show()

# Number formatting
df.select(stddev('Sales').alias('StdDev')).select(format_number('StdDev', 2)).show()

# Sort ascending order
df.orderBy('Sales').show()

# Sort descending order
df.orderBy(df['Sales'].desc()).show()
