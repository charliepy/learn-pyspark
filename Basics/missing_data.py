from pyspark.sql import SparkSession
from pyspark.sql.functions import mean

spark = SparkSession.builder.appName('miss').getOrCreate()
df = spark.read.csv('Input/ContainsNull.csv', inferSchema=True, header=True)

# Drop any row with missing data
df.na.drop().show()

# Drop any row that does not have at least 2 non-null values
df.na.drop(thresh=2).show()

# Drop rows if any null value found
df.na.drop(how='any').show()

# Drop rows if all null values in subset columns
df.na.drop(subset=['Name', 'Sales'], how='all').show()

# Spark is able to infer which columns are string and fill those
# Leaves numeric columns alone
df.na.fill('fill value').show()

# Sales is numeric and is left alone
df.na.fill('fill value', subset=['Name', 'Sales']).show()

# Get mean value of Sales column and use that value to fill null values
mean_val = df.select(mean(df['Sales'])).collect()[0][0]
df.na.fill(mean_val, subset=['Sales']).show()
