import findspark
findspark.init('/home/charlie/spark-2.1.0-bin-hadoop2.7')
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Basics').getOrCreate()
df = spark.read.json('Input/people.json')

# Shows DataFrame
df.show()

# Shows a DataFrame Statistical Summary
df.describe().show()

# Shows Columns and DataTypes
df.printSchema()

# Get specific columns in DataFrame
df.select(['age']).show()

# Get first row
df.head()

# Create new column from existing column
# Note your original df is unchanged
df.withColumn('new_age', df['age']).show()

# Rename a column
# Note your original df is unchanged
df.withColumnRenamed('age', 'new_age').show()

# Create an in memory reference to DataFrame
# Perform SQL queries on this reference
df.createOrReplaceTempView('people')
results = spark.sql('select * from people where age = 30')
results.show()
