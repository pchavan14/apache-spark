'''
Please explain how to get a count of non null and non nan values of all columns , selected columns in dataframe?
'''

# In python Null is equal to None


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [(None,"CA"),("Julia",""),("Ram",None),("Ramya","NULL")]
df =spark.createDataFrame(data,["name","state"])
df.show()

#1. PySpark count of non null values of a single  column
df.filter(col("name").isNull()).show()

#2. PySpark count of non null values on all columns
df.select([count(when(col(c).isNull(),c)).alias(c) for c in df.columns]).show()

#3. PySpark count of non null values on selected columns
df_columns = ["name","state"]

df.select([count(when(col(c).isNotNull(),c)).alias(c) for c in df_columns]).show()

#4. PySpark how to ignore NULL or None literal strings

df.select([count(when(~col(c).contains('NULL'),c)).alias(c) for c in df_columns]).show()

#5. PySpark how to check non nan values

isnan(df.name)







