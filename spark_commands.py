
import pyspark
from pyspark.sql import *
spark = SparkSession.builder.master("local[1]").appName("Example").getOrCreate()
'''
master() - If you are running it on a cluster , you need to identify the name of the master 
local - when running in standalone mode , the number identifies how many partitions should it create , x is the number of CPU cores you have

We only have one context per pyspark application , but we can create a new session

'''


spark2 = SparkSession.newSession

#We can also create a DataFrame using pyspark

df = spark.createDataFrame([("Scala", 25000), ("Spark", 35000), ("PHP", 21000)])
df.show()

'''
Using Spark Session we can access PySpark/Spark SQL capabilities in PySpark. In order to use SQL features first you need to create a temporary view in PySpark
Once you have a temporary view you can run any ANSI SQL on it
'''

df.createOrReplaceTempView("sample_table")
df2 = spark.sql("Select * from sample_table")
df2.show()

