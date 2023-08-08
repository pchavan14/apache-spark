#GroupBY() - similarly in SQL we can also groupby data in pyspark dataframes

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","CA",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)
  ]

spark = SparkSession.builder.getOrCreate()

schema = ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)

#1. Using groupBy() with sum()
'''
When we perform groupBy() on PySpark Dataframe, 
it returns GroupedData object which contains  aggregate functions like sum , max , min and count
'''


df.groupBy(col("department")).sum("salary").show()

df.groupBy(col("department")).count().show()

df.groupBy(col("department")).min("salary").show()

# similarly we can use max , mean etc

#we can also use multiple columns in the groupBy()

df.groupBy(col("department"),col("state")).agg(sum("salary").alias("sum_salary"),sum("bonus").alias("sum_bonus")).show()

#Aggregate functions - these can be used to run multiple aggregations on columns
df.groupBy(col("department"),col("state")).agg(sum("salary").alias("sum_salary"),sum("bonus").alias("sum_bonus")).show()

#Similar the having clause we can also filter the grouped data
df.groupBy(col("department")).agg(sum("salary"),sum("bonus").alias("sum_bonus"),avg("salary"),max("bonus")).filter(col("sum_bonus")>=50000).show()

