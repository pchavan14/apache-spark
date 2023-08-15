#spark dataframe column and rows count

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

#1. get row counts
print(df.count())

#2. get column columns - 
print(df.columns) #this gives the columns list
print(len(df.columns))

#3. count values in a column
df.select(count(col("employee_name"))).show()

df.groupBy(col("department"),col("state")).agg(count("department"),sum("bonus")).show()

