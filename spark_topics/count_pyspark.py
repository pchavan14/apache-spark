'''
Problem: Could you please explain how to get a count of non null and non nan values of all columns, 
selected columns from DataFrame with Python examples?
'''
'''
In python None is equal to null value , so on PySpark None values are shown as null
'''

import pyspark.sql
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]").appName('check').getOrCreate()

columns  = ["name","state"]
data = [(None,"CA"),("Julia",""),("Ram",None),("Ramya","NULL")]

df_spark = spark.createDataFrame(data,columns)

df_spark.show()

#To print a specific column from a pyspark dataframe use select and show
df_spark.select("name").show()
df_spark.select("name").show()





