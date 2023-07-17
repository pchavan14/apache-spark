
#Differen ways to create a data frame

#create data frame from list data object instead of an RDD inside a spark programme
#the data and columns are as below


import pyspark.sql
from pyspark.sql import SparkSession

#spark is an object of the SparkSession
spark = SparkSession.builder.master("local[1]").appName("Example").getOrCreate()

columns = ["language","user_count"]
data = [("Java","20000"),("Python","100000"),("Scala","3000")]

#using createDataFrame() from SparkSession

df = spark.createDataFrame(data).toDF(*columns)

#pyspark uses show instead of the print
df.show()

##Create data frame from CSV, text , JSON files similar to real time use-cases
#Spark supports reading from CSV and writing to CSV files


df = spark.read.option("header","True").csv("/Users/prachichavan/Downloads/zipcodes.csv")
df2 = spark.read.option("header","True").format("csv").load("/Users/prachichavan/Downloads/zipcodes.csv")


df2.show()







