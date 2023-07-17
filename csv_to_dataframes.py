
#Differen ways to create a data frame

#create data frame from list data object instead of an RDD inside a spark programme
#the data and columns are as below


import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

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
#by using format method we can parametrize the type of file ingested
df2 = spark.read.option("header","True").option("inferSchema","True").format("csv").load("/Users/prachichavan/Downloads/zipcodes.csv")
df3 = spark.read.option("header","True").option("inferSchema","True").format("csv").load("/Users/prachichavan/Downloads/zipcodes.csv")

#You can also read multiple files in a folder using the csv method
'''
Infer schema , the default value is set to false we can change it to true to infer schema from the CSV file itself. It require reading the data one more time
'''

df2.printSchema()

'''
We can also use schema option to get the defined schema instead
'''
schema = StructType().add("RecordNumber",IntegerType(),"True")
df_with_schema = spark.read.format("csv").option("header", "true").schema(schema).load("src/main/resources/zipcodes.csv")
df_with_schema.printSchema()
df_with_schema.show()













