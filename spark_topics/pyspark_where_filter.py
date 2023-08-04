from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField 
from pyspark.sql.types import StringType, IntegerType, ArrayType
spark=SparkSession.builder.master("local[1]").getOrCreate()

data = [
    (("James","","Smith"),["Java","Scala","C++"],"OH","M"),
    (("Anna","Rose",""),["Spark","Java","C++"],"NY","F"),
    (("Julia","","Williams"),["CSharp","VB"],"OH","F"),
    (("Maria","Anne","Jones"),["CSharp","VB"],"NY","M"),
    (("Jen","Mary","Brown"),["CSharp","VB"],"NY","M"),
    (("Mike","Mary","Williams"),["Python","VB"],"OH","M")
 ]
        
schema = StructType([
     StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
     ])),
     StructField('languages', ArrayType(StringType()), True),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
 ])

df = spark.createDataFrame(data = data, schema = schema)
df.printSchema()
df.show(truncate=False)

#1. DataFrame filter() with column
from pyspark.sql.functions import *

df.filter(col("state")=="OH").show()

#2. DataFrame filter() with SQL expression

df.filter("state == 'OH'").show()

#3. DataFrame filter() with multiple conditions (& , |)

df.filter((col("state")=="OH") & (col("gender") != "M")).show()

#4. Filter based on list values - if we have list of elements we can filter the data based on if it is present in list or not
li = ["OH","CA","DE"]

df.filter(col("state").isin(li)).show()

#5. Filter based on starts with, ends with, contains method of the column class
df.filter(df.state.startswith("O")).show()

#6. PySpark filter like and rlike

data2 = [(2,"Michael Rose"),(3,"Robert Williams"),
     (4,"Rames Rose"),(5,"Rames rose")
  ]
df2 = spark.createDataFrame(data=data2,schema=["id","name"])

df2.show()
#like feature of SQL can use used here
df2.filter(col("name").like("%rose%")).show()



