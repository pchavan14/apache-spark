# We can either use orderBy() or sort() function to sort the data in ascending or descding in the python dataframe PySpark

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Raman","Finance","CA",99000,40,24000), \
    ("Scott","Finance","NY",83000,36,19000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]
columns= ["employee_name","department","state","salary","age","bonus"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

#1 - DataFrame sorting using sort function - we can even sort on two columns
df.sort(col("department"),col("state")).show()

#2 - DataFrame sorting using orderBy function
df.orderBy(col("employee_name")).show()

#By default the columns are sorted in ascending , we can also specific in any order we want
df.sort(col("state").desc()).show()


