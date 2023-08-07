# Explode array or list and map columns to rows


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayData = [
        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})
]

df = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])
df.printSchema()
df.show()

# we can store different types of data into the DataFrames
'''
explode() - this function splits the array values into different rows and for map it creates two new columns for key and value
this will ignore null or empty element (the arrays which have all the elements as null or empty)
'''


df.select(col("name"),col("properties"),explode(col("knownLanguages"))).show()


df.select(col("name"),explode(col("properties"))).show()
'''
explode_outer() - it works same as explode by creating rows for each different values in array , but
unlike explode it also considers the nulls
'''

df.select(col("name"),explode_outer(col("properties"))).show()

'''
posexplode() - converts arrays into different rows but also create extra columns to mention the location 
'''

df.select(col("name"),posexplode(col("knownLanguages"))).show()

df.select(col("name"),posexplode(col("properties"))).show()

'''

How to explode and flatten nested arrays or array of arrays into rows ?

'''

# below we are creating a nested array columns


import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayArrayData = [
  ("James",[["Java","Scala","C++"],["Spark","Java"]]),
  ("Michael",[["Spark","Java","C++"],["Spark","Java"]]),
  ("Robert",[["CSharp","VB"],["Spark","Python"]])
]

df = spark.createDataFrame(data=arrayArrayData, schema = ['name','subjects'])
df.printSchema()
df.show(truncate=False)

df.select(col("name"),explode(col("subjects"))).show()

# If you want to flatten array of arrays , we can use flatten() function

df.select(col("name"),flatten(col("subjects"))).show(truncate=False)



