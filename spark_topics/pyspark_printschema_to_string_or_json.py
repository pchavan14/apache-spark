'''
How to export  spark/PySpark printSchema() result to string or json ?
'''


# Import
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()
                    
# Create DataFrame                
columns = ["language","fee"]
data = [("Java", 20000), ("Python", 10000), ("Scala", 10000)]

df = spark.createDataFrame(data=data,schema=columns)
# df.printSchema()
# df.show()

#1. Save PySpark printSchema() to string , the source code of above statement does below
schemaString = df._jdf.schema().treeString()
print(schemaString)

