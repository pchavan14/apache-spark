#1. Retrive all column data types and names

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [(1,"Robert"), (2,"Julia")]
df =spark.createDataFrame(data,["id","name"])

#1. Retrieve column name and data types
for col in df.dtypes:
    print(col[0],col[1])

#2. Retrieve column name and data types by using df.schema.fields
# for field in df.schema.fields:
#     print(field[0],field[1])

#3. get data type of specific column
print(df.schema["name"].dataType)

#4. get all the columns as a list
print(df.columns)