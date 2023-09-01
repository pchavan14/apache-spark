
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

jsonString="""{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
df=spark.createDataFrame([(1, jsonString)],["id","value"])
df.show(truncate=False)


#1. from_json is used to convert JSON string to struct or map types

from pyspark.sql.types import MapType , StringType , StructType

df2 = df.withColumn("value",from_json(col("value"),MapType(StringType(),StringType())))

df2.show(truncate=False)

#2. to_json - this function is used to convert MapType or structType data into json

#3. json_tuple() - is used to query or extrcat the elements from JSON column and create the result as a new columns

df.select(col("id"),json_tuple(col("value"),"Zipcode" , "ZipCodeType","City","State")).show()

#4. get_json_object() - It is used to extract the JSON string based on path from the JSON column

df.select(col("id"),get_json_object(col("value"),"$.ZipCodeType").alias("ZipCodeType")).show()