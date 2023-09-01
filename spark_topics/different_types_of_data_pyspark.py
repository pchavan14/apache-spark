#different data types in PySpark

#The Create dataframe can take - list of list , list of tuples , list of raw objects , pandas dataframe and csv etc

#1. MapType - this is used to represent a column that contains key-value pairs similar to dictionary
from pyspark.sql import SparkSession
from pyspark.sql.types import MapType, StringType, ArrayType, StructType, StructField, IntegerType
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("MapTypeExample").getOrCreate()


data = [{
        "id" :"1",
        "input" :"2020-02-01"
}]

df = spark.createDataFrame(data = data , schema = MapType(StringType(),StringType()))
df.show(truncate=False)

df.select(col("value").getItem("input")).show()

#2. Structtype() - it is used to create a dataframe with a struct column , with data which has different types of values in a row

data = [
    ["Alice", 30,[2,3,4],{"height": "170 cm", "weight": "65 kg"}],
    ["Bob", 25,[3,4,5],{"height": "170 cm", "weight": "65 kg"}]
]

schema = StructType(
[
StructField("name",StringType(),True),
StructField("age",IntegerType(),True),
StructField("salary",ArrayType(IntegerType()),True),
StructField("body",MapType(StringType(),StringType()),True)
]
)

df2 = spark.createDataFrame(data=data,schema=schema)

df2.show()

df2.printSchema()

## when we have different datatypes inside a dataframe definition its good to use structypes