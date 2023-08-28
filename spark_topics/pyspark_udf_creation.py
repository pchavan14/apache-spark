# create UDF inside the pyspark dataframe


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

columns = ["Seqno","Name"]
data = [("1", "john jones"),
    ("2", "tracey smith"),
    ("3", "amy sanders")]

df = spark.createDataFrame(data=data,schema=columns)

df.show(truncate=False)

# next we first create a python function which later can be converted to UDF


def convertCase(str):
    resStr=""
    arr = str.split(" ")
    for x in arr:
       resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr 


#convert a python functio to pyspark UDF
from pyspark.sql.functions import udf 
from pyspark.sql.functions import *

convertUDF = udf(lambda z: convertCase(z)) 


df.select(col("Seqno"), \
    convertUDF(col("Name")).alias("Name") ) \
   .show(truncate=False)

#you can also use UDF withColumn()

df.withColumn("curated_string",convertUDF(col("Name"))).show(truncate=False)