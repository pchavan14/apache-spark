
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
address = [(1,"14851 Jeffrey Rd","DE"),
    (2,"43421 Margarita St","NY"),
    (3,"13111 Siemon Ave","CA")]
df =spark.createDataFrame(address,["id","address","state"])
df.show()


#1. PySpark Replace string column values using withColumn() where first value is the column we are working on

from pyspark.sql.functions import *

df.withColumn("address",regexp_replace("address","Rd","Road")).show()

#2. overlay() function replaces the input with replacing , which  starts at pos and is of the length len


#Overlay
from pyspark.sql.functions import overlay
df = spark.createDataFrame([("ABCDE_XYZ", "FGH")], ("col1", "col2"))
df.select(overlay("col1", "col2", 7).alias("overlayed")).show()

#+---------+
#|overlayed|
#+---------+
#|ABCDE_FGH|
#+---------+


