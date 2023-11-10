import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[1]").appName("Example").getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", "5") #by deafult 200

flightData2015 = spark.read.option("inferSchema","true").option("header","true").csv("flight-data_csv/2015-summary.csv")

flightData2015.sort("count").explain()

#physical partition goes from 1 to 5
flightData2015.createOrReplaceTempView("flight_data_2015")

sqlWay = spark.sql("SELECT DEST_COUNTRY_NAME, count(1) FROM flight_data_2015 GROUP BY DEST_COUNTRY_NAME")

#max data
flightData2015.show(1)  #select is used to print the column data

flightData2015.select(max(col("count"))).show()

flightData2015.groupBy(col("DEST_COUNTRY_NAME")).agg(sum(col("count"))).limit(5).explain()