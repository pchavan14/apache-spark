import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import window , column , desc , col

spark = SparkSession.builder.getOrCreate()

#as we are running the code in local its furth reducing the shuffle partitions
spark.conf.set("spark.sql.shuffle.partitions","5")

staticDataFrame = spark.read.format("csv").option("header","true").option("inferSchema","true").load("retail_data")

staticDataFrame.createOrReplaceTempView("retail_data")
staticSchema = staticDataFrame.schema

#spark.sql("Select * from retail_data where CustomerID = 14075.0").show(5)

staticDataFrame.selectExpr(
    "CustomerID",
    "(UnitPrice * Quantity) as total_cost", #for this condition we used selectExpr()
    "InvoiceDate"
    ).groupBy(col("CustomerID"), window(col("InvoiceDate"),"1 day")).sum("total_cost").show(2)

# selectExpr() - it is a variant of select that accepts SQL operations

#making the dataframe streaming

streamingDataFrame = spark.readStream.schema(staticSchema).option("maxFilesPerTrigger","1").format("csv").option("header","true").load("retail_data")


streamingDataFrame.isStreaming

purchaseByCustomerPerHour = streamingDataFrame.selectExpr(
    "CustomerID",
    "(UnitPrice * Quantity) as total_cost", #for this condition we used selectExpr()
    "InvoiceDate"
    ).groupBy(col("CustomerID"), window(col("InvoiceDate"),"1 day")).sum("total_cost")


purchaseByCustomerPerHour.writeStream.format("console").queryName("customer_purchases").outputMode("complete").start()

#spark.sql("select * from customer_purchases order by `sum(total_cost)`")