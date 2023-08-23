from pyspark.sql import SparkSession

# Create a Spark session
# this repartition technique would help to make the joins faster for execution
spark = SparkSession.builder.appName("JoinRepartitionExample").getOrCreate()

data1 = [("Alice", 25), ("Bob", 30), ("Cathy", 22)]
data2 = [("Alice", "Engineer"), ("Bob", "Manager"), ("David", "Analyst")]
columns1 = ["name", "age"]
columns2 = ["name", "occupation"]


df1 = spark.createDataFrame(data1, columns1)
df2 = spark.createDataFrame(data2, columns2)

df1.show()

repartitioned_df1 = df1.repartition(2,"name")

repartitioned_df2 = df2.repartition(2,"name")

repartitioned_df1.show()

joined_df = repartitioned_df1.join(repartitioned_df2,repartitioned_df1.name == repartitioned_df2.name, "inner")

joined_df.show()

## Coalesce() functiojn is used to decrease the number of partitions

data = [("Alice", 25), ("Bob", 30), ("Cathy", 22), ("David", 28)]
columns = ["name", "age"]

df = spark.createDataFrame(data,columns)

df.show()



print(df.rdd.getNumPartitions())

coalesced_df = df.coalesce(4)

print(coalesced_df.rdd.getNumPartitions())

df4 = df.groupBy("name")
print(df4.rdd.getNumPartitions())





