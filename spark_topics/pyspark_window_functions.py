from pyspark.sql import SparkSession


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )
 
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

# import Window functions

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

windowSpec = Window.partitionBy("department").orderBy("salary")

df.withColumn("row_number",row_number().over(windowSpec)).show(truncate=False)


# rank and dense_rank function 
from pyspark.sql.window import Window
from pyspark.sql.functions import rank , dense_rank , lead , lag  , col 

windowSpec = Window.partitionBy("department").orderBy("salary")

df.withColumn("rank_number",dense_rank().over(windowSpec)).show(truncate=False)

#lag and lead functions 
df.withColumn("lead_salary" , lead("salary").over(windowSpec)).show(truncate=False)
df.withColumn("lead_salary" , lag("salary").over(windowSpec)).show(truncate=False)





from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum

# Create a Spark session
spark = SparkSession.builder.appName("WindowSumExample").getOrCreate()

# Sample data
data = [
    ("A", 100),
    ("B", 200),
    ("A", 150),
    ("B", 300),
]

# Create a DataFrame
columns = ["category", "amount"]
df = spark.createDataFrame(data, columns)

df.show()

# Define the window specification
window_spec = Window.partitionBy("category").orderBy("amount")

# Calculate the running total using sum() over the window
df_with_running_total = df.withColumn("running_total", sum(col("amount")).over(window_spec))

# Show the result
df_with_running_total.show()



