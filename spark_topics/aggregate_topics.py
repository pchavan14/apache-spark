#PySpark provides a list of aggregated functions , all these functions accepts column type and return column type



from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.getOrCreate()
simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)

#different aggregate functions are 
#1. approx_count_distint - count the distinct values 
df.select(approx_count_distinct(col("salary"))).show()

#2. avg() - aggregate function
df.select(avg(col("salary"))).show()

#3. collect_list() - returns all values from input columns with duplicates
df.select(collect_list(col("salary")).alias("collection")).show(truncate=False)

#4. collect_set() - returns all values from input columns by eliminating duplicates
df.select(collect_set(col("salary")).alias("collection")).show(truncate=False)

#5. countDistinct() - to count the distinct values
#6. count() - to count the values in the columns
#7.first() and last() - they return the first and last non null elements in a column