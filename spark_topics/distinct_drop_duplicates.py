
# Import pySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# Prepare Data
data = [("James", "Sales", 3000), \
    ("Michael", "Sales", 4600), \
    ("Robert", "Sales", 4100), \
    ("Maria", "Finance", 3000), \
    ("James", "Sales", 3000), \
    ("Scott", "Finance", 3300), \
    ("Jen", "Finance", 3900), \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000), \
    ("Saif", "Sales", 4100) \
  ]

# Create DataFrame
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.orderBy(col("employee_name")).show(truncate=False) #order by descending / ascending can also be done

#distinct creates a new dataframe with all duplicate removed
df_new = df.distinct()
df_new_1 = df.dropDuplicates()

df_new.orderBy(col("employee_name")).show(truncate=False)

#we can also select columns we want duplicate data to be eliminated using 

df_new_2 = df.dropDuplicates(["department","salary"])
df_new_2.orderBy(col("salary")).show()