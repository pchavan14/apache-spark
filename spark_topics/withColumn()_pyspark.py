'''
PySpark withColumn() is a transformation function of a dataframe which is used to change the value, convert the datatype
of an existing column and create a new column etc
'''

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
df = spark.createDataFrame(data=data,schema=columns)

df.show()

#1. Change the datatype using PySpark withColumn() - the dataframes in PySpark are immutable
from pyspark.sql.functions import *
print(df.printSchema())
df.withColumn("salary",col("salary").cast("Integer"))
print(df.printSchema())

#2. Update the value of an existing column
df.withColumn("salary",col("salary")*1000).show()

#3. Create a column from an existing
df.withColumn("new_column",col("salary")*-1).show()

#4. Create a new column using withColumn() by lit() function to add literal value
df.withColumn("new_column",lit("USA")).show()

#5. Rename a column name
df.withColumnRenamed("gender","sex").show()

#6. Drop a column from the DataFrame
df.drop("salary").show()
