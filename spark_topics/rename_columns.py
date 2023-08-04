'''
PySpark withColumnRenamed() to rename a DataFrame column.
Since dataframes are immutable (unchangeable) we cannot rename or update a column instead when using withColumnRenamed()
it creates a new DataFrame with updated column names

'''

#base schema with nested structure

dataDF = [(('James','','Smith'),'1991-04-01','M',3000),
  (('Michael','Rose',''),'2000-05-19','M',4000),
  (('Robert','','Williams'),'1978-09-05','M',4000),
  (('Maria','Anne','Jones'),'1967-12-01','F',4000),
  (('Jen','Mary','Brown'),'1980-02-17','F',-1)
]


from pyspark.sql.types import StructType,StructField, StringType, IntegerType
schema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('dob', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
df = spark.createDataFrame(data = dataDF, schema = schema)
df.printSchema()

'''
There are different ways to change the name of the column which  are dicussed below
'''

#1. PySpark withColumnRenamed() - to rename a dataframe column name , this function creates a new Dataframe the older one remains same

df_renamed = df.withColumnRenamed("dob","DateOfBirth")

df_renamed.show()

#2. PySpark withColumnRenamed() - to rename multiple columns in a dataframe
df_renamed_multiple = df.withColumnRenamed("dob","DataOfBirth").withColumnRenamed("salary","salary_amount")
df_renamed_multiple.show()

#3. Using PySpark StructType - To rename a nested column in DataFrame
'''
changing a column name on a nested columns we create a new schema and use a cast function
'''

from pyspark.sql.functions import col

schema2 = StructType(
[
StructField("fname",StringType()),
StructField("mname",StringType()),
StructField("lname",StringType())
]
)

df.select(col("name").cast(schema2),col("dob"),col("gender")).printSchema()

#4 . Using Select to rename the nested columns by Alias function
df.printSchema()

df.select(col("name.firstname").alias("fname"),col("name.middlename").alias("mname"),col("name.lastname").alias("lname")).printSchema()
df.printSchema()

#5. Using toDF() - to change all columns in a PySpark Dataframe
newColumns = ["newCol1","newCol2","newCol3","newCol4"]
df.toDF(*newColumns).printSchema()








