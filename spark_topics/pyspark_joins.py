#PySpark joins to join two dataframes
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

#1. Inner Join 

empDF.join(deptDF,empDF.emp_dept_id == deptDF.dept_id,"inner").show(truncate=False)

#2. We can similarly use different types of joins like outer , left and right
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id,"left").show(truncate=False)

#3.leftsemi join - only joins the joined rows from the left table
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id,"leftsemi").show(truncate=False)

#4. leftanti - it is opposite of leftsemi
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id,"leftanti").show(truncate=False)

#5. When we need to join multiple dataframes we can either use SQL views or chain the dataframes

df1.join(df2,df2.id1 == df1.id1,"inner").join(df3,df3.id1 == df1.id1,"inner") #chaining of joins