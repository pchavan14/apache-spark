from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import *

# Create a Spark session
spark = SparkSession.builder.appName("BroadcastExample").getOrCreate()

# Create a DataFrame for employees and departments
employee_data = [("Alice", 101), ("Bob", 102), ("Cathy", 101)]
department_data = [(101, "Engineering"), (102, "Sales")]
columns_employee = ["name", "department_id"]
columns_department = ["department_id", "department_name"]

employee_df = spark.createDataFrame(data=employee_data,schema=columns_employee)
department_df = spark.createDataFrame(data=department_data,schema=columns_department)

broadcast_department_df = broadcast(department_df)

joined_df = employee_df.join(broadcast_department_df,employee_df.department_id == broadcast_department_df.department_id,"inner").select(col("name"),broadcast_department_df["department_id"]).show()

