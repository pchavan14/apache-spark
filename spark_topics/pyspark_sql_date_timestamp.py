#PySpark SQL have many timestamp functions to work on 

from pyspark.sql.functions import *
from pyspark.sql import SparkSession

import pandas as pd

spark = SparkSession.builder.getOrCreate()

#cannot be done in pyspark dataframe that easily , but can be done in pandas dataframe
data = {
        "id" :[1,2,3],
        "input" :["2020-02-01","2019-03-01","2021-03-01"]
}

df = pd.DataFrame(data)

df_spark = spark.createDataFrame(df).show()

#or the data can be represented in below way

data = [(1,"2020-02-01"),(2,"2019-03-01"),(3,"2021-03-01")]

df = spark.createDataFrame(data=data,schema=["id","input"])

#current date

df.select(current_date()).show(1) #this will show only the top 1 record

df.select(current_timestamp()).show(2,truncate=False)

#many such different functions are available for Date and timestamp

