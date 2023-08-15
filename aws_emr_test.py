import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType


#spark is an object of the SparkSession
spark = SparkSession.builder.appName("Example").getOrCreate()


columns = ["language","user_count"]
data = [("Java","20000"),("Python","100000"),("Scala","3000")]
df_parquet=spark.createDataFrame(data,columns)


df_parquet.write.mode("overwrite").parquet("s3://pyspark-emr-test/parquet/people.parquet")



