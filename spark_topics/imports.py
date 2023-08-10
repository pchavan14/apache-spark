from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark_var = SparkSession.builder.getOrCreate()