from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from faker import Faker
import time
import random

# Create a Spark session
spark = SparkSession.builder.appName("SemiStructuredStreamingExample").getOrCreate()

# Create a Faker instance
fake = Faker()

# Define user types
user_types = ["registered", "guest"]

# Simulate a streaming data source
def generate_streaming_data():
    while True:
        data = {
            "timestamp": int(time.time()),
            "user": {
                "type": random.choice(user_types),
                "name": fake.name(),
                "age": random.randint(18, 60),
                "location": fake.city()
            }
        }
        yield data
        time.sleep(1)  # Simulate data arriving every second

# Create a DataFrame from the streaming data source
streaming_data = generate_streaming_data()

for data in streaming_data:
    print(data)

# stream_df = spark.createDataFrame(streaming_data, samplingRatio=1)

# # Print the streaming data to the console
# query = stream_df.writeStream.format("console").start()
# # Wait for the query to terminate
# query.awaitTermination()

