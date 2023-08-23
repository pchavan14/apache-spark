from pyspark import SparkContext, SparkConf

#spark configuration and context
conf = SparkConf().setAppName("sample_test")
sc = SparkContext(conf=conf)

#read unstrcutured file
text_rdd = sc.textFile("/Users/prachichavan/Documents/apache-spark/spark_topics/sample_data.txt")

print(text_rdd)

total_word_count = text_rdd.flatMap(lambda line: line.split(" ")).count()

print("Total word count",total_word_count)

sc.stop()


