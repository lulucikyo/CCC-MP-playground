import findspark
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2'
findspark.init()

spark = SparkSession \
    .builder \
    .appName("MP2") \
    .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "b-1.mp2.uhzy0o.c3.kafka.us-east-1.amazonaws.com:9094,b-2.mp2.uhzy0o.c3.kafka.us-east-1.amazonaws.com:9094,b-3.mp2.uhzy0o.c3.kafka.us-east-1.amazonaws.com:9094") \
  .option("subscribe", "my-topic") \
  .option("startingOffsets", "earliest") \
  .load()

#.option("kafka.group.id", "str-test") \

df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

query = (
    df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
)

query.awaitTermination()