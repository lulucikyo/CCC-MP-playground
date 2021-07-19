import findspark
import os
import pyspark
import time

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import boto3


def stop_stream_query(query, wait_time):
    while query.isActive:
        msg = query.status['message']
        data_avail = query.status['isTriggerActive']
        trigger_active = query.status['isTriggerActive']
        if not data_avail and not trigger_active and msg!='Initializing sources':
            print('Stopping query')
            query.stop()
        time.sleep(0.5)
    print("Awaiting Termination ...")
    query.awaitTermination(wait_time)

client = boto3.client("dynamodb", region_name="us-east-1")

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2'
findspark.init()

spark = SparkSession \
    .builder \
    .appName("MP2") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "b-1.mp2-2.bd6aae.c3.kafka.us-east-1.amazonaws.com:9092,b-2.mp2-2.bd6aae.c3.kafka.us-east-1.amazonaws.com:9092,b-3.mp2-2.bd6aae.c3.kafka.us-east-1.amazonaws.com:9092") \
  .option("subscribe", "test") \
  .option("startingOffsets", "earliest") \
  .load()
#.option("kafka.group.id", "str-test") \

#df.isStreaming()
df.printSchema()

df = df.selectExpr("CAST(value AS STRING)")

schema = StructType([StructField("DayOfWeek", StringType(), True),
                    StructField("FlightDate", StringType(), True),
                    StructField("UniqueCarrier", StringType(), True),
                    StructField("FlightNum", StringType(), True),
                    StructField("Origin", StringType(), True),
                    StructField("Dest", StringType(), True),
                    StructField("CRSDepTime", StringType(), True),
                    StructField("DepTime", StringType(), True),
                    StructField("DepDelay", StringType(), True),
                    StructField("CRSArrTime", StringType(), True),
                    StructField("ArrTime", StringType(), True),
                    StructField("ArrDelay", StringType(), True)
                    ])

df = df.select(from_json(df.value, schema).alias("json"))
df = df.select(col("json.*"))

# Question 1.2
dfq1_2 = df.groupby("UniqueCarrier").agg(mean("ArrDelay")) \
        .orderBy("avg(ArrDelay)").select("UniqueCarrier", "avg(ArrDelay)") \
        .limit(10)
# Question 1.3
dfq1_3 = df.groupby("DayOfWeek").agg(mean("ArrDelay")) \
        .orderBy("avg(ArrDelay)").select("DayOfWeek", "avg(ArrDelay)")

# Question 2.1
dfq2_1 = df.groupby("Origin", "UniqueCarrier").agg(mean("DepDelay"))
window = Window.partitionBy(dfq2_1["Origin"]).orderBy(dfq2_1["avg(DepDelay)"])
dfq2_1 = dfq2_1.select("*", rank().over(window).alias("rank")) \
        .filter(col("rank")<=10).where(dfq2_1["Origin"].isin('SRQ','CMH','JFK','SEA','BOS'))

query1 = (
    dfq1_2.writeStream.trigger(processingTime="1 seconds") \
    .outputMode("complete").option("truncate", "false") \
    .format("console") \
    .start()
)

query2 = (
    dfq1_3.writeStream.trigger(processingTime="1 seconds") \
    .outputMode("complete").option("truncate", "false") \
    .format("console") \
    .start()
)
"""
query3 = (
    dfq2_1.writeStream.trigger(processingTime="5 seconds") \
    .outputMode("complete").option("truncate", "false") \
    .format("console") \
    .start()
)
"""
stop_stream_query(query1, 10)
stop_stream_query(query2, 10)
#stop_stream_query(query3, 10)