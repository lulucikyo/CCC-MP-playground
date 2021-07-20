import os
import time

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window


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

spark = SparkSession \
    .builder \
    .appName("MP2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "b-1.mp2-3.bfzneb.c3.kafka.us-east-1.amazonaws.com:9092,b-2.mp2-3.bfzneb.c3.kafka.us-east-1.amazonaws.com:9092") \
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

query1 = (
    dfq1_2.writeStream \
    .outputMode("complete").option("truncate", "false") \
    .format("console").start()
)

# Question 1.3
dfq1_3 = df.groupby("DayOfWeek").agg(mean("ArrDelay")) \
        .orderBy("avg(ArrDelay)").select("DayOfWeek", "avg(ArrDelay)")

query2 = (
    dfq1_3.writeStream \
    .outputMode("complete").option("truncate", "false") \
    .format("console").start()
)


stop_stream_query(query1, 5)
stop_stream_query(query2, 5)