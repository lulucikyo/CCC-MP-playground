import os
import pyspark
import time

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *


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

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2'

spark = SparkSession \
    .builder \
    .appName("MP2") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "b-1.mp2-3.bfzneb.c3.kafka.us-east-1.amazonaws.com:9092,b-2.mp2-3.bfzneb.c3.kafka.us-east-1.amazonaws.com:9092") \
  .option("subscribe", "alldata") \
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


# Question 2.1
dfq1 = df.groupby("Origin", "UniqueCarrier").agg(mean("DepDelay"))
dfq1_1 = dfq1.where(col("Origin")=='SRQ').orderBy(col("avg(DepDelay)")).limit(10)
dfq1_2 = dfq1.where(col("Origin")=='CMH').orderBy(col("avg(DepDelay)")).limit(10)
dfq1_3 = dfq1.where(col("Origin")=='JFK').orderBy(col("avg(DepDelay)")).limit(10)
dfq1_4 = dfq1.where(col("Origin")=='SEA').orderBy(col("avg(DepDelay)")).limit(10)
dfq1_5 = dfq1.where(col("Origin")=='BOS').orderBy(col("avg(DepDelay)")).limit(10)


query1 = (
    dfq1_1.writeStream \
    .outputMode("complete").option("truncate", "false") \
    .format("console") \
    .start()
)

query2 = (
    dfq1_2.writeStream \
    .outputMode("complete").option("truncate", "false") \
    .format("console") \
    .start()
)

query3 = (
    dfq1_3.writeStream \
    .outputMode("complete").option("truncate", "false") \
    .format("console") \
    .start()
)

query4 = (
    dfq1_4.writeStream \
    .outputMode("complete").option("truncate", "false") \
    .format("console") \
    .start()
)

query5 = (
    dfq1_5.writeStream \
    .outputMode("complete").option("truncate", "false") \
    .format("console") \
    .start()
)


stop_stream_query(query1, 5)
stop_stream_query(query2, 5)
stop_stream_query(query3, 5)
stop_stream_query(query4, 5)
stop_stream_query(query5, 5)