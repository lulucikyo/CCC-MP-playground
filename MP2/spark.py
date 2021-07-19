import findspark
import os
import pyspark

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2'
findspark.init()

spark = SparkSession \
    .builder \
    .appName("MP2") \
    .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "b-1.mp2-1.5xqfr1.c3.kafka.us-east-1.amazonaws.com:9092,b-2.mp2-1.5xqfr1.c3.kafka.us-east-1.amazonaws.com:9092,b-3.mp2-1.5xqfr1.c3.kafka.us-east-1.amazonaws.com:9092") \
  .option("subscribe", "my-topic") \
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
                    StructField("ArrDelay", DoubleType(), True)
                    ])

df = df.select(from_json(df.value, schema).alias("json"))
df = df.select(col("json.*"))

dfq2 = df.groupby("UniqueCarrier").agg(mean("ArrDelay")) \
        .orderBy("avg(ArrDelay)").select("UniqueCarrier", "avg(ArrDelay)") \
        .limit(10)

query = (
    dfq2.writeStream \
    .outputMode("append").option("truncate", "false") \
    .format("console") \
    .start()
)



query.awaitTermination()
