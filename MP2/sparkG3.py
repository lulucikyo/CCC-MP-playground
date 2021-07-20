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
  .option("kafka.bootstrap.servers", "b-1.mp2-2.bd6aae.c3.kafka.us-east-1.amazonaws.com:9092,b-2.mp2-2.bd6aae.c3.kafka.us-east-1.amazonaws.com:9092,b-3.mp2-2.bd6aae.c3.kafka.us-east-1.amazonaws.com:9092") \
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

# Question 3.2
dfnew = df.withColumn("CRSDepTime", lpad(df["CRSDepTime"],4,"0"))
dfnew = dfnew.filter(col("FlightDate").substr(1,4)=="2008")
dfnew = dfnew.withColumn("ArrDelay", col("ArrDelay").cast("double"))
dfnew.printSchema()


df1 = dfnew.filter(col("CRSDepTime")<"1200")
df1 = df1.select(col("Origin"), col("Dest"), concat(col('UniqueCarrier'),lit(" "),col('FlightNum')).alias("Flight"), 
                col("ArrDelay"), 
                to_timestamp(concat(col("FlightDate"),col("CRSDepTime")), "yyyy-MM-ddHHmm").alias("CRSDep"), 
                to_date(col("FlightDate"), "yyyy-MM-dd").alias("Date"))
df1 = df1.alias("l")
dfgroupby1 = df1.groupBy("Origin", "Dest", "Date").agg(min("ArrDelay")).alias("ll")
cond = [col("l.Origin")==col("ll.Origin"),
        col("l.Dest")==col("ll.Dest"),
        col("l.Date")==col("ll.Date"),
        col("l.ArrDelay")==col("ll.min(ArrDelay)")
        ]
df1 = df1.join(dfgroupby1, cond, "inner")
df1.printSchema()

df2 = dfnew.filter(col("CRSDepTime")>"1200")
df2 = df2.select(col("Origin"), col("Dest"), concat(col('UniqueCarrier'),lit(" "),col('FlightNum')).alias("Flight"), 
                col("ArrDelay"), 
                to_timestamp(concat(col("FlightDate"),col("CRSDepTime")), "yyyy-MM-ddHHmm").alias("CRSDep"), 
                to_date(col("FlightDate"), "yyyy-MM-dd").alias("Date"))
df2 = df2.alias("r")
dfgroupby2 = df2.groupBy("Origin", "Dest", "Date").agg(min("ArrDelay")).alias("rr")
cond = [col("r.Origin")==col("rr.Origin"),
        col("r.Dest")==col("rr.Dest"),
        col("r.Date")==col("rr.Date"),
        col("r.ArrDelay")==col("rr.min(ArrDelay)")
        ]
df2 = df2.join(dfgroupby2, cond, "inner")
df2.printSchema()

cond = [col("l.Dest")==col("r.Origin"), datediff(col("r.CRSDep"), col("l.CRSDep"))==2]
dfjoin = df1.join(df2, cond)

df3 = dfjoin.select("l.Origin", "l.Dest","l.Flight", \
                    date_format(col("l.CRSDep"),"HH:mm dd/MM/yyyy").alias("l.CRSDep"), \
                    "l.ArrDelay", "r.Origin", "r.Dest","r.Flight", \
                    date_format(col("r.CRSDep"),"HH:mm dd/MM/yyyy").alias("r.CRSDep"), \
                    "r.ArrDelay", \
                    (col("l.ArrDelay")+col("r.ArrDelay")).alias("TotDelay")) 

df3_1 = df3.where("l.Origin=='BOS' and l.Dest=='ATL' and r.Dest=='LAX' and l.CRSDep LIKE '%03/04/2008'")
df3_2 = df3.where("l.Origin=='PHX' and l.Dest=='JFK' and r.Dest=='MSP' and l.CRSDep LIKE '%07/09/2008'")
df3_3 = df3.where("l.Origin=='DFW' and l.Dest=='STL' and r.Dest=='ORD' and l.CRSDep LIKE '%24/01/2008'")
df3_4 = df3.where("l.Origin=='LAX' and l.Dest=='MIA' and r.Dest=='LAX' and l.CRSDep LIKE '%16/05/2008'")

query6_1 = (
    df3_1.writeStream \
    .outputMode("complete").option("truncate", "false") \
    .format("console") \
    .start()
)

query6_2 = (
    df3_2.writeStream \
    .outputMode("complete").option("truncate", "false") \
    .format("console") \
    .start()
)

query6_3 = (
    df3_4.writeStream \
    .outputMode("complete").option("truncate", "false") \
    .format("console") \
    .start()
)

query6_4 = (
    df3_4.writeStream \
    .outputMode("complete").option("truncate", "false") \
    .format("console") \
    .start()
)


stop_stream_query(query6_1, 5)
stop_stream_query(query6_2, 5)
stop_stream_query(query6_3, 5)
stop_stream_query(query6_4, 5)