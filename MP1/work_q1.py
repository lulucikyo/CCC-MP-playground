import argparse
import logging
from operator import add
from random import random

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')


def run_q1():
    data_uri = "s3://aws-logs-784082445748-us-east-1/elasticmapreduce/j-29WKE6APA5GV2/final_combined.csv"
    with SparkSession.builder.appName("work1").getOrCreate() as spark:
        df = spark.read.options(header="True").csv(data_uri)
        dfq1 = df.groupby("UniqueCarrier").agg(F.mean("ArrDelay")).orderBy("avg(ArrDelay)", ascending=False).select("UniqueCarrier", "avg(ArrDelay)")
        dfq2 = df.groupby("DayOfWeek").agg(F.mean("ArrDelay")).orderBy("avg(ArrDelay)").select("DayOfWeek", "avg(ArrDelay)")
        dfq1.show()
        dfq2.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    run_q1()
