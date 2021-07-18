import pandas as pd
import os
from pathlib import Path


KEEP = ["DayOfWeek","FlightDate","UniqueCarrier","FlightNum","Origin","Dest","CRSDepTime","DepTime","DepDelay","CRSArrTime","ArrTime","ArrDelay","Cancelled","Diverted"]

data_path = "/data/"
os.chdir(Path(data_path))
#allfile = os.listdir(Path(data_path))
allfile = ["On_Time_On_Time_Performance_2008_5.csv"]

count = 0
for f in allfile:
    if f=="readme.html" or f=="combined.csv" or f=="final_combined.csv":
        continue

    count += 1
    print("Processing No.{} file: ".format(count),f)

    df = pd.read_csv(f)
    print("before drop:", len(df))

    print(df.columns)
    df = df[df.columns[df.columns.isin(KEEP)]]

    df = df[(df["Cancelled"]==0) & (df["Diverted"]==0)]
    print("remove cancelled/diverted:", len(df))

    df = df.drop(columns=["Cancelled","Diverted"])
    df = df.dropna()
    print("drop na:", len(df), "\n")

    if count == 1:
        df.to_csv("/data/fianl_v2.csv", index = False, mode = "w")
    else:
        df.to_csv("/data/final_v2.csv", header = False, index = False, mode = "a")