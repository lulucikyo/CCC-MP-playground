import pandas as pd
import os
from pathlib import Path


KEEP = ["Year", "Month","DayofMonth","DayOfWeek","FlightDate","UniqueCarrier","AirlineID","Origin","Dest","CRSDepTime","DepTime","DepDelay","DepDelayMinutes","CRSArrTime","ArrTime","ArrDelay","ArrDelayMinutes","Cancelled","Diverted"]
allfile = ["On_Time_Reporting_Carrier_On_Time_Performance_(1987_present)_2008_1.csv"]

count = 0
for f in allfile:
    if f=="readme.html" or f=="combined.csv" or f=="final_combined.csv":
        continue

    count += 1
    print("Processing No.{} file: ".format(count),f)

    df = pd.read_csv(f)
    print("before drop:", len(df))

    
    print(df.columns)
    print(df.columns.isin(KEEP))

    df = df[df.columns[df.columns.isin(KEEP)]]

    df = df[(df["Cancelled"]==0) & (df["Diverted"]==0)]
    print("remove cancelled/diverted:", len(df))

    df = df.drop(columns=["Cancelled","Diverted"])
    df = df.dropna()
    print("drop na:", len(df), "\n")

    if count == 1:
        df.to_csv("final_combined.csv", index = False, mode = "w")
    else:
        df.to_csv("final_combined.csv", header = False, index = False, mode = "a")