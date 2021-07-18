import os
import zipfile
from pathlib import Path

data_path = "/data/"
os.chdir(Path(data_path))
allfile = os.listdir(Path(data_path))
exfile = ["On_Time_On_Time_Performance_2008_1.csv", "On_Time_On_Time_Performance_2008_10.csv"]

for fname in exfile:
    print(fname)
    f = open(fname, "r")
    head = f.readline()
    print("head:\n", head, "\n# of head cells:\n", len(head.split(",")))
    row1 = f.readline()
    print("row1:\n", row1, "\n# of row1 cells:\n", len(row1.split(",")))
    headl = head.split(",")
    row = row1.split(",")
    for i in range(len(headl)):
        print(i+1, "(",headl[i]," , ",row[i],")")
    print("remaining: ", row[-2], row[-1])
    f.close()