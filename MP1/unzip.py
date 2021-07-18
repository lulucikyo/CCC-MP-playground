import os
import zipfile
from pathlib import Path


extract_path = "/data/"

"""
ex = ["/tdata/aviation/airline_ontime/2008/On_Time_On_Time_Performance_2008_1.zip",
"/tdata/aviation/airline_ontime/2008/On_Time_On_Time_Performance_2008_10.zip"]
for fname in ex:
    print(fname)
    with zipfile.ZipFile(fname, "r") as zip_ref:
        zip_ref.extractall(extract_path)
"""

tdata_path = "/tdata/aviation/airline_ontime/"
yr = os.listdir(Path(tdata_path))
for year in yr:
    yr_path = tdata_path+year
    flist = os.listdir(Path(yr_path))
    for f in flist:
        fname = yr_path+"/"+f
        print(fname)
        with zipfile.ZipFile(fname, "r") as zip_ref:
            zip_ref.extractall(extract_path)