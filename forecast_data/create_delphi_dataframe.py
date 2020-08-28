import math
import us
from datetime import datetime, timedelta
import numpy as np
import logging
import pandas as pd
import os, sys, glob
from matplotlib import pyplot as plt
import us
import structlog
from functools import reduce
import os, sys

# import covidcast
# data2 = covidcast.signal("safegraph","full_time_work_prop", None, None, "state")
# data2 = covidcast.signal("safegraph","part_time_work_prop", None, None, "state")

CSV_REFORMAT_FOLDER = "/Users/natashawoods/Desktop/later.nosync/covid_act_now.nosync/covid-data-public/forecast_data/delphi/hard_format/"
CSV_FOLDER = "/Users/natashawoods/Desktop/later.nosync/covid_act_now.nosync/covid-data-public/forecast_data/delphi/easy_format/"

#Create output folder
CSV_FINAL_FOLDER = "/Users/natashawoods/Desktop/later.nosync/covid_act_now.nosync/covid-data-public/forecast_data/delphi/final_format/"
os.system(f'rm -r {CSV_FINAL_FOLDER}')
os.system(f'mkdir {CSV_FINAL_FOLDER}')
os.system(f'cp {CSV_FOLDER}/*csv {CSV_FINAL_FOLDER}')

aggregate_level_name = "aggregate_level"
aggregate_select = "state"

# load CAN input data
# can_data = "/Users/natashawoods/Desktop/later.nosync/covid_act_now.nosync/covid-data-model/pyseir_data/merged_results.csv"
can_data = "./CAN_DATA/merged_results_2020_08_27.csv"
can_df = pd.read_csv(can_data, converters={"fips": str}, parse_dates=True, index_col="date")


# Delphi Data
delphi_dataframes = []
# Mobility data that we don't currently cache
#full_time_safegraph = "/Users/natashawoods/Desktop/later.nosync/covid_act_now.nosync/covid-data-model/pyseir_data/state_full_time_work_prop.csv"
#part_time_safegraph = "/Users/natashawoods/Desktop/later.nosync/covid_act_now.nosync/covid-data-model/pyseir_data/state_part_time_work_prop.csv"


reformat_csv_files = glob.glob(CSV_REFORMAT_FOLDER + "*csv")
for thisfile in reformat_csv_files:
    var_name = thisfile[thisfile.rfind("/") + 1 : -4]
    print(var_name)
    full_df = pd.read_csv(thisfile, parse_dates=True)
    full_df["fips"] = full_df.apply(lambda x: us.states.lookup(x["geo_value"]).fips, axis=1)
    full_df.rename(columns={"time_value": "date", "value": var_name}, inplace=True)
    full_df["aggregate_level"] = "state"
    full_df["state"] = full_df.apply(lambda x: x["geo_value"].upper(), axis=1)
    full_df.to_csv(f"{var_name}.csv")
    os.system(f"mv {var_name}.csv {CSV_FINAL_FOLDER}")
    print(full_df.columns)
    print(full_df.head())




# delphi_dataframes.append(full_df)
# delphi_dataframes.append(part_df)

# Get list of all available CSV files
csv_files = glob.glob(CSV_FINAL_FOLDER + "*csv")
print(csv_files)


for delphi_file in csv_files:
    print(delphi_file)
    delphi_var_df = pd.read_csv(
        delphi_file, converters={"fips": str}, parse_dates=True, index_col="date"
    )

    delphi_var_df = delphi_var_df[delphi_var_df[aggregate_level_name] == aggregate_select]

    delphi_dataframes.append(delphi_var_df)
    print(delphi_var_df.columns)

merged_df = reduce(
    lambda left, right: pd.merge(
        left,
        right,
        left_on=["fips", "date", aggregate_level_name, aggregate_select],
        right_on=["fips", "date", aggregate_level_name, aggregate_select],
    ),
    delphi_dataframes,
)

merged_df.to_csv("merged_delphi_df.csv")
# Merge dataframes
final_merged_df = pd.merge(
    can_df,
    merged_df,
    how="left",
    left_on=["fips", "date", aggregate_level_name, aggregate_select],
    right_on=["fips", "date", aggregate_level_name, aggregate_select],
)

final_merged_df.to_csv("merged_delphi_df_latest.csv")

# merged_df["fips_int"] = merged_df["fips"].astype(int)
# merged_df.to_csv("delphi_merged.csv")
