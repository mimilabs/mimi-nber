# Databricks notebook source

import pandas as pd

# The original data is downloaded from the below URL:
# https://www.nber.org/research/data/ssa-federal-information-processing-series-fips-state-and-county-crosswalk
fn = "/Volumes/mimi_ws_1/nber/src/ssa_fips_state_county_2023.csv"
pdf = pd.read_csv(fn, dtype={"fipscounty": str, 
                                "fy2023cbsa": str,
                                "ssa_code": str})
pdf["fy2023cbsa"] = pdf["fy2023cbsa"].str.slice(0, 5)
df = spark.createDataFrame(pdf)
(df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("mimi_ws_1.nber.ssa2fips_state_and_county"))

# COMMAND ----------


