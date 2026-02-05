# Databricks notebook source
# MAGIC %md
# MAGIC # Curated Assets - Albuminuria
# MAGIC
# MAGIC This notebook curates a table detailing an individual's complete history of albumin/creatinine ratio measurements (derived from GDPPR). 
# MAGIC
# MAGIC The following **data cleaning** has been applied:
# MAGIC - where a person has more than one reading on the same day, the mean is imputed
# MAGIC
# MAGIC
# MAGIC **Author(s)** Anna Stevenson (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Reviewers** Jadene Lewis, Laura Sherlock (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Data Output** 
# MAGIC - **`{proj}_kdsc_curated_assets_acr`** 

# COMMAND ----------

# MAGIC %md
# MAGIC # Set-up

# COMMAND ----------

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import pandas as pd
import numpy as np

import re
import io
import datetime

import matplotlib
import matplotlib.pyplot as plt
from matplotlib import dates as mdates
import seaborn as sns

print("Matplotlib version: ", matplotlib.__version__)
print("Seaborn version: ", sns.__version__)
_datetimenow = datetime.datetime.now() # .strftime("%Y%m%d")
print(f"_datetimenow:  {_datetimenow}")

import os

# COMMAND ----------

# MAGIC %run "../functions"

# COMMAND ----------

# MAGIC %run "../0. parameters/kdsc-parameters"

# COMMAND ----------

# MAGIC %md
# MAGIC # Codelist

# COMMAND ----------

codelist_path = f'/Workspace{os.path.dirname(os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()))}/1. codelists/acr_codelist.csv'
paths = [codelist_path]

spark_dfs = []

for path in paths:
    pandas_df = pd.read_csv(path, keep_default_na=False)
    spark_df = spark.createDataFrame(pandas_df)
    spark_dfs.append(spark_df)

if spark_dfs:
    codelist = spark_dfs[0]
    for df in spark_dfs[1:]:
        codelist = codelist.union(df)

acr_codelist = codelist.select(f.col("code").alias("CODE"))
acr_codelist = acr_codelist.withColumn("CODE", f.col("CODE").cast("string"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Data

# COMMAND ----------

gdppr_prepared = spark.table(f'{dsa}.{proj}_kdsc_curated_data_gdppr_{algorithm_timestamp}')

# COMMAND ----------

demographics = spark.table(f'{dsa}.{proj}_curated_assets_demographics_{algorithm_timestamp}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Curate

# COMMAND ----------

gdppr_acr = acr_codelist.join(gdppr_prepared,on="CODE",how="inner")

# COMMAND ----------

display(gdppr_acr.filter(f.col("VALUE2_CONDITION").isNotNull()).count())

# COMMAND ----------

acr_cohort = (
  gdppr_acr.drop("VALUE2_CONDITION")
  .withColumnRenamed("VALUE1_CONDITION","val")
 # .withColumn('val', f.round(f.col('val'), 0))
  .filter(f.col('val').isNotNull())
  .withColumnRenamed("val","acr")
  .distinct()
)

# COMMAND ----------

acr_cohort = (
    acr_cohort
    #ties - if more than one record on same day then take mean
    .groupBy("PERSON_ID", "DATE").agg(f.mean("acr").alias("acr"))
    .distinct()
)

# COMMAND ----------

display(acr_cohort)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save

# COMMAND ----------

save_table(df=acr_cohort, out_name=f'{proj}_kdsc_curated_assets_acr{algorithm_timestamp}', save_previous=False)