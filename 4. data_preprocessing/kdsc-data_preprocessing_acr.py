# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC %md
# MAGIC # Data Preprocessing - ACR cohort
# MAGIC
# MAGIC **Description** This notebook curates the ACR cohort in which ACR is categorised into <3, 3-30 and >30 for CKD staging.
# MAGIC
# MAGIC The following data cleaning/pre-processing has been done:
# MAGIC - readings before DOB removed
# MAGIC
# MAGIC Feature engineering is then performed to establish the following:
# MAGIC - The date at which the ACR categorisation was was found in a person's histroy 
# MAGIC
# MAGIC
# MAGIC
# MAGIC **Author(s)** Anna Stevenson (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Reviewers** Jadene Lewis, Laura Sherlock (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`{proj}_kdsc_data_preprocessing_acr_long`**
# MAGIC - **`{proj}_kdsc_data_preprocessing_acr`**

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

# COMMAND ----------

# MAGIC %run "../functions"

# COMMAND ----------

# MAGIC %run "../0. parameters/kdsc-parameters"

# COMMAND ----------

# MAGIC %md
# MAGIC # Data

# COMMAND ----------

acr = spark.table(f'{dsa}.{proj}_kdsc_curated_assets_acr{algorithm_timestamp}')

# COMMAND ----------

demographics = spark.table(f'{dsa}.{proj}_curated_assets_demographics_{algorithm_timestamp}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Curate

# COMMAND ----------

acr_cleaned = (
        acr
        .join((demographics.select(f.col("PERSON_ID"),"date_of_birth")),how="left",on="PERSON_ID")
        .filter(f.col("DATE")>=f.col("date_of_birth"))
        .drop("date_of_birth"))

# COMMAND ----------

acr = (
    acr
    .withColumn("acr_class",
                f.when(f.col("acr") <=3, "<3")
                .when((f.col("acr") > 3) & (f.col("acr") < 30), "3-30")
                .when(f.col("acr") >30, ">30"))
)

# COMMAND ----------

# DBTITLE 1,capture latest acr
window_desc = Window.partitionBy("PERSON_ID").orderBy(f.col("DATE").desc())

most_recent_acr = (
    acr
    .withColumn("rn_recent", f.row_number().over(window_desc))
.filter(f.col("rn_recent") == 1)
.select(
        f.col("PERSON_ID"),
        f.col("DATE").alias("latest_acr_date"),
        f.col("acr").alias("latest_acr")
)
)

most_recent_acr = most_recent_acr.withColumn("latest_acr", f.col("latest_acr").cast("double"))

# COMMAND ----------

# DBTITLE 1,flag for in acr cohort
most_recent_acr = (
    most_recent_acr
    .withColumn("flag_acr_cohort", f.lit(1))
)


# COMMAND ----------

display(most_recent_acr.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC # Save

# COMMAND ----------

save_table(df=most_recent_acr, out_name=f'{proj}_kdsc_data_preprocessing_acr_{algorithm_timestamp}', save_previous=False)