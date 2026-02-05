# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Curated Data - HES APC Procedures
# MAGIC
# MAGIC **Description** This notebook curates a prepared version of HES APC procedures (OPCS4 codes) for use in all subsequent notebooks.
# MAGIC
# MAGIC The following data cleaning has been done:
# MAGIC - HES APC procedures filtered to the archived_on batch defined in parameters
# MAGIC - Any records dated 1800-01-01 or 1801-01-01 removed
# MAGIC - Records > than the last observable date removed
# MAGIC - Individual censor dates applied and for those who have died, records > date of death removed
# MAGIC - Null dates and PERSON_ID removed
# MAGIC
# MAGIC **Author(s)** Anna Stevenson (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Reviewers** Jadene Lewis, Laura Sherlock (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Data Output** 
# MAGIC - **`{proj}_kdsc_curated_data_hes_apc_proc`**

# COMMAND ----------

# MAGIC %md
# MAGIC # Set-up

# COMMAND ----------

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import pyspark.pandas as ps
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

# MAGIC %md
# MAGIC # Parameters

# COMMAND ----------

# MAGIC %run "../0. parameters/kdsc-parameters"

# COMMAND ----------

# MAGIC %run "../0. parameters/kdsc-last_observable_date"

# COMMAND ----------

# MAGIC %md
# MAGIC # Data

# COMMAND ----------

hes_apc_proc = spark.table(path_hes_apc_proc)
display(hes_apc_proc)

# COMMAND ----------

demographics = spark.table(f'{dsa}.{proj}_kdsc_curated_assets_demographics_{algorithm_timestamp}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look-ups

# COMMAND ----------

opcs4_lookup = spark.table('reference_data.dss_corporate.opcs_codes_v02')

# COMMAND ----------

display(opcs4_lookup)

# COMMAND ----------

opcs4_lookup_clean = (
    opcs4_lookup
    .select(f.col("ALT_OPCS_CODE").alias("CODE"), "OPCS_DESCRIPTION")
    .withColumn('CODE', f.regexp_replace('CODE', r'X$', ''))
    .withColumn('CODE', f.regexp_replace('CODE', r'[.,\-\s]', ''))
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Curate

# COMMAND ----------

if individual_censor_dates_flag==True:
    individual_censor_dates = spark.table(individual_censor_dates_table)
else:
    individual_censor_dates = demographics.select("PERSON_ID",f.col("DATE_OF_DEATH").alias("CENSOR_END")).filter(f.col("CENSOR_END").isNotNull())

# COMMAND ----------

hes_apc_proc_formatted = (
  hes_apc_proc
  # .join(opcs4_lookup_clean,on="CODE",how="left") #code descriptions
  .withColumn("EPISTART", f.when((f.col("epistart") == "1800-01-01") | (f.col("epistart") == "1801-01-01"), None).otherwise(f.col("epistart")))
  .where(f.col('epistart').isNotNull())
  .filter(f.col("epistart")<last_observable_date)
  #filter out records > DATE_OF_DEATH
  .join(individual_censor_dates,on="PERSON_ID",how="left")
  .withColumn("CENSOR_END", f.when(f.col("CENSOR_END").isNull(),last_observable_date).otherwise(f.col("CENSOR_END")))
  .filter(f.col("EPISTART")<=f.col("CENSOR_END"))
  .drop("CENSOR_END")
)

# COMMAND ----------

display(hes_apc_proc_formatted)

# COMMAND ----------

if individual_censor_dates_flag==True:
    hes_apc_proc_formatted = hes_apc_proc_formatted
else:
    hes_apc_proc_formatted = hes_apc_proc_formatted.filter(f.col("EPISTART")<study_end_date)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save

# COMMAND ----------

save_table(df=hes_apc_proc_formatted, out_name=f'{proj}_kdsc_curated_data_hes_apc_proc{algorithm_timestamp}', save_previous=False)