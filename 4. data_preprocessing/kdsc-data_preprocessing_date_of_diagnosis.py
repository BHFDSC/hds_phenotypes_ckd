# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Data Preprocessing - Date of Diagnosis
# MAGIC
# MAGIC **Description** This notebook curates the date of diagnosis, defined as the first CKD code (of any type) or the second eGFR â‰¤90 (at least 90 days after the first)
# MAGIC
# MAGIC **Author(s)** Anna Stevenson (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Reviewers** Jadene Lewis, Laura Sherlock (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Acknowledgements** Based on ddsc-data_preprocessing_date_of_diagnosis (Fionna Chalmers)
# MAGIC
# MAGIC **Data Output** 
# MAGIC - **`{proj}_kdsc_data_preprocessing_date_of_diagnosis`**

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

ckd = spark.table(f'{dsa}.{proj}_kdsc_data_preprocessing_ckd_{algorithm_timestamp}')

# COMMAND ----------

egfr = spark.table(f'{dsa}.{proj}_kdsc_data_preprocessing_egfr_{algorithm_timestamp}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Curate

# COMMAND ----------

date_of_diagnosis = (
    ckd.join(egfr,on="PERSON_ID",how="full")
    .withColumn(
    "date_of_diagnosis",
    f.when(
            f.col("EGFR_LOW_DATE_FIRST").isNotNull() ,
            f.col("EGFR_LOW_DATE_FIRST")
    ).otherwise(
        f.when(
            f.col("CKD_DATE_FIRST").isNotNull(),
            f.col("CKD_DATE_FIRST")
        ).otherwise(
            f.col("EGFR_LOW_DATE_FIRST")
        )
    )
)
    .select("PERSON_ID","date_of_diagnosis")
)

# COMMAND ----------

save_table(df=date_of_diagnosis, out_name=f'{proj}_kdsc_data_preprocessing_date_of_diagnosis_{algorithm_timestamp}', save_previous=False)