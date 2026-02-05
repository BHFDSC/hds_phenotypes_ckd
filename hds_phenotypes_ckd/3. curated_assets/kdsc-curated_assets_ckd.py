# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Curated Assets - CKD codes from primary and secondary care
# MAGIC
# MAGIC **Description** This notebook curates a table detailing an individual's complete history of CKD codes from GDPPR, HES APC and HES APC Procedures. Code level detail.
# MAGIC
# MAGIC **Author(s)** Anna Stevenson (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Reviewers** Jadene Lewis, Laura Sherlock (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Acknowledgements** based on ddsc-curated_assets_diabetes (Fionna Chalmers)
# MAGIC
# MAGIC **Data Output** 
# MAGIC - **`{proj}_kdsc_curated_assets_ckd`**

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

import os

# COMMAND ----------

# MAGIC %run "../functions"

# COMMAND ----------

# MAGIC %run "../0. parameters/kdsc-parameters"

# COMMAND ----------

# MAGIC %md
# MAGIC # Codelists

# COMMAND ----------

ckd_path = f'/Workspace{os.path.dirname(os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()))}/1. codelists/ckd_update_jan2026.csv'

ckd_codelist = pd.read_csv(ckd_path, keep_default_na=False)
ckd_codelist = spark.createDataFrame(ckd_codelist)

display(ckd_codelist)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data

# COMMAND ----------

gdppr_prepared = spark.table(f'{dsa}.{proj}_kdsc_curated_data_gdppr_{algorithm_timestamp}')
hes_apc = spark.table(f'{dsa}.{proj}_kdsc_curated_data_hes_apc_{algorithm_timestamp}')
hes_apc_proc = spark.table(f'{dsa}.{proj}_kdsc_curated_data_hes_apc_proc{algorithm_timestamp}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Curate

# COMMAND ----------

hes_apc_ckd = (
    ckd_codelist
    .filter(col("terminology").contains("ICD10"))
    .join(hes_apc,on="CODE",how="inner")
    .select("PERSON_ID",f.col("EPISTART").alias("DATE"), "code", "description", "terminology", "validate", "transplant", "dialysis", "congenital")
    
)

gdppr_ckd = (
    ckd_codelist.join(gdppr_prepared,on="CODE",how="inner")
    .select("PERSON_ID",f.col("DATE"),"code","description", "terminology", "validate", "transplant", "dialysis", "congenital")
    
)

hes_apc_proc_ckd = (
    ckd_codelist
    .filter(f.col("terminology") == "OPCS4")
    .join(hes_apc_proc,on="CODE",how="inner")
    .select("person_id",f.col("epistart").alias("DATE"),"code","description", "terminology", "validate", "transplant", "dialysis", "congenital")
    
)

# COMMAND ----------

ckd = (
    gdppr_ckd
    .union(hes_apc_ckd)
    .union(hes_apc_proc_ckd)
    .distinct()
)

# COMMAND ----------

display(ckd)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save

# COMMAND ----------

save_table(df=ckd, out_name=f'{proj}_kdsc_curated_assets_ckd_{algorithm_timestamp}', save_previous=False)