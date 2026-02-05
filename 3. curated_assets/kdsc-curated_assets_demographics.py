# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Curated Assets - Demographics
# MAGIC
# MAGIC **Description** This notebook curates a table detailing an individual's most recent demographic data
# MAGIC
# MAGIC The HDS curated asset is used.
# MAGIC
# MAGIC **Author(s)** Anna Stevenson (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Reviewers** Jadene Lewis, Laura Sherlock (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Acknowledgements** Based on ddsc-curated_assets_demographics (Fionna Chalmers)
# MAGIC
# MAGIC **Data Output** 
# MAGIC - **`kdsc_curated_assets_demographics_{algorithm_timestamp}`** 

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

# COMMAND ----------

# MAGIC %run "../functions"

# COMMAND ----------

# MAGIC %run "../0. parameters/kdsc-parameters"

# COMMAND ----------

# MAGIC %md
# MAGIC # Data

# COMMAND ----------

demographics = spark.table(path_demographics)
display(demographics)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save

# COMMAND ----------

save_table(df=demographics, out_name=f'{proj}_kdsc_curated_assets_demographics_{algorithm_timestamp}', save_previous=False)