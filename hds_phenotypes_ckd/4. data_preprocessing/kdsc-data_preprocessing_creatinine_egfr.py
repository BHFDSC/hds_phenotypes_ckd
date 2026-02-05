# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Data Preprocessing - Creatinine/eGFR cohort
# MAGIC
# MAGIC **Description** This notebook curates the creatinine/eGFR cohort in which someone must have at least 2 eGFR â‰¤90 at least 90 days apart.
# MAGIC
# MAGIC The following data cleaning/pre-processing has been done:
# MAGIC - readings before DOB removed
# MAGIC - remove those who do not have at least 2 consecutive eGFR â‰¤90 readings in their history 
# MAGIC
# MAGIC Feature engineering is then performed to establish the following:
# MAGIC - If at least 2 consecutive eGFR â‰¤90 (separated by at least 90 days) were ever found in a person's history
# MAGIC - The date at which the second eGFR â‰¤90 was found in a person's histroy 
# MAGIC
# MAGIC
# MAGIC
# MAGIC **Author(s)** Anna Stevenson (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Reviewers** Jadene Lewis, Laura Sherlock (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Data Output**
# MAGIC - **`{proj}_kdsc_data_preprocessing_creat_egfr_long`**
# MAGIC - **`{proj}_kdsc_data_preprocessing_creat_egfr`**

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

creatinine_egfr = spark.table(f'{dsa}.{proj}_kdsc_curated_assets_creatinine_egfr{algorithm_timestamp}')

# COMMAND ----------

demographics = spark.table(f'{dsa}.{proj}_curated_assets_demographics_{algorithm_timestamp}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Curate

# COMMAND ----------

creat_egfr_cleaned = (
        creatinine_egfr
        .join((demographics.select(f.col("PERSON_ID"),"date_of_birth")),how="left",on="PERSON_ID")
        .filter(f.col("DATE")>=f.col("date_of_birth"))
        .drop("date_of_birth")
)

# COMMAND ----------

creat_egfr_cleaned = (
    creat_egfr_cleaned
    .withColumn("egfr_class",
                f.when(f.col("egfr_creat") >90, "high")
                .when(f.col("egfr_creat") <=90, "low"))
)

# COMMAND ----------

# DBTITLE 1,identify if individuals have two eGFR <=90 at least 90days apart
window_spec = Window.partitionBy("PERSON_ID").orderBy("DATE")

tmp = creat_egfr_cleaned
tmp = (tmp
       .withColumn("prev_egfr_group", f.lag("egfr_class").over(window_spec))
       .withColumn("consecutive_low", 
                   f.when((f.col("egfr_class") == "low") & (f.col("prev_egfr_group") == "low"), 1).otherwise(0))
)

# ensure the first 2 consecutive dates are at least 90 days apart
window_spec_date = Window.partitionBy("PERSON_ID").orderBy("DATE")

tmp = (
    tmp
    .withColumn("PREV_DATE", f.lag("DATE").over(window_spec_date))
    .withColumn("DATE_DIFF", f.when(f.col("PREV_DATE").isNotNull(), f.datediff(f.col("DATE"), f.col("PREV_DATE"))).otherwise(None))
)

# COMMAND ----------

# DBTITLE 1,identify first consecutive low date
window_spec_row = Window.partitionBy("PERSON_ID").orderBy("DATE")

tmp = tmp.withColumn("row_num", f.row_number().over(window_spec_row))

first_consecutive_low = (
    tmp.filter( (f.col("consecutive_low") == 1) & (f.col("DATE_DIFF")>=90) )
                           .groupBy("PERSON_ID")
                           .agg({"DATE": "min"})
                           .withColumnRenamed("min(DATE)", "first_consecutive_low_date")
                           )

# COMMAND ----------

# DBTITLE 1,Retain latest eGFR measurement
window_desc = Window.partitionBy("PERSON_ID").orderBy(f.col("DATE").desc())

most_recent_egfr = (
    tmp
    .withColumn("rn_recent", f.row_number().over(window_desc))
.filter(f.col("rn_recent") == 1)
.select(
        f.col("PERSON_ID"),
        f.col("DATE").alias("latest_egfr_date"),
        f.col("egfr_creat").alias("latest_egfr")
)
)


# first_consecutive_low = (
#     first_consecutive_low
#     .join(most_recent_egfr, on="PERSON_ID", how="left")
# )

# COMMAND ----------

tmp = (
    tmp
    .join(first_consecutive_low, "PERSON_ID", "left")
    .withColumn("first_consecutive_low_flag_second", 
                   f.when((f.col("DATE") == f.col("first_consecutive_low_date")) & (f.col("consecutive_low") == 1), 1).otherwise(0))
    .drop("prev_egfr_group", "consecutive_low", "row_num", "first_consecutive_low_date")
)

egfr_low_with_flag = (
    (creat_egfr_cleaned.filter(f.col("egfr_class")=="low"))
    .join( (tmp.drop("egfr_class").filter(f.col('first_consecutive_low_flag_second')==1)),on=["PERSON_ID","DATE","egfr_creat"],how="left")
    .orderBy(["PERSON_ID","DATE"])
    )

# COMMAND ----------

# remove people in the low cohort who do have low records but never 2 consecutive
egfr_low_consec_only = (
    egfr_low_with_flag
    .filter(f.col('first_consecutive_low_flag_second')==1).select("PERSON_ID")
    .join(egfr_low_with_flag,on="PERSON_ID",how="left")
    .orderBy(["PERSON_ID","DATE"])
    )

window_spec = Window.partitionBy("PERSON_ID").orderBy("DATE")

egfr_low_consec_only_first = (
   egfr_low_consec_only.withColumn("NEXT_FLAG", f.lag("first_consecutive_low_flag_second", -1).over(window_spec))
    .withColumn("first_consecutive_low_flag", (f.col("NEXT_FLAG") == 1).cast("int"))
    .drop("NEXT_FLAG","first_consecutive_low_flag_second")
)


window_spec = Window.partitionBy("PERSON_ID").orderBy("DATE").rowsBetween(Window.unboundedPreceding, Window.currentRow)

egfr_low_consec_only_first=(egfr_low_consec_only_first.withColumn("has_low_flag", f.max(f.when(f.col("first_consecutive_low_flag") == 1, 1).otherwise(0)).over(window_spec)))


# COMMAND ----------

# Date of first record
low_egfr_date_first = (
  egfr_low_consec_only_first
    .filter(f.col("first_consecutive_low_flag")==1)
    .select("PERSON_ID",f.col("DATE").alias("EGFR_LOW_DATE_FIRST"))
)


# No of low Records in history
low_egfr_no = (
    egfr_low_consec_only_first.select("PERSON_ID","DATE").groupBy("PERSON_ID").count().withColumnRenamed("count","EGFR_LOW_NO")
    .withColumn("EGFR_LOW_EVER",f.lit(1))
)

# Last Date
_win_last = Window\
  .partitionBy('PERSON_ID')\
  .orderBy(f.col('DATE').desc())

low_egfr_date_last = (
    egfr_low_consec_only_first.select("PERSON_ID","DATE")
    .withColumn('_rownum', f.row_number().over(_win_last))
    .where(f.col('_rownum') == 1)
    .drop("_rownum")
    .withColumnRenamed("DATE","EGFR_LOW_DATE_LAST")
)

# Combine
egfr_low = (
    low_egfr_no
    .join(low_egfr_date_first,on="PERSON_ID",how="outer")
    .join(low_egfr_date_last,on="PERSON_ID",how="outer")
    .select("PERSON_ID","EGFR_LOW_EVER","EGFR_LOW_NO","EGFR_LOW_DATE_FIRST","EGFR_LOW_DATE_LAST")
    # flag for if in egfr cohort
    .withColumn("flag_egfr_cohort",f.lit(1))
)

# COMMAND ----------

# DBTITLE 1,Join to most recent eGFR

egfr_low = (
    egfr_low
    .join(most_recent_egfr, on="PERSON_ID", how="left")
)

# COMMAND ----------

egfr_low.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Save

# COMMAND ----------

# save_table(df=egfr_flags, out_name=f'{proj}_kdsc_data_preprocessing_egfr_long_{algorithm_timestamp}', save_previous=False)

# COMMAND ----------

save_table(df=egfr_low, out_name=f'{proj}_kdsc_data_preprocessing_egfr_{algorithm_timestamp}', save_previous=False)