# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Data Preprocessing - CKD codes cohort
# MAGIC
# MAGIC **Description** This notebook curates the CKD codes cohort.
# MAGIC
# MAGIC The following data cleaning/pre-processing has been done:
# MAGIC - CKD records before DOB removed
# MAGIC
# MAGIC Feature enginerring is them performed to establish the following:
# MAGIC - identify (and remove) individuals who **only** have codes that need validated and no other CKD code
# MAGIC - If a code type was ever found in a persons history
# MAGIC - If an individual has any CKD stage codes
# MAGIC - The date at which the first code was found in a person's histroy
# MAGIC - The date at which the last code was found in a person's history
# MAGIC
# MAGIC
# MAGIC **Author(s)** Anna Stevenson (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Reviewers** Jadene Lewis, Laura Sherlock (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Data Output** 
# MAGIC - **`{proj}_kdsc_data_preprocessing_ckd_long_{algorithm_timestamp}`**
# MAGIC - **`{proj}_kdsc_data_preprocessing_ckd_{algorithm_timestamp}`**

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

ckd_codes = spark.table(f'{dsa}.{proj}_kdsc_curated_assets_ckd_{algorithm_timestamp}')

# COMMAND ----------

demographics = spark.table(f'{dsa}.{proj}_curated_assets_demographics_{algorithm_timestamp}')

# COMMAND ----------

display(ckd_codes)

# COMMAND ----------

# MAGIC %md
# MAGIC # Curate

# COMMAND ----------

ckd_cleaned = (
        ckd_codes
     #   .drop("code")
        .distinct()
        .join((demographics.select(f.col("PERSON_ID"),"date_of_birth")),how="left",on="PERSON_ID")
        .filter(f.col("DATE")>=f.col("date_of_birth"))
)

# COMMAND ----------

display(ckd_cleaned.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC # Validate codes

# COMMAND ----------

# For each person, check the minimum of validate column (if min_validate == 1, then all their codes have validate = 1 so need removed)
by_id = (
    ckd_cleaned.groupBy("PERSON_ID")
      .agg(f.min("validate").alias("min_validate"))
)

# IDs where all validate == 1
to_remove_ids = (
    by_id.filter(f.col("min_validate") == 1)
         .select("PERSON_ID")
)

ckd_cleaned_validated = ckd_cleaned.join(to_remove_ids, on="PERSON_ID", how="left_anti")

# COMMAND ----------

display(ckd_cleaned_validated)

# COMMAND ----------

## CHECKS ##
# check removed individuals
removed = ckd_cleaned.join(to_remove_ids, on="PERSON_ID", how="inner")

# get summary table
removed_summary = (
    removed
      .groupBy("PERSON_ID")
      .agg(
          f.collect_set("code").alias("codes"),        
          f.collect_set("validate").alias("validates"),# sanity check - should all be 1
          f.count("*").alias("n_rows")               
      )
)

display(removed_summary)


# COMMAND ----------

tab(removed_summary, "n_rows")

# COMMAND ----------

# number of individuals removed
display(n_removed_ids = to_remove_ids.count())


# distinct codes and n individuals/rows
removed_codes_summary = (
    removed
    .groupBy("code")
             .agg(
                 f.countDistinct("PERSON_ID").alias("n_individuals"),
                 f.count("*").alias("n_rows")
             )
)

display(removed_codes_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC # CKD stage codes

# COMMAND ----------

# DBTITLE 1,Identify codes with stage descriptions and create column to flag
# define regex
regex = r"(?i)stage\s*(\d)"

ckd_cleaned_validated = ckd_cleaned_validated.withColumn(
    "ckd_stage_code",
    f.when(
        f.regexp_extract(f.col("description"), regex, 1) != "",
        f.concat(f.lit("Stage "), f.regexp_extract("description", regex, 1))
    ).otherwise(None)
)

# COMMAND ----------

# DBTITLE 1,Identify most recent stage code
w = Window.partitionBy("PERSON_ID").orderBy(f.col("DATE").desc())

ckd_cleaned_validated = ckd_cleaned_validated.withColumn(
    "latest_ckd_stage",
    f.first("ckd_stage_code", ignorenulls=True).over(w)
)

# COMMAND ----------

display(ckd_cleaned_validated)

# COMMAND ----------

# MAGIC %md
# MAGIC # Wrangle

# COMMAND ----------

ckd_wrangled = (

    ckd_cleaned_validated

    .withColumn("CKD_EVER_PRIMARY", f.when((f.col("terminology") == "SNOMED"), 1).otherwise(0))

    .withColumn("CKD_EVER_SECONDARY", f.when((f.col("terminology") == "ICD10") | (f.col("terminology") == "OPCS4"), 1).otherwise(0))

   # .withColumn("CONGENITAL_EVER", f.when((f.col("congenital") == "1"), 1).otherwise(0))

    #.withColumn("TRANSPLANT_EVER", f.when((f.col("transplant") == "1"), 1).otherwise(0))

    #.withColumn("DIALYSIS_EVER", f.when((f.col("dialysis") == "1"), 1).otherwise(0))

    .groupBy("PERSON_ID").agg(

    (f.max(f.col("CKD_EVER_PRIMARY")) > 0).cast("int").alias("CKD_EVER_PRIMARY"),
    (f.max(f.col("CKD_EVER_SECONDARY")) > 0).cast("int").alias("CKD_EVER_SECONDARY"),
    (f.max(f.col("latest_ckd_stage")).alias("latest_ckd_stage")),
    (f.max(f.col("transplant")) > 0).cast("int").alias("TRANSPLANT_EVER"),
    (f.max(f.col("congenital")) > 0).cast("int").alias("CONGENITAL_EVER"),
    (f.max(f.col("dialysis")) > 0).cast("int").alias("DIALYSIS_EVER"),

    f.count(f.when((f.col("terminology") == "SNOMED"), True)).alias("CKD_NO_PRIMARY"),
    f.count(f.when((f.col("terminology") == "ICD10") | (f.col("terminology") == "OPCS4"), True)).alias("CKD_NO_SECONDARY"),
     
    f.min(f.when((f.col("terminology") == "SNOMED"), f.col("DATE"))).alias("CKD_DATE_FIRST_PRIMARY"),
    f.min(f.when((f.col("terminology") == "ICD10") | (f.col("terminology") == "OPCS4"), f.col("DATE"))).alias("CKD_DATE_FIRST_SECONDARY"),
   
    f.max(f.when((f.col("terminology") == "SNOMED"), f.col("DATE"))).alias("CKD_DATE_LAST_PRIMARY"),
    f.max(f.when((f.col("terminology") == "ICD10") | (f.col("terminology") == "OPCS4"), f.col("DATE"))).alias("CKD_DATE_LAST_SECONDARY"),
  
)

    .withColumn("CKD_EVER", f.when((f.col("CKD_EVER_PRIMARY") == 1) | (f.col("CKD_EVER_SECONDARY") == 1), 1).otherwise(0))
 
    .withColumn("CKD_NO", f.col("CKD_NO_PRIMARY") + f.col("CKD_NO_SECONDARY"))

    .withColumn("CKD_DATE_FIRST", f.least(f.col("CKD_DATE_FIRST_PRIMARY"), f.col("CKD_DATE_FIRST_SECONDARY")))

    .withColumn("CKD_DATE_LAST", f.greatest(f.col("CKD_DATE_LAST_PRIMARY"), f.col("CKD_DATE_LAST_SECONDARY"))) 


    # flag for if in CKD codes cohort
    .withColumn("flag_ckd_cohort",f.lit(1))

)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save

# COMMAND ----------

save_table(df=ckd_wrangled, out_name=f'{proj}_kdsc_data_preprocessing_ckd_{algorithm_timestamp}', save_previous=False)


# COMMAND ----------

# save_table(df=ckd_cleaned, out_name=f'{proj}_kdsc_data_preprocessing_ckd_long_{algorithm_timestamp}', save_previous=False)