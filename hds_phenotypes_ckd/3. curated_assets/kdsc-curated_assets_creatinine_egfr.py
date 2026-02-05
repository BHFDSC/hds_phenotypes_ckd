# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Curated Assets - Creatinine
# MAGIC
# MAGIC **Description** This notebook curates a table detailing an individual's complete history of Creatinine readings (derived from GDPPR). eGFR is then derived from Creatinine using the below formula:
# MAGIC
# MAGIC  Sex | Creat. (mg/dL) | CKD-EPI Age-Sex Equation (2021) | 
# MAGIC  | ----------- | ----------- | ----------- | 
# MAGIC  | $$\text{Female}$$ | $$\leq 0.7$$ | $$142 \times (Creat/0.7)^{-0.241} \times 0.9938^{Age} \times 1.012$$ |
# MAGIC  |   | $$> 0.7$$ | $$142 \times (Creat/0.7)^{-1.200} \times 0.9938^{Age} \times 1.012$$ |
# MAGIC  | $$\text{Male}$$  | $$\leq 0.9$$ | $$142 \times (Creat/0.9)^{-0.302} \times 0.9938^{Age}$$ |
# MAGIC  |   | $$> 0.9$$ | $$142 \times (Creat/0.9)^{-1.200} \times 0.9938^{Age}$$ |
# MAGIC
# MAGIC The following **data cleaning** has been applied:
# MAGIC - records that are outwith realistic lower and upper limits removed (ie. remove creatinine readings <20 and >2000)
# MAGIC - where a person has more than one reading on the same day, the mean is imputed
# MAGIC
# MAGIC
# MAGIC **Author(s)** Anna Stevenson (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Reviewers** Jadene Lewis, Laura Sherlock (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Data Output** 
# MAGIC - **`{proj}_kdsc_curated_assets_creatinine_egfr`** 

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

codelist_path = f'/Workspace{os.path.dirname(os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()))}/1. codelists/creatinine_codelist.csv'
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

creatinine_codelist = codelist.select(f.col("code").alias("CODE"))

creatinine_codelist = creatinine_codelist.withColumn("CODE", f.col("CODE").cast("string"))

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

gdppr_creatinine = creatinine_codelist.join(gdppr_prepared,on="CODE",how="inner")

# COMMAND ----------

# DBTITLE 1,Check: no VALUE2_CONDITION values
display(gdppr_creatinine.filter(f.col("VALUE2_CONDITION").isNotNull()).count())

# COMMAND ----------

creatinine_cohort = (
  gdppr_creatinine.drop("VALUE2_CONDITION")
  .withColumnRenamed("VALUE1_CONDITION","val")
  .withColumn('val', f.round(f.col('val'), 0))
  .filter(f.col('val').isNotNull())
  .withColumnRenamed("val","creatinine")
  .distinct()
)

# COMMAND ----------

# DBTITLE 1,Check: % of records outside lower and upper limits
# lower = creatinine_cohort.filter(f.col("creatinine")<20).count()
# upper = creatinine_cohort.filter(f.col("creatinine")>2000).count()

# print(f'Number of records readings <20: {lower}')
# print(f'Number of records readings >2000: {upper}')

# COMMAND ----------

creatinine_cohort = (
    creatinine_cohort
    #remove records that are outwith limits (ie remove <20 and >2000)
    .filter(f.col("creatinine")>=20)
    .filter(f.col("creatinine")<=2000)
)

# COMMAND ----------

# DBTITLE 1,Check: % of records with ties
# ties = (
#     creatinine_cohort
#     .groupBy("PERSON_ID", "DATE").count()
#     .filter(f.col("count")>1)
#     .count()

# )

# window_spec = Window.partitionBy("PERSON_ID", "DATE")
# tmp = creatinine_cohort.withColumn("count", f.count("*").over(window_spec))
# ties = tmp.filter(f.col("count") > 1).select("PERSON_ID", "DATE").distinct().count()

# COMMAND ----------

# display(ties)

# COMMAND ----------

creatinine_cohort = (
    creatinine_cohort
    #ties - if more than one record on same day then take mean
    .groupBy("PERSON_ID", "DATE").agg(f.mean("creatinine").alias("creatinine"))
    .distinct()
)

# COMMAND ----------

# distinct_persons_date = (creatinine_cohort.select("PERSON_ID", "DATE").distinct().count())
# print(f'Number of ties as a %: {ties}, ({round(ties/distinct_persons_date*100,4)}%)')

# COMMAND ----------

# MAGIC %md
# MAGIC # eGFR

# COMMAND ----------

# DBTITLE 1,Define conversion rules
def convert_creatinine_units(col):
    # Given a column of creatinine values in micromoles/L (umol/L), converts it to mg/dL with a CF of 88.42
    return col/88.42

def derive_egfr_from_creatinine(creat, sex, age):
    return (f
        .when((sex == 'F') & (creat <= 0.7), (142 * (creat/0.7)**(-0.241) * (0.9938)**(age) * 1.012))
        .when((sex == 'F') & (creat > 0.7), (142 * (creat/0.7)**(-1.200) * (0.9938)**(age) * 1.012))
        .when((sex == 'M') & (creat <= 0.9), (142 * (creat/0.9)**(-0.302) * (0.9938)**(age)))
        .when((sex == 'M') & (creat > 0.9), (142 * (creat/0.9)**(-1.200) * (0.9938)**(age)))
    )


# COMMAND ----------

# DBTITLE 1,Join creatinine cohort to demographics
creatinine_demographics = creatinine_cohort.join(demographics,on="PERSON_ID",how="inner")

# COMMAND ----------

# DBTITLE 1,Create eGFR cohort
creatinine_egfr_cohort = (
    creatinine_demographics
    # Take the difference (in years) between the measurement date and DOB to obtain the age at measurement time
    .withColumn('age', f.datediff('DATE', 'date_of_birth')/365.25)
    # Convert the measurement units from umol/L to mg/dL
    .withColumn('creat_convert', convert_creatinine_units(f.col('creatinine')))
    # Apply the conversion rule to obtain eGFR from creatinine using creatinine, sex, and age at measurement time
    .withColumn('egfr_creat', derive_egfr_from_creatinine(
        f.col('creat_convert'), f.col('sex'), f.col('age')
    ))
)


# COMMAND ----------

creatinine_egfr_cohort = (
    creatinine_egfr_cohort
    .withColumn('egfr_group',
                f.when(f.col("egfr_creat") >=90,"â‰¥90")
                 .when((f.col("egfr_creat") >= 60) & (f.col("egfr_creat") <= 89), "60-89")
     .when((f.col("egfr_creat") >= 45) & (f.col("egfr_creat") <= 59), "45-59")
     .when((f.col("egfr_creat") >= 30) & (f.col("egfr_creat") <= 44), "30-44")
     .when((f.col("egfr_creat") >= 15) & (f.col("egfr_creat") <= 29), "15-29")
     .when(f.col("egfr_creat") < 15, "<15")
     .otherwise(None))
    .distinct()
)

# COMMAND ----------

creatinine_egfr_cohort = (
    creatinine_egfr_cohort
    .select("PERSON_ID","DATE","creatinine","egfr_creat", "egfr_group")
)

# COMMAND ----------

display(creatinine_egfr_cohort)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save

# COMMAND ----------

save_table(df=creatinine_egfr_cohort, out_name=f'{proj}_kdsc_curated_assets_creatinine_egfr{algorithm_timestamp}', save_previous=False)