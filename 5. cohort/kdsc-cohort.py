# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Cohort
# MAGIC
# MAGIC **Description** This notebook curates the CKD cohort in which a person is included if they have:
# MAGIC
# MAGIC - at least one CKD code form primary or secondary care
# MAGIC - have at least 2 eGFR â‰¤90 at least 90 days apart
# MAGIC - flag if they ever have a congenital, transplant or dialysis code
# MAGIC
# MAGIC All variables derived in the data_preprocessing stage are joined onto the cohort for use in the algorithm.
# MAGIC
# MAGIC Feature engineering is done here for use in the algorithm.
# MAGIC
# MAGIC **Author(s)** Anna Stevenson (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Reviewers** Jadene Lewis, Laura Sherlock (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Data Output** 
# MAGIC - **`{proj}_kdsc_cohort`**

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

# MAGIC %run "../0. parameters/kdsc-last_observable_date"

# COMMAND ----------

# MAGIC %md
# MAGIC # Data

# COMMAND ----------

egfr_df = spark.table(f'{dsa}.{proj}_kdsc_data_preprocessing_egfr_{algorithm_timestamp}')

acr_df = spark.table(f'{dsa}.{proj}_kdsc_data_preprocessing_acr_{algorithm_timestamp}')

ckd_df = spark.table(f'{dsa}.{proj}_kdsc_data_preprocessing_ckd_{algorithm_timestamp}')

date_of_diagnosis_df = spark.table(f'{dsa}.{proj}_kdsc_data_preprocessing_date_of_diagnosis_{algorithm_timestamp}')

# COMMAND ----------

demographics = (
    spark.table(f'{dsa}.{proj}_kdsc_curated_assets_demographics_{algorithm_timestamp}')
    .select("PERSON_ID",f.col("date_of_birth").alias("DATE_OF_BIRTH"),f.col("date_of_death").alias("DATE_OF_DEATH"),
                            f.col("sex").alias("SEX"),f.col("ethnicity_5_group").alias("ETHNICITY"),"in_gdppr")
)

# COMMAND ----------

main_cohort = (
    ckd_df
    .join(egfr_df,on="PERSON_ID",how="full")
    .select("PERSON_ID").distinct()
)

# COMMAND ----------

cohort = (

    main_cohort

    .join(ckd_df,on="PERSON_ID",how="left")

    .join(egfr_df,on="PERSON_ID",how="left")

    .join(acr_df,on="PERSON_ID",how="left")

    .join(demographics,on="PERSON_ID",how="left")

    .join(date_of_diagnosis_df,on="PERSON_ID",how="left")

    .withColumn("CKD_EVER_PRIMARY",f.when(f.col("CKD_EVER_PRIMARY").isNull(),0).otherwise(f.col("CKD_EVER_PRIMARY")))
    .withColumn("CKD_EVER_SECONDARY",f.when(f.col("CKD_EVER_SECONDARY").isNull(),0).otherwise(f.col("CKD_EVER_SECONDARY")))
    
    .withColumn("TRANSPLANT_EVER",f.when(f.col("TRANSPLANT_EVER").isNull(),0).otherwise(f.col("TRANSPLANT_EVER")))
    .withColumn("CONGENITAL_EVER",f.when(f.col("CONGENITAL_EVER").isNull(),0).otherwise(f.col("CONGENITAL_EVER")))
     .withColumn("DIALYSIS_EVER",f.when(f.col("DIALYSIS_EVER").isNull(),0).otherwise(f.col("DIALYSIS_EVER")))

    .withColumn("CKD_NO_PRIMARY",f.when(f.col("CKD_NO_PRIMARY").isNull(),0).otherwise(f.col("CKD_NO_PRIMARY")))
    .withColumn("CKD_NO_SECONDARY",f.when(f.col("CKD_NO_SECONDARY").isNull(),0).otherwise(f.col("CKD_NO_SECONDARY")))
  
    .withColumn("CKD_EVER",f.when(f.col("CKD_EVER").isNull(),0).otherwise(f.col("CKD_EVER")))
    
    .withColumn("CKD_NO",f.when(f.col("CKD_NO").isNull(),0).otherwise(f.col("CKD_NO")))

    .withColumn("flag_ckd_cohort",f.when(f.col("flag_ckd_cohort").isNull(),0).otherwise(f.col("flag_ckd_cohort")))

    #.withColumn("latest_egfr",f.when(f.col("latest_egfr").isNull(),0).otherwise(f.col("latest_egfr")))
    .withColumn("flag_egfr_cohort",f.when(f.col("flag_egfr_cohort").isNull(),0).otherwise(f.col("flag_egfr_cohort")))
   # .withColumn("latest_acr",f.when(f.col("latest_acr").isNull(),"none").otherwise(f.col("latest_acr")))
    .withColumn("flag_acr_cohort",f.when(f.col("flag_acr_cohort").isNull(),0).otherwise(f.col("flag_acr_cohort")))

    .withColumn("SEX",f.when(f.col("SEX").isNull(),f.lit("Unknown")).otherwise(f.col("SEX")))
    .withColumn("ETHNICITY",f.when(f.col("ETHNICITY").isNull(),f.lit("Unknown")).otherwise(f.col("ETHNICITY")))

    .withColumn("in_gdppr",f.when(f.col("in_gdppr").isNull(),0).otherwise(f.col("in_gdppr")))

    .filter(f.col('PERSON_ID').isNotNull())

)

# COMMAND ----------

if individual_censor_dates_flag==True:
    individual_censor_dates = (spark.table(individual_censor_dates_table)
    )
else:
    individual_censor_dates = (demographics
                               .select("PERSON_ID",f.col("DATE_OF_DEATH").alias("CENSOR_END"))
                               .filter(f.col("CENSOR_END").isNotNull())
                               )

# COMMAND ----------

    cohort = (
    cohort
    
    .join(individual_censor_dates, on="PERSON_ID", how="left")
    .withColumn("last_observable_date",f.to_date(f.lit(last_observable_date), "yyyy-MM-dd"))
    .withColumn("last_observable_date",f.when(f.col("CENSOR_END").isNull(),
                                              f.col("last_observable_date")).otherwise(f.col("CENSOR_END"))
    )
    # in cases where CENSOR_END > last_observable_date then last_observable_date is used
    .withColumn("last_observable_date",f.when(f.to_date(f.lit(last_observable_date), "yyyy-MM-dd") > f.col("CENSOR_END"),
                                              f.to_date(f.lit(last_observable_date), "yyyy-MM-dd")).otherwise(f.col("last_observable_date"))
    )
    # when study_end_date comes before LOS/DOD this is used
    .withColumn("last_observable_date",f.when(f.to_date(f.lit(study_end_date), "yyyy-MM-dd") < f.col("last_observable_date"),
                                              f.to_date(f.lit(study_end_date), "yyyy-MM-dd")).otherwise(f.col("last_observable_date"))
    )

    .withColumn("date_diagnosis_to_last_observable_date_lt",
                f. months_between(f.col("last_observable_date"),f.col("date_of_diagnosis"))/12)
    
    .withColumn("age_at_diagnosis",
                (f.months_between(f.col("date_of_diagnosis"), f.col("DATE_OF_BIRTH")) / 12))

                    .distinct()

)

# COMMAND ----------

display(cohort)

# COMMAND ----------

# MAGIC %md
# MAGIC # Save

# COMMAND ----------

save_table(df=cohort, out_name=f'{proj}_kdsc_cohort_{algorithm_timestamp}', save_previous=False)