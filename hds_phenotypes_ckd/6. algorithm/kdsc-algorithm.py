# Databricks notebook source
# MAGIC %md
# MAGIC # KDSC Algorithm - PySpark
# MAGIC
# MAGIC **Description** This notebook runs the KDSC Algorithm in PySpark
# MAGIC
# MAGIC **Author(s)** Anna Stevenson (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Reviewers** Jadene Lewis, Laura Sherlock (Health Data Science Team, BHF Data Science Centre)
# MAGIC
# MAGIC **Data Output** 
# MAGIC - **`{proj}_kdsc_cohort_out`**

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
# MAGIC # Data

# COMMAND ----------

# Cohort In
cohort = spark.table(f'{dsa}.{proj}_kdsc_cohort_{algorithm_timestamp}')

# COMMAND ----------

# MAGIC %md
# MAGIC # Algorithm

# COMMAND ----------

# Step 1: *Only* eGFR >90 OR no eGFR records
cohort = cohort.withColumn("step_1",
                           f.when((f.col("latest_egfr") >90) | (f.col("latest_egfr").isNull()), "Yes")
                           .otherwise("No"))

# Step 2: Any ACR records
cohort = cohort.withColumn("step_2",
                           f.when((f.col("step_1") == "Yes") & 
                                  (f.col("latest_acr") >=0), "Yes")
                           .when((f.col("step_1") == "Yes") & (f.col("latest_acr").isNull()), "No")
                           .otherwise(None))        
                           
# Step 2.1: Latest ACR <=3
cohort = cohort.withColumn("step_2_1",
                           f.when((f.col("step_2") == "Yes") & 
                                  (f.col("latest_acr") <=3), "Yes")
                           .when((f.col("step_2") == "Yes") & (f.col("latest_acr") >3), "No")
                           .otherwise(None))

# Step 2.2: Latest ACR 3-30
cohort = cohort.withColumn("step_2_2",
                           f.when((f.col("step_2_1") == "No") & 
                                  (f.col("latest_acr").between(3, 30)), "Yes")
                           .when((f.col("step_2_1") == "No") & (f.col("latest_acr") >30), "No")
                           .otherwise(None))              


# Step 2.3: Latest ACR >30
cohort = cohort.withColumn("step_2_3",
                           f.when((f.col("step_2_2") == "No") & 
                                  (f.col("latest_acr") >30), "Yes")
                           .when((f.col("step_2_2") == "No") & (f.col("latest_acr") < 30), "No")
                           .otherwise(None))                                                                                        

# Step 3: Any CKD stage codes
cohort = cohort.withColumn("step_3",
                           f.when((f.col("step_2") == "No") & 
                                  (f.col("latest_ckd_stage").isNotNull()), "Yes")
                           .when((f.col("step_2") == "No") & (f.col("latest_ckd_stage").isNull()), "No")
                           .otherwise(None))                           

# Step 4.1: eGFR 60-89
cohort = cohort.withColumn("step_4_1",
                           f.when((f.col("step_1") == "No") & f.col("latest_egfr").between(60, 90), "Yes")
                           .when((f.col("step_1") == "No") & (f.col("latest_egfr") < 60), "No")
                           .otherwise(None))

# Step 4.2: eGFR 45-59
cohort = cohort.withColumn("step_4_2",
                           f.when((f.col("step_4_1") == "No") & f.col("latest_egfr").between(45,60), "Yes")
                           .when((f.col("step_4_1") == "No") & (f.col("latest_egfr") <45), "No")
                           .otherwise(None))

                           
# Step 4.3: eGFR 30-44
cohort = cohort.withColumn("step_4_3",
                           f.when((f.col("step_4_2") == "No") & f.col("latest_egfr").between(30,45), "Yes")
                           .when((f.col("step_4_2") == "No") & (f.col("latest_egfr") <30), "No")
                           .otherwise(None))


# Step 4.4: eGFR 15-29
cohort = cohort.withColumn("step_4_4",
                           f.when((f.col("step_4_3") == "No") & f.col("latest_egfr").between(15,30), "Yes")
                           .when((f.col("step_4_3") == "No") & (f.col("latest_egfr") <15), "No")
                           .otherwise(None))


# Step 4.5: eGFR <15
cohort = cohort.withColumn("step_4_5",
                           f.when((f.col("step_4_4") == "No") & 
                                  ((f.col("latest_egfr") <15) & (f.col("latest_egfr") !=0)), "Yes")
                           .when((f.col("step_4_4") == "No") & (f.col("latest_egfr") ==0), "No")
                           .otherwise(None))


# Step 5.1: G staging + ACR <=3
cohort = cohort.withColumn("step_5_1",
                           f.when(((f.col("step_4_1") == "Yes") |  (f.col("step_4_2") == "Yes") |
                                  (f.col("step_4_3") == "Yes") | (f.col("step_4_4") == "Yes") |
                                  (f.col("step_4_5") == "Yes")) & (f.col("latest_acr") <=3) & 
                                  (f.col("latest_acr") >=0), "Yes")
                           .when(((f.col("step_4_1") == "Yes") |  (f.col("step_4_2") == "Yes") |
                                  (f.col("step_4_3") == "Yes") | (f.col("step_4_4") == "Yes") |
                                  (f.col("step_4_5") == "Yes")) & (f.col("latest_acr") >3), "No")
                           .otherwise(None))


# Step 5.2: G staging + ACR 3-30
cohort = cohort.withColumn("step_5_2",
                            f.when((f.col("step_5_1") == "No") & 
                                  (f.col("latest_acr").between(3,30)), "Yes") 
                            .when((f.col("step_5_1") == "No") & (f.col("latest_acr") >30), "No")     
                            .otherwise(None))                            
                     

# Step 5.3 G staging + ACR >30
cohort = cohort.withColumn("step_5_3",
                           f.when((f.col("step_5_2") == "No") & 
                                  (f.col("latest_acr") >30), "Yes") 
                           .when((f.col("step_5_2") == "No") & (f.col("latest_acr") <30), "No")     
                           .otherwise(None)) 



# COMMAND ----------

# MAGIC %md
# MAGIC # Results

# COMMAND ----------

# Create CKD Variable
cohort = cohort.withColumn(
    "out_ckd",
    
    # CKD A1
    f.when(
        ((f.col("step_1") == "Yes") & (f.col("step_2") == "Yes") & (f.col("step_2_1") == "Yes")
        ), "CKD A1")

   # CKD A2
    .when(((f.col("step_1") == "Yes") & (f.col("step_2") == "Yes") & (f.col("step_2_1") == "No") & (f.col("step_2_2") == "Yes")), "CKD A2")

    # CKD A3
    .when(((f.col("step_1") == "Yes") & (f.col("step_2") == "Yes") & (f.col("step_2_1") == "No") & (f.col("step_2_2") == "No") & (f.col("step_2_3") == "Yes")), "CKD A3")

    # CKD by stage code only
    .when(((f.col("step_1") == "Yes") & (f.col("step_2") == "No") & (f.col("step_3") == "Yes")), "CKD by stage code only")

    # CKD unclassified
    .when(((f.col("step_1") == "Yes") & (f.col("step_2") == "No") & (f.col("step_3") == "No")), "CKD unclassified")

    # CKD G2 flag
    .when(((f.col("step_1") == "No") & (f.col("step_4_1") == "Yes")), "CKD G2")

    # CKD G3a flag
    .when(((f.col("step_1") == "No") & (f.col("step_4_1") == "No") & (f.col("step_4_2") == "Yes")), "CKD G3a")

    # CKD G3b flag
    .when(((f.col("step_1") == "No") & (f.col("step_4_1") == "No") & (f.col("step_4_2") == "No") & (f.col("step_4_3") == "Yes")), "CKD G3b")

    # CKD G4 flag
    .when(((f.col("step_1") == "No") & (f.col("step_4_1") == "No") & (f.col("step_4_2") == "No") & (f.col("step_4_3") == "No") & (f.col("step_4_4") == "Yes")), "CKD G4")

    # CKD G5 flag
    .when(((f.col("step_1") == "No") & (f.col("step_4_1") == "No") & (f.col("step_4_2") == "No") & (f.col("step_4_3") == "No") & (f.col("step_4_4") == "No") & (f.col("step_4_5") == "Yes") | (f.col("DIALYSIS_EVER") == 1)), "CKD G5")
  
.otherwise(None))

# COMMAND ----------

# DBTITLE 1,Append ACR staging
cohort = cohort.withColumn(
    "out_ckd",

# A1
f.when(((f.col("step_1") == "Yes") | (f.col("step_4_1") == "Yes") | (f.col("step_4_2") == "Yes") | (f.col("step_4_3") == "Yes") | (f.col("step_4_4") == "Yes") | (f.col("step_4_5") == "Yes")) & 
    (f.col("step_5_1") == "Yes"),
          f.concat_ws(
              "; ",
              f.col("out_ckd"),
              f.lit("A1"))
          
          ).otherwise(f.col("out_ckd"))
)

# A2
cohort = cohort.withColumn(
    "out_ckd",

f.when(((f.col("step_1") == "Yes") | (f.col("step_4_1") == "Yes") | (f.col("step_4_2") == "Yes") | (f.col("step_4_3") == "Yes") | (f.col("step_4_4") == "Yes") | (f.col("step_4_5") == "Yes")) & 
    (f.col("step_5_2") == "Yes"),
          f.concat_ws(
              "; ",
              f.col("out_ckd"),
              f.lit("A2"))
          
          ).otherwise(f.col("out_ckd"))
)

# A2
cohort = cohort.withColumn(
    "out_ckd",

f.when(((f.col("step_1") == "Yes") | (f.col("step_4_1") == "Yes") | (f.col("step_4_2") == "Yes") | (f.col("step_4_3") == "Yes") | (f.col("step_4_4") == "Yes") | (f.col("step_4_5") == "Yes")) & 
    (f.col("step_5_3") == "Yes"),
          f.concat_ws(
              "; ",
              f.col("out_ckd"),
              f.lit("A3"))
          
          ).otherwise(f.col("out_ckd"))
)

# COMMAND ----------

display(cohort)

# COMMAND ----------

results = cohort.groupBy("out_ckd").count()
display(results)

# COMMAND ----------

results_with_sdc = (
    results
    .withColumn("n_pct", f.round(f.col("count") / f.sum("count").over(Window.partitionBy()) * 100, 2))
    .withColumn("count", f.when(f.col("count") < 10, 10).otherwise(f.round(f.col("count") / 5) * 5))
    .withColumnRenamed("count", "n")
)

display(results_with_sdc)

# COMMAND ----------

tab(cohort, "CONGENITAL_EVER")

# COMMAND ----------

tab(cohort, "TRANSPLANT_EVER")

# COMMAND ----------

tab(cohort, "DIALYSIS_EVER")

# COMMAND ----------

# MAGIC %md
# MAGIC # Save

# COMMAND ----------

save_table(df=cohort, out_name=f'{proj}_cohort_{algorithm_timestamp}', save_previous=False)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {dsa}.{proj}_cohort_out_{algorithm_timestamp}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Drop temp tables

# COMMAND ----------


spark.sql(f"DROP TABLE IF EXISTS {dsa}.{proj}_kdsc_curated_assets_demographics_{algorithm_timestamp}")
spark.sql(f"DROP TABLE IF EXISTS {dsa}.{proj}_kdsc_curated_assets_ckd_{algorithm_timestamp}")
spark.sql(f"DROP TABLE IF EXISTS {dsa}.{proj}_kdsc_curated_assets_creatinine_egfr_{algorithm_timestamp}")
spark.sql(f"DROP TABLE IF EXISTS {dsa}.{proj}_kdsc_curated_assets_acr_{algorithm_timestamp}")

spark.sql(f"DROP TABLE IF EXISTS {dsa}.{proj}_kdsc_curated_data_gdppr_{algorithm_timestamp}")
spark.sql(f"DROP TABLE IF EXISTS {dsa}.{proj}_kdsc_curated_data_hes_apc_{algorithm_timestamp}")
spark.sql(f"DROP TABLE IF EXISTS {dsa}.{proj}_kdsc_curated_data_hes_apc_procedures_{algorithm_timestamp}")


spark.sql(f"DROP TABLE IF EXISTS {dsa}.{proj}_kdsc_data_preprocessing_date_of_diagnosis_{algorithm_timestamp}")
spark.sql(f"DROP TABLE IF EXISTS {dsa}.{proj}_kdsc_data_preprocessing_ckd_{algorithm_timestamp}")
spark.sql(f"DROP TABLE IF EXISTS {dsa}.{proj}_kdsc_data_preprocessing_acr_{algorithm_timestamp}")
spark.sql(f"DROP TABLE IF EXISTS {dsa}.{proj}_kdsc_data_preprocessing_creatinine_egfr_{algorithm_timestamp}")


# Tables that remain
# {proj}_kdsc_parameters_df_datasets
# {proj}_kdsc_parameters_df_last_observable_date
# {proj}_kdsc_cohort
# {proj}_kdsc_cohort_out