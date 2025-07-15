# Databricks notebook source
# MAGIC %md # CCU046_01-D10-gdppr-hes-apc-event-check
# MAGIC  
# MAGIC **Description** This notebook matches the MI codelist to GDPPR and HES APC to check whether cohort patients have an associated MI event in GDPPR and HES APC (based on date +/- 1 day);
# MAGIC  
# MAGIC **Authors** Tom Bolton, John Nolan
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on previous work by Tom Bolton, Alexia Sampri for CCU018_01 and the earlier CCU002 sub-projects.
# MAGIC
# MAGIC **Notes**

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

# DBTITLE 1,Libraries
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window

from functools import reduce

import databricks.koalas as ks
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

# DBTITLE 1,Functions
# MAGIC %run "/Repos/jn453@medschl.cam.ac.uk/shds/common/functions"

# COMMAND ----------

# MAGIC %md # 0 Parameters

# COMMAND ----------

# MAGIC %run "./CCU046_01-D01-parameters"

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

codelist_cohort  = spark.table(path_out_codelist_cohort)

spark.sql(f"""REFRESH TABLE {dbc}.{proj}_out_cohort""")
cohort       = spark.table(path_out_cohort)
#path_out_cohort_tmp = f'{dbc}.ccu003_05_out_cohort_pre20221218_074110'
#cohort = cohort.limit(100000)
#cohort = temp_save(df=cohort, out_name=f'{proj}_tmp_cohort_jn')

gdppr        = extract_batch_from_archive(parameters_df_datasets, 'gdppr')

hes_apc_long = spark.table(path_cur_hes_apc_long)

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Prepare cohort lookup with required date fields

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('individual_censor_dates')
print('--------------------------------------------------------------------------------------')
print(f'study_start_date = {study_start_date}')
individual_censor_dates = (
  cohort
  .select('PERSON_ID', 'DOB', 'mi_event_date')
)

# check
count_var(individual_censor_dates, 'PERSON_ID'); print()
print(individual_censor_dates.limit(10).toPandas().to_string()); print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Prepare GDPPR data

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('gdppr')
print('--------------------------------------------------------------------------------------')
# reduce and rename columns
gdppr_prepared = (
  gdppr
  .select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'), 'DATE', 'CODE')
)

# check
# count_var(gdppr_prepared, 'PERSON_ID'); print()

# add individual censor dates
gdppr_prepared = (
  gdppr_prepared
  .join(individual_censor_dates, on='PERSON_ID', how='inner')
)

# check
# count_var(gdppr_prepared, 'PERSON_ID'); print()

# check
count_var(gdppr_prepared, 'PERSON_ID'); print()
# print(gdppr_prepared.limit(10).toPandas().to_string()); print()

# temp save (checkpoint)
gdppr_prepared = temp_save(df=gdppr_prepared, out_name=f'{proj}_tmp_covariates_gdppr')

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.3 Prepare HES APC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3.1 Diag

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('hes_apc')
print('--------------------------------------------------------------------------------------')
# reduce and rename columns
hes_apc_long_prepared = (
  hes_apc_long
  .select('PERSON_ID', f.col('EPISTART').alias('DATE'), 'CODE', 'DIAG_POSITION', 'DIAG_DIGITS')
)

# check 1
count_var(hes_apc_long_prepared, 'PERSON_ID'); print()

# merge in individual censor dates
# _hes_apc = merge(_hes_apc, individual_censor_dates, ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0); print()
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .join(individual_censor_dates, on='PERSON_ID', how='inner')
)

# check 2
count_var(hes_apc_long_prepared, 'PERSON_ID'); print()

# temp save (checkpoint)
hes_apc_long_prepared = temp_save(df=hes_apc_long_prepared, out_name=f'{proj}_tmp_covariates_hes_apc_diag')

# COMMAND ----------

# MAGIC %md
# MAGIC # 3 MI Event

# COMMAND ----------

# MAGIC %md ## 3.1 Codelist

# COMMAND ----------

display(codelist_cohort)

# COMMAND ----------

# check
tmpt = tab(codelist_cohort, 'name', 'terminology'); print()

# COMMAND ----------

# MAGIC %md ## 3.2 HES APC

# COMMAND ----------

hes_apc_long_prepared = hes_apc_long_prepared\
  .drop('DIAG_POSITION', 'DIAG_DIGITS')
display(hes_apc_long_prepared)

# COMMAND ----------

cohort_mi_events_apc = (hes_apc_long_prepared
                        .join(codelist_cohort
                              .where(f.col('terminology') == 'ICD10')
                              .withColumnRenamed('code', 'CODE'), 
                              on = 'code', 
                              how = 'inner')
                        .withColumn('days_diff', f.datediff('mi_event_date','DATE'))
                        .withColumn('abs_days_diff', f.abs(f.col('days_diff')))                       
)
display(cohort_mi_events_apc)


# COMMAND ----------

cohort_mi_events_apc_min = (cohort_mi_events_apc
                              .groupBy('PERSON_ID')
                              .agg(f.min(f.col('abs_days_diff')).alias('min_days_diff'))
)
count_var(cohort_mi_events_apc_min, 'PERSON_ID')

# COMMAND ----------

apc_days_diff_summary = (cohort_mi_events_apc_min
                           .groupBy('min_days_diff')
                           .agg(f.count(f.col('min_days_diff')).alias('count'))
                           .sort('min_days_diff')
)
display(apc_days_diff_summary.where(f.col('min_days_diff')<=90))

# COMMAND ----------

cohort_mi_events_apc_final = (cohort_mi_events_apc
                        .where(f.col('abs_days_diff').isin([0,1]))
                        .select('PERSON_ID')
                        .dropDuplicates(['PERSON_ID'])
                        .withColumn('hes_apc_diag_flag', f.lit(1))
)
count_var(cohort_mi_events_apc_final, 'PERSON_ID')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 GDPPR

# COMMAND ----------

cohort_mi_events_gdppr = (gdppr_prepared
                        .join(codelist_cohort
                              .where(f.col('terminology') == 'SNOMED')
                              .withColumnRenamed('code', 'CODE'), 
                              on = 'code', 
                              how = 'inner')
                        .withColumn('days_diff', f.datediff('mi_event_date','DATE'))
                        .withColumn('abs_days_diff', f.abs(f.col('days_diff')))                       
)
display(cohort_mi_events_gdppr)

# COMMAND ----------

cohort_mi_events_gdppr_min = (cohort_mi_events_gdppr
                              .groupBy('PERSON_ID')
                              .agg(f.min(f.col('abs_days_diff')).alias('min_days_diff'))
)
count_var(cohort_mi_events_gdppr_min, 'PERSON_ID')

# COMMAND ----------

gdppr_days_diff_summary = (cohort_mi_events_gdppr_min
                           .groupBy('min_days_diff')
                           .agg(f.count(f.col('min_days_diff')).alias('count'))
                           .sort('min_days_diff')
)
display(gdppr_days_diff_summary.where(f.col('min_days_diff')<=90))

# COMMAND ----------

cohort_mi_events_gdppr_final = (cohort_mi_events_gdppr
                        .where(f.col('abs_days_diff') <= 7)
                        .select('PERSON_ID')
                        .dropDuplicates(['PERSON_ID'])
                        .withColumn('gdppr_diag_flag', f.lit(1))
)
count_var(cohort_mi_events_gdppr_final, 'PERSON_ID')

# COMMAND ----------

# MAGIC %md ## 3.4 Merge

# COMMAND ----------

# merge hes apc and gdppr coded MI events
cohort_mi_events_final = merge(cohort_mi_events_gdppr_final, cohort_mi_events_apc_final, ['PERSON_ID'], validate = '1:1')
display(cohort_mi_events_final)      

# COMMAND ----------

cohort_mi_events_final = (merge(cohort,cohort_mi_events_final, ['PERSON_ID'], validate = '1:1')
                          .withColumn('gdppr_diag_flag', f.when(f.col('gdppr_diag_flag').isNull(), f.lit(0)).otherwise(f.col('gdppr_diag_flag')))
                          .withColumn('hes_apc_diag_flag', f.when(f.col('hes_apc_diag_flag').isNull(), f.lit(0)).otherwise(f.col('hes_apc_diag_flag')))
                          .select('PERSON_ID', 'gdppr_diag_flag', 'hes_apc_diag_flag')
)
display(cohort_mi_events_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 Check

# COMMAND ----------

tmpt = tab(cohort_mi_events_final, 'gdppr_diag_flag'); print()
tmpt = tab(cohort_mi_events_final, 'hes_apc_diag_flag'); print()

# COMMAND ----------

# MAGIC %md # 4 Save

# COMMAND ----------

# save name
outName = f'{proj}_out_gdppr_hes_apc_mi_event'.lower()

# save previous version for comparison purposes
tmpt = spark.sql(f"""SHOW TABLES FROM {dbc}""")\
  .select('tableName')\
  .where(f.col('tableName') == outName)\
  .collect()
if(len(tmpt)>0):
  _datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
  outName_pre = f'{outName}_pre{_datetimenow}'.lower()
  print(outName_pre)
  spark.table(f'{dbc}.{outName}').write.mode('overwrite').saveAsTable(f'{dbc}.{outName_pre}')
#  spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# save
print(f'saving {dbc}.{outName}')
cohort_mi_events_final.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
