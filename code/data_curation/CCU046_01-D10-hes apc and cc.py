# Databricks notebook source
# MAGIC %md # CCU046_01-D10-hes apc and cc
# MAGIC  
# MAGIC **Description** This notebook gets all of the required variables for covariates and outcomes from the HES APC associated to the MI record in MINAP:
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

#codelist_exp  = spark.table(path_out_codelist_exposure)
#codelist_com = spark.table(path_out_codelist_comorbidity)

spark.sql(f"""REFRESH TABLE {dbc_old}.{proj}_out_cohort""")
cohort       = spark.table(path_out_cohort)
#path_out_cohort_tmp = f'{dbc}.ccu003_05_out_cohort_pre20221218_074110'
#cohort = cohort.limit(100000)
#cohort = temp_save(df=cohort, out_name=f'{proj}_tmp_cohort_jn')

#gdppr        = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
# gdppr_1st    = extract_batch_from_archive(parameters_df_datasets, 'gdppr_1st')
hes_apc      = extract_batch_from_archive(parameters_df_datasets, 'hes_apc')
#hes_apc_long = spark.table(path_cur_hes_apc_long)
#hes_op_long = spark.table(path_cur_hes_op_long)
hes_cc      = extract_batch_from_archive(parameters_df_datasets, 'hes_cc')

minap      = extract_batch_from_archive(parameters_df_datasets, 'minap')

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

## 2.1 Prepare Cohort lookup with required date fields

print('--------------------------------------------------------------------------------------')
print('individual_censor_dates')
print('--------------------------------------------------------------------------------------')
print(f'study_start_date = {study_start_date}')
individual_censor_dates = (
  cohort
  .select('PERSON_ID', 'DOB', 'mi_event_date')
  .withColumnRenamed('DOB', 'CENSOR_DATE_START')
  .withColumnRenamed('mi_event_date','CENSOR_DATE_END')
)

# check
count_var(individual_censor_dates, 'PERSON_ID'); print()
print(individual_censor_dates.limit(10).toPandas().to_string()); print()

# COMMAND ----------

## 2.2 Prepare HES APC Data
# get required fields
hes_apc = (hes_apc
           .select('PERSON_ID_DEID', 'EPISTART', 'EPIEND', 'ADMIDATE', 'DISDATE', 'SUSRECID',
                   'EPIORDER', 'PCTTREAT', 'PROCODE3', 'PROCODE5', 'DIAG_3_CONCAT', 'DIAG_4_CONCAT',
                   'LSOA11')
           .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')
          )

# inner join to cohort
hes_apc_prepared = (merge(cohort.select('PERSON_ID', 'mi_event_date'),
                          hes_apc, 
                          ['PERSON_ID'], 
                          validate='1:m', 
                          keep_results=['left_only', 'both'], 
                          indicator=0))

# final variable selection and sort
hes_apc_prepared = (hes_apc_prepared
                    .select('PERSON_ID', 'mi_event_date', 'EPISTART', 'EPIEND', 'ADMIDATE', 'DISDATE', 'SUSRECID',
                            'EPIORDER', 'PCTTREAT', 'PROCODE3', 'PROCODE5', 'DIAG_3_CONCAT', 'DIAG_4_CONCAT',
                            'LSOA11')
                    .sort('PERSON_ID', 'EPISTART')
                   )

# COMMAND ----------

## merge cohort hes_apc data to hes_cc

# prepare hes_cc data
hes_cc_prepared = (hes_cc
           .where(f.col('BESTMATCH') == 1)
           .select('PERSON_ID_DEID', 'SUSRECID', 'CCSTARTDATE')
           .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')
           .withColumn('ICU', f.lit(1))
           .distinct()
          )

# COMMAND ----------

# merge
hes_apc_prepared = (merge(hes_apc_prepared,
                          hes_cc_prepared, 
                          ['PERSON_ID', 'SUSRECID'], 
                          validate='m:m', 
                          keep_results=['left_only', 'both'], 
                          indicator=0))


# COMMAND ----------

display(hes_apc_prepared)

# COMMAND ----------

# calculate days difference between mi_event_date and EPISTART

# create window to count total number of records for each individual
win = Window\
  .partitionBy('PERSON_ID')

hes_apc_prepared_2 = (hes_apc_prepared
                      .withColumn('days_diff', f.datediff('mi_event_date','EPISTART'))
                      .withColumn('abs_days_diff', f.abs(f.col('days_diff')))
                      .withColumn('min_days_diff', f.min('abs_days_diff').over(win))
                     )

display(hes_apc_prepared_2)

# COMMAND ----------

# identify the min days diff to HES APC for each MINAP record
hes_apc_prepared_3 = (hes_apc_prepared_2
                      .where(f.col('abs_days_diff') == f.col('min_days_diff'))
                      .select('PERSON_ID', 'mi_event_date', 'min_days_diff')
                      .distinct()
                     )

count_var(hes_apc_prepared_3, 'PERSON_ID')
tab(hes_apc_prepared_3, 'min_days_diff'); print()

# COMMAND ----------

# looking at cases where the min days difference is 1 (15% cases)
# is the mi_event_date 1 day before or after the closest EPISTART
hes_apc_prepared_4 = (hes_apc_prepared_2
                      .where(f.col('abs_days_diff') == 1)
                      .select('PERSON_ID', 'mi_event_date', 'days_diff')
                      .distinct()
                     )
# so in most (95%) cases the EPISTART is 1 day before the closest MI event date
tab(hes_apc_prepared_4, 'days_diff')

# COMMAND ----------

# take the hes apc record that has min days difference (still will be multiple rows per person)
hes_apc_prepared_5 = (hes_apc_prepared_2
                      .where(f.col('abs_days_diff') == f.col('min_days_diff'))
                      .select('PERSON_ID', 'mi_event_date', 'days_diff', 'EPISTART', 'PCTTREAT', 'PROCODE3', 'PROCODE5', 'LSOA11', 'DIAG_4_CONCAT', 'ICU')
                      .sort('PERSON_ID')
                     )
count_var(hes_apc_prepared_5, 'PERSON_ID')

# COMMAND ----------

# now summarise to get single HES APC record to join to MINAP record

# create window to count total number of records for each individual
win = Window\
  .partitionBy('PERSON_ID', 'mi_event_date')
# create window to order admissions
win_order = Window\
  .partitionBy('PERSON_ID', 'mi_event_date')\
  .orderBy(f.col('EPISTART').asc_nulls_last())

hes_apc_prepared_final = (hes_apc_prepared_5
                          .where(f.col('abs_days_diff') == f.col('min_days_diff'))
                          .where(f.col('min_days_diff').isin([0, 1]))
                          .withColumn('epi_order', f.row_number().over(win_order))\
                          .withColumn('ICU', f.max(f.col('ICU')).over(win_order))
                          .where(f.col('epi_order') == 1)
                          .select('PERSON_ID', 'mi_event_date', 'PCTTREAT', 'PROCODE3', 'PROCODE5', 'ICU', 'LSOA11'))
display(hes_apc_prepared_final)


# COMMAND ----------

# should be 1 row per patient, and match to number of patients with a min difference of 0 or 1 day
# there are now fewer ids because we have restricted to those with a max date difference of 1 day
count_var(hes_apc_prepared_final, 'PERSON_ID')

# COMMAND ----------

hes_apc_prepared_final = (hes_apc_prepared_final
                          .withColumnRenamed('LSOA11', 'LSOA11_HES')
                          .drop('mi_event_date'))

# COMMAND ----------

# MAGIC %md
# MAGIC #3 HES APC and CC

# COMMAND ----------

display(hes_apc_prepared_final)

# COMMAND ----------

# MAGIC %md # F Save

# COMMAND ----------

# check
count_var(hes_apc_prepared_final, 'PERSON_ID'); print()
#print(len(tmp3.columns)); print()
#print(pd.DataFrame({f'_cols': tmp3.columns}).to_string()); print()

# COMMAND ----------

display(hes_apc_prepared_final)

#KF: 
#tab(hes_apc_prepared_final, 'hes_diag_flag'); print()

# COMMAND ----------

# save name
outName = f'{proj}_out_hes_apc'.lower()

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
  #spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# COMMAND ----------

spark.sql(f'drop table dsa_391419_j3w9t_collab.ccu046_01_out_hes_apc')

# COMMAND ----------

# save
hes_apc_prepared_final.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
minap_prepared_final_out = spark.table(f'{dbc}.{outName}')

# COMMAND ----------

# check final
display(minap_prepared_final_out)

# COMMAND ----------

# tidy - drop temporary tables
# drop_table(table_name=f'{proj}_tmp_covariates_hes_apc')
# drop_table(table_name=f'{proj}_tmp_covariates_pmeds')
# drop_table(table_name=f'{proj}_tmp_covariates_lsoa')
# drop_table(table_name=f'{proj}_tmp_covariates_lsoa_2')
# drop_table(table_name=f'{proj}_tmp_covariates_lsoa_3')
# drop_table(table_name=f'{proj}_tmp_covariates_n_consultations')
# drop_table(table_name=f'{proj}_tmp_covariates_unique_bnf_chapters')
# drop_table(table_name=f'{proj}_tmp_covariates_hx_out_1st_wide')
# drop_table(table_name=f'{proj}_tmp_covariates_hx_com_1st_wide')