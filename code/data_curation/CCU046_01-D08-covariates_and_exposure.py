# Databricks notebook source
# MAGIC %md # CCU046_01-D10-covariates_and_exposure
# MAGIC  
# MAGIC **Description** This notebook creates the covariates. Covariates will be defined from the latest records before the study start date (with the exception of LSOA) for each individual as follows:
# MAGIC * Prior history of exposure;
# MAGIC * Prior history of comorbidities;
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

codelist_exp  = spark.table(path_out_codelist_exposure)
codelist_com = spark.table(path_out_codelist_comorbidity)

spark.sql(f"""REFRESH TABLE {dbc}.{proj}_out_cohort""")
cohort       = spark.table(path_out_cohort)
#path_out_cohort_tmp = f'{dbc}.ccu003_05_out_cohort_pre20221218_074110'
#cohort = cohort.limit(100000)
#cohort = temp_save(df=cohort, out_name=f'{proj}_tmp_cohort_jn')

gdppr        = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
# gdppr_1st    = extract_batch_from_archive(parameters_df_datasets, 'gdppr_1st')
hes_apc_long = spark.table(path_cur_hes_apc_long)
hes_apc_oper_long = spark.table(path_cur_hes_apc_oper_long)
hes_op_long = spark.table(path_cur_hes_op_long)

minap      = extract_batch_from_archive(parameters_df_datasets, 'minap')

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
  .withColumnRenamed('DOB', 'CENSOR_DATE_START')
  .withColumnRenamed('mi_event_date','CENSOR_DATE_END')
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

# filter to after CENSOR_DATE_START and on or before CENSOR_DATE_END
gdppr_prepared = (
  gdppr_prepared
  .where(
    (f.col('DATE') > f.col('CENSOR_DATE_START'))
    & (f.col('DATE') <= f.col('CENSOR_DATE_END'))
  )
)

# check
# count_var(gdppr_prepared, 'PERSON_ID'); print()
# print(gdppr_prepared.limit(10).toPandas().to_string()); print()

# temp save (checkpoint)
#gdppr_prepared = temp_save(df=gdppr_prepared, out_name=f'{proj}_tmp_covariates_gdppr_tb')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Prepare HES APC data

# COMMAND ----------

# MAGIC %md ### 2.3.1 Diag

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
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()

# merge in individual censor dates
# _hes_apc = merge(_hes_apc, individual_censor_dates, ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0); print()
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .join(individual_censor_dates, on='PERSON_ID', how='inner')
)

# check 2
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()

# check before CENSOR_DATE_END, accounting for nulls
# note: checked in curated_data for potential columns to use in the case of null DATE (EPISTART) - no substantial gain from other columns
# 1 - DATE is null
# 2 - DATE is not null and DATE <= CENSOR_DATE_END
# 3 - DATE is not null and DATE > CENSOR_DATE_END
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .withColumn('flag_1',
    f.when((f.col('DATE').isNull()), 1)
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') <= f.col('CENSOR_DATE_END')), 2)
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') >  f.col('CENSOR_DATE_END')), 3)
  )
)
# tmpt = tab(hes_apc_long_prepared, '_tmp1'); print()

# filter to before CENSOR_DATE_END
# keep _tmp1 == 2
# tidy
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .where(f.col('flag_1').isin([2]))
  .drop('flag_1')
)

# check 3
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()

# check on or after CENSOR_DATE_START
# note: nulls were replaced in previous data step
# 1 - DATE >= CENSOR_DATE_START
# 2 - DATE <  CENSOR_DATE_START
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .withColumn('flag_2',\
    f.when((f.col('DATE') >= f.col('CENSOR_DATE_START')), 1)\
     .when((f.col('DATE') <  f.col('CENSOR_DATE_START')), 2)\
  )
)
# tmpt = tab(hes_apc_long_prepared, 'flag_2'); print()

# filter to on or after CENSOR_DATE_START
# keep _tmp2 == 1
# tidy
hes_apc_long_prepared = (
  hes_apc_long_prepared
  .where(f.col('flag_2').isin([1]))
  .drop('flag_2')
)

# check 4
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()
# print(hes_apc_long_prepared.limit(10).toPandas().to_string()); print()

# temp save (checkpoint)
#hes_apc_long_prepared = temp_save(df=hes_apc_long_prepared, out_name=f'{proj}_tmp_covariates_hes_apc_tb')

# COMMAND ----------

# MAGIC %md ### 2.3.2 Oper

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('hes_apc')
print('--------------------------------------------------------------------------------------')
# reduce and rename columns
hes_apc_oper_long_prepared = (
  hes_apc_oper_long
  .select('PERSON_ID', f.col('EPISTART').alias('DATE'), 'CODE', 'OPERTN_POSITION', 'OPERTN_DIGITS')
)

# check 1
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()

# merge in individual censor dates
# _hes_apc = merge(_hes_apc, individual_censor_dates, ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0); print()
hes_apc_oper_long_prepared = (
  hes_apc_oper_long_prepared
  .join(individual_censor_dates, on='PERSON_ID', how='inner')
)

# check 2
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()

# check before CENSOR_DATE_END, accounting for nulls
# note: checked in curated_data for potential columns to use in the case of null DATE (EPISTART) - no substantial gain from other columns
# 1 - DATE is null
# 2 - DATE is not null and DATE <= CENSOR_DATE_END
# 3 - DATE is not null and DATE > CENSOR_DATE_END
hes_apc_oper_long_prepared = (
  hes_apc_oper_long_prepared
  .withColumn('flag_1',
    f.when((f.col('DATE').isNull()), 1)
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') <= f.col('CENSOR_DATE_END')), 2)
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') >  f.col('CENSOR_DATE_END')), 3)
  )
)
# tmpt = tab(hes_apc_long_prepared, '_tmp1'); print()

# filter to before CENSOR_DATE_END
# keep _tmp1 == 2
# tidy
hes_apc_oper_long_prepared = (
  hes_apc_oper_long_prepared
  .where(f.col('flag_1').isin([2]))
  .drop('flag_1')
)

# check 3
# count_var(hes_apc_long_prepared, 'PERSON_ID'); print()

# check on or after CENSOR_DATE_START
# note: nulls were replaced in previous data step
# 1 - DATE >= CENSOR_DATE_START
# 2 - DATE <  CENSOR_DATE_START
hes_apc_oper_long_prepared = (
  hes_apc_oper_long_prepared
  .withColumn('flag_2',\
    f.when((f.col('DATE') >= f.col('CENSOR_DATE_START')), 1)\
     .when((f.col('DATE') <  f.col('CENSOR_DATE_START')), 2)\
  )
)
# tmpt = tab(hes_apc_long_prepared, 'flag_2'); print()

# filter to on or after CENSOR_DATE_START
# keep _tmp2 == 1
# tidy
hes_apc_oper_long_prepared = (
  hes_apc_oper_long_prepared
  .where(f.col('flag_2').isin([1]))
  .drop('flag_2')
)

# check 4
# count_var(hes_apc_oper_long_prepared, 'PERSON_ID'); print()
# print(hes_apc_oper_long_prepared.limit(10).toPandas().to_string()); print()

# temp save (checkpoint)
#hes_apc_oper_long_prepared = temp_save(df=hes_apc_long_prepared, out_name=f'{proj}_tmp_covariates_hes_apc_tb')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Prepare HES OP data

# COMMAND ----------

print('--------------------------------------------------------------------------------------')
print('hes_op')
print('--------------------------------------------------------------------------------------')
# reduce and rename columns
hes_op_long_prepared = (
  hes_op_long
  .select('PERSON_ID', f.col('APPTDATE').alias('DATE'), 'CODE', 'DIAG_POSITION', 'DIAG_DIGITS')
)

# check 1
# count_var(hes_op_long_prepared, 'PERSON_ID'); print()

# merge in individual censor dates
# _hes_op = merge(_hes_op, individual_censor_dates, ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0); print()
hes_op_long_prepared = (
  hes_op_long_prepared
  .join(individual_censor_dates, on='PERSON_ID', how='inner')
)

# check 2
# count_var(hes_op_long_prepared, 'PERSON_ID'); print()

# check before CENSOR_DATE_END, accounting for nulls
# note: checked in curated_data for potential columns to use in the case of null DATE (EPISTART) - no substantial gain from other columns
# 1 - DATE is null
# 2 - DATE is not null and DATE <= CENSOR_DATE_END
# 3 - DATE is not null and DATE > CENSOR_DATE_END
hes_op_long_prepared = (
  hes_op_long_prepared
  .withColumn('flag_1',
    f.when((f.col('DATE').isNull()), 1)
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') <= f.col('CENSOR_DATE_END')), 2)
     .when((f.col('DATE').isNotNull()) & (f.col('DATE') >  f.col('CENSOR_DATE_END')), 3)
  )
)
# tmpt = tab(hes_op_long_prepared, '_tmp1'); print()

# filter to before CENSOR_DATE_END
# keep _tmp1 == 2
# tidy
hes_op_long_prepared = (
  hes_op_long_prepared
  .where(f.col('flag_1').isin([2]))
  .drop('flag_1')
)

# check 3
# count_var(hes_op_long_prepared, 'PERSON_ID'); print()

# check on or after CENSOR_DATE_START
# note: nulls were replaced in previous data step
# 1 - DATE >= CENSOR_DATE_START
# 2 - DATE <  CENSOR_DATE_START
hes_op_long_prepared = (
  hes_op_long_prepared
  .withColumn('flag_2',\
    f.when((f.col('DATE') >= f.col('CENSOR_DATE_START')), 1)\
     .when((f.col('DATE') <  f.col('CENSOR_DATE_START')), 2)\
  )
)
# tmpt = tab(hes_op_long_prepared, 'flag_2'); print()

# filter to on or after CENSOR_DATE_START
# keep _tmp2 == 1
# tidy
hes_op_long_prepared = (
  hes_op_long_prepared
  .where(f.col('flag_2').isin([1]))
  .drop('flag_2')
)

# check 4
# count_var(hes_op_long_prepared, 'PERSON_ID'); print()
# print(hes_op_long_prepared.limit(10).toPandas().to_string()); print()

# temp save (checkpoint)
#hes_op_long_prepared = temp_save(df=hes_op_long_prepared, out_name=f'{proj}_tmp_covariates_hes_op_tb')

# COMMAND ----------

# MAGIC %md
# MAGIC # 3 Exposure (SMI)

# COMMAND ----------

# MAGIC %md ## 3.1 Codelist

# COMMAND ----------

codelist_exp = codelist_exp\
  .withColumn('name', f.when(f.col('name') == 'Bipolar disorder', 'Bipolar_disorder').otherwise(f.col('name')))
display(codelist_exp)

# COMMAND ----------

# check
tmpt = tab(codelist_exp, 'name', 'terminology'); print()

# COMMAND ----------

# MAGIC %md ## 3.2 Create

# COMMAND ----------

hes_apc_long_prepared = hes_apc_long_prepared\
  .drop('DIAG_POSITION', 'DIAG_DIGITS')
display(hes_apc_long_prepared)

# COMMAND ----------

hes_op_long_prepared = hes_op_long_prepared\
  .drop('DIAG_POSITION', 'DIAG_DIGITS')
display(hes_op_long_prepared)

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
dict_hx_exp = {
    'hes_apc':  ['hes_apc_long_prepared',  'codelist_exp',  1]
  , 'gdppr':        ['gdppr_prepared',             'codelist_exp', 2]
  , 'hes_op':        ['hes_op_long_prepared',             'codelist_exp', 3]
}

# run codelist match and codelist match summary functions
hx_exp, hx_exp_1st, hx_exp_1st_wide = codelist_match(dict_hx_exp, _name_prefix=f'cov_hx_exp_'); print()
hx_exp_summ_name, hx_exp_summ_name_code = codelist_match_summ(dict_hx_exp, hx_exp); print()

# COMMAND ----------

# create summary SMI variable
hx_exp_1st_wide = hx_exp_1st_wide\
  .withColumn('cov_hx_exp_smi_label', f.when(f.col('cov_hx_exp_schizophrenia_flag') == 1, 'schizophrenia')\
                             .when(f.col('cov_hx_exp_bipolar_disorder_flag') == 1, 'bipolar_disorder')\
                              .when(f.col('cov_hx_exp_depression_flag') == 1, 'depression'))
tab(hx_exp_1st_wide, 'cov_hx_exp_smi_label')

# COMMAND ----------

display(hx_exp_1st_wide)

# COMMAND ----------

# MAGIC %md ## 3.3 Check

# COMMAND ----------

count_var(hx_exp_1st_wide, 'PERSON_ID')

# COMMAND ----------

# check codelist match summary by name and source
display(hx_exp_summ_name)

# COMMAND ----------

# check codelist match summary by name, source, and code
display(hx_exp_summ_name_code)

# COMMAND ----------

# MAGIC %md # 4 HX Comorbidities

# COMMAND ----------

# MAGIC %md ## 4.1 Codelist

# COMMAND ----------

display(codelist_com)

# COMMAND ----------

tab(codelist_com, 'name', 'terminology')

# COMMAND ----------

codelist_com_icd = codelist_com.where(f.col('terminology') == "ICD10")
codelist_com_opcs = codelist_com.where(f.col('terminology') == "OPCS4")
codelist_com_snomed = codelist_com.where(f.col('terminology') == "SNOMED")

# COMMAND ----------

# MAGIC %md ## 4.2 Create

# COMMAND ----------

# drop diagnostic field position and digit fields
hes_apc_long_prepared_all = (
  hes_apc_long_prepared
  .drop('DIAG_POSITION', 'DIAG_DIGITS')
)

# drop diagnostic field position and digit fields
hes_apc_oper_long_prepared_all = (
  hes_apc_oper_long_prepared
  .drop('OPERTN_POSITION', 'OPERTN_DIGITS')
)

# COMMAND ----------

# dictionary - dataset, codelist, and ordering in the event of tied records
dict_hx_com = {
     'hes_apc': ['hes_apc_long_prepared_all', 'codelist_com_icd',    1]
   , 'hes_apc_oper':   ['hes_apc_oper_long_prepared_all', 'codelist_com_opcs', 2]
   , 'gdppr':   ['gdppr_prepared',            'codelist_com_snomed', 3]
}

# run codelist match and codelist match summary functions
hx_com, hx_com_1st, hx_com_1st_wide = codelist_match(dict_hx_com, _name_prefix=f'cov_hx_com_'); print()
hx_com_summ_name, hx_com_summ_name_code = codelist_match_summ(dict_hx_com, hx_com); print()

# COMMAND ----------

# MAGIC %md ## 4.3 Check

# COMMAND ----------

count_var(hx_com_1st_wide, 'PERSON_ID')

# COMMAND ----------

# check result
display(hx_com_1st_wide)

# COMMAND ----------

# check codelist match summary by name
display(hx_com_summ_name)

# COMMAND ----------

# check codelist match summary by name and code
display(hx_com_summ_name_code)

# COMMAND ----------

# MAGIC %md # 5 Save

# COMMAND ----------

# MAGIC %md ##5.1 Exposure (SMI)

# COMMAND ----------

tmp = merge(cohort.select('PERSON_ID'), hx_exp_1st_wide.drop('CENSOR_DATE_START', 'CENSOR_DATE_END'), ['PERSON_ID'], validate='1:1', assert_results=['both', 'left_only'], indicator=0)

# COMMAND ----------

display(tmp)

# COMMAND ----------

# save name
outName = f'{proj}_out_exposure'.lower()

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
# spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# save
print(f'saving {dbc}.{outName}')
tmp.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
#print(f'  saved')

# COMMAND ----------

# save codelist match outputs for data checks script
hx_exp_all = hx_exp['all']
hx_exp_all = temp_save(df=hx_exp_all, out_name=f'{proj}_tmp_exposure_hx_all'); print()
#hx_exp_1st = temp_save(df=hx_exp_1st, out_name=f'{proj}_tmp_exposure_hx_1st'); print()
hx_exp_1st_wide = temp_save(df=hx_exp_1st_wide, out_name=f'{proj}_tmp_exposure_hx_1st_wide'); print()
hx_exp_summ_name = temp_save(df=hx_exp_summ_name, out_name=f'{proj}_tmp_exposure_hx_summ_name'); print()
hx_exp_summ_name_code = temp_save(df=hx_exp_summ_name_code, out_name=f'{proj}_tmp_exposure_hx_summ_name_code'); print()

# COMMAND ----------

# MAGIC %md ## 5.2 Comorbidities

# COMMAND ----------

tmp = merge(cohort.select('PERSON_ID'), hx_com_1st_wide.drop('CENSOR_DATE_START', 'CENSOR_DATE_END'), ['PERSON_ID'], validate='1:1', assert_results=['both', 'left_only'], indicator=0)

# COMMAND ----------

# save name
outName = f'{proj}_out_comorbidities'.lower()

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
# spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# save
print(f'saving {dbc}.{outName}')
spark.sql(f'DROP TABLE IF EXISTS {dbc}.{outName}')
tmp.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
#print(f'  saved')

# COMMAND ----------

# save codelist match outputs for data checks script
hx_com_all = hx_com['all']
hx_com_all = temp_save(df=hx_com_all, out_name=f'{proj}_tmp_covariates_hx_com_all'); print()
hx_com_1st = temp_save(df=hx_com_1st, out_name=f'{proj}_tmp_covariates_hx_com_1st'); print()
hx_com_1st_wide = temp_save(df=hx_com_1st_wide, out_name=f'{proj}_tmp_covariates_hx_com_1st_wide'); print()
hx_com_summ_name = temp_save(df=hx_com_summ_name, out_name=f'{proj}_tmp_covariates_hx_com_summ_name'); print()
hx_com_summ_name_code = temp_save(df=hx_com_summ_name_code, out_name=f'{proj}_tmp_covariates_hx_com_summ_name_code'); print()