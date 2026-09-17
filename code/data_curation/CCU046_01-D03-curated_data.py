# Databricks notebook source
# MAGIC %md # CCU046_01-D03a-curated_data
# MAGIC
# MAGIC **Description** This notebook creates the curated tables.
# MAGIC
# MAGIC **Authors** Tom Bolton, John Nolan
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on CCU003_05.
# MAGIC
# MAGIC **Notes**

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

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

hes_apc = extract_batch_from_archive(parameters_df_datasets, 'hes_apc')
hes_op = extract_batch_from_archive(parameters_df_datasets, 'hes_op')
deaths  = extract_batch_from_archive(parameters_df_datasets, 'deaths')
geog    = spark.table(path_ref_geog)
imd     = spark.table(path_ref_imd)

# COMMAND ----------

# MAGIC %md # 2 HES_APC

# COMMAND ----------

# select columns (PERSON_ID, RECORD_ID, DATE, Diagnostic columns)
# rename PERSON_ID
_hes_apc = (
  hes_apc  
  .select(['PERSON_ID_DEID', 'EPIKEY', 'EPISTART'] 
          + [col for col in list(hes_apc.columns) if re.match(r'^DIAG_(3|4)_\d\d$', col)]
          + [col for col in list(hes_apc.columns) if re.match(r'^OPERTN_(3|4)_\d\d$', col)])
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')
  .orderBy('PERSON_ID', 'EPIKEY')
)

# check
count_var(hes_apc, 'PERSON_ID_DEID'); print()
count_var(hes_apc, 'EPIKEY'); print()

# check for null EPISTART and potential use ADMIDATE to supplement
tmpp = (
  hes_apc
  .select('EPISTART', 'ADMIDATE')
  .withColumn('_EPISTART', f.when(f.col('EPISTART').isNotNull(), 1).otherwise(0))
  .withColumn('_ADMIDATE', f.when(f.col('ADMIDATE').isNotNull(), 1).otherwise(0))
)
tmpt = tab(tmpp, '_EPISTART', '_ADMIDATE', var2_unstyled=1); print()
# => ADMIDATE is always null when EPISTART is null

# COMMAND ----------

# check
display(_hes_apc)

# COMMAND ----------

# MAGIC %md ## 2.1 Diag

# COMMAND ----------

# MAGIC %md ### 2.1.1 Create

# COMMAND ----------

# reshape twice, tidy, and remove records with missing code

hes_apc_long = (
  reshape_wide_to_long_multi(_hes_apc, i=['PERSON_ID', 'EPIKEY', 'EPISTART'], j='POSITION', stubnames=['DIAG_4_', 'DIAG_3_'])
  .withColumn('_tmp', f.substring(f.col('DIAG_4_'), 1, 3))
  .withColumn('_chk', udf_null_safe_equality('DIAG_3_', '_tmp').cast(t.IntegerType()))
  .withColumn('_DIAG_4_len', f.length(f.col('DIAG_4_')))
  .withColumn('_chk2', f.when((f.col('_DIAG_4_len').isNull()) | (f.col('_DIAG_4_len') <= 4), 1).otherwise(0))
)

# check
tmpt = tab(hes_apc_long, '_chk'); print()
assert hes_apc_long.where(f.col('_chk') == 0).count() == 0
tmpt = tab(hes_apc_long, '_DIAG_4_len'); print()
tmpt = tab(hes_apc_long, '_chk2'); print()
assert hes_apc_long.where(f.col('_chk2') == 0).count() == 0

# tidy
hes_apc_long = (
  hes_apc_long
  .drop('_tmp', '_chk')
)

hes_apc_long = reshape_wide_to_long_multi(hes_apc_long, i=['PERSON_ID', 'EPIKEY', 'EPISTART', 'POSITION'], j='DIAG_DIGITS', stubnames=['DIAG_'])\
  .withColumnRenamed('POSITION', 'DIAG_POSITION')\
  .withColumn('DIAG_POSITION', f.regexp_replace('DIAG_POSITION', r'^[0]', ''))\
  .withColumn('DIAG_DIGITS', f.regexp_replace('DIAG_DIGITS', r'[_]', ''))\
  .withColumn('DIAG_', f.regexp_replace('DIAG_', r'X$', ''))\
  .withColumn('DIAG_', f.regexp_replace('DIAG_', r'[.,\-\s]', ''))\
  .withColumnRenamed('DIAG_', 'CODE')\
  .where((f.col('CODE').isNotNull()) & (f.col('CODE') != ''))\
  .orderBy(['PERSON_ID', 'EPIKEY', 'DIAG_DIGITS', 'DIAG_POSITION'])

# COMMAND ----------

# MAGIC %md ### 2.1.2 Save

# COMMAND ----------

# save before checks for efficiency
outName = f'{proj}_cur_hes_apc_all_years_archive_long'.lower()  
hes_apc_long.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
hes_apc_long = spark.table(f'{dbc}.{outName}')

# COMMAND ----------

# MAGIC %md ### 2.1.3 Check

# COMMAND ----------

# check
count_var(hes_apc_long, 'PERSON_ID'); print()
count_var(hes_apc_long, 'EPIKEY'); print()

# check removal of trailing X
tmpp = hes_apc_long\
  .where(f.col('CODE').rlike('X'))\
  .withColumn('flag', f.when(f.col('CODE').rlike('^X.*'), 1).otherwise(0))
tmpt = tab(tmpp, 'flag'); print()
tmpt = tab(tmpp.where(f.col('CODE').rlike('X')), 'CODE', 'flag', var2_unstyled=1); print()

# COMMAND ----------

display(hes_apc_long)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Oper

# COMMAND ----------

# MAGIC %md ### 2.2.1 Create

# COMMAND ----------

# reshape twice, tidy, and remove records with missing code

hes_apc_long_oper = (
  reshape_wide_to_long_multi(_hes_apc, i=['PERSON_ID', 'EPIKEY', 'EPISTART'], j='POSITION', stubnames=['OPERTN_4_', 'OPERTN_3_'])
  .withColumn('_tmp', f.substring(f.col('OPERTN_4_'), 1, 3))
  .withColumn('_chk', udf_null_safe_equality('OPERTN_3_', '_tmp').cast(t.IntegerType()))
  .withColumn('_OPERTN_4_len', f.length(f.col('OPERTN_4_')))
  .withColumn('_chk2', f.when((f.col('_OPERTN_4_len').isNull()) | (f.col('_OPERTN_4_len') <= 4), 1).otherwise(0))
)

# check
tmpt = tab(hes_apc_long_oper, '_chk'); print()
#assert hes_apc_long_oper.where(f.col('_chk') == 0).count() == 0
tmpt = tab(hes_apc_long_oper, '_OPERTN_4_len'); print()
tmpt = tab(hes_apc_long_oper, '_chk2'); print()
#assert hes_apc_long_oper.where(f.col('_chk2') == 0).count() == 0

# tidy
hes_apc_long_oper = (
  hes_apc_long_oper
  .drop('_tmp', '_chk')
)

hes_apc_long_oper = reshape_wide_to_long_multi(hes_apc_long_oper, i=['PERSON_ID', 'EPIKEY', 'EPISTART', 'POSITION'], j='OPERTN_DIGITS', stubnames=['OPERTN_'])\
  .withColumnRenamed('POSITION', 'OPERTN_POSITION')\
  .withColumn('OPERTN_POSITION', f.regexp_replace('OPERTN_POSITION', r'^[0]', ''))\
  .withColumn('OPERTN_DIGITS', f.regexp_replace('OPERTN_DIGITS', r'[_]', ''))\
  .withColumn('OPERTN_', f.regexp_replace('OPERTN_', r'X$', ''))\
  .withColumn('OPERTN_', f.regexp_replace('OPERTN_', r'[.,\-\s]', ''))\
  .withColumnRenamed('OPERTN_', 'CODE')\
  .where((f.col('CODE').isNotNull()) & (f.col('CODE') != ''))\
  .orderBy(['PERSON_ID', 'EPIKEY', 'OPERTN_DIGITS', 'OPERTN_POSITION'])

# COMMAND ----------

display(hes_apc_long_oper.where(f.col('_chk') == 0))

# COMMAND ----------

# MAGIC %md ###2.2.2 Save

# COMMAND ----------

 # save before checks for efficiency
outName = f'{proj}_cur_hes_apc_all_years_archive_oper_long'.lower()  
hes_apc_long_oper.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
hes_apc_long_oper = spark.table(f'{dbc}.{outName}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2.3 Check

# COMMAND ----------

# check
count_var(hes_apc_long_oper, 'PERSON_ID'); print()
count_var(hes_apc_long_oper, 'EPIKEY'); print()

# check removal of trailing X
tmpp = hes_apc_long_oper\
  .where(f.col('CODE').rlike('X'))\
  .withColumn('flag', f.when(f.col('CODE').rlike('^X.*'), 1).otherwise(0))
tmpt = tab(tmpp, 'flag'); print()
tmpt = tab(tmpp.where(f.col('CODE').rlike('X')), 'CODE', 'flag', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md
# MAGIC # 3 HES OP

# COMMAND ----------

display(hes_op)

# COMMAND ----------

# select columns (PERSON_ID, RECORD_ID, DATE, Diagnostic columns)
# rename PERSON_ID
_hes_op = (
  hes_op  
  .select(['PERSON_ID_DEID', 'APPTDATE'] 
          + [col for col in list(hes_op.columns) if re.match(r'^DIAG_(3|4)_\d\d$', col)])
  .withColumnRenamed('PERSON_ID_DEID', 'PERSON_ID')
  .orderBy('PERSON_ID', 'APPTDATE')
)

# check
#count_var(hes_op, 'PERSON_ID_DEID'); print()
#count_var(hes_op, 'EPIKEY'); print()

# check for null EPISTART and potential use ADMIDATE to supplement
tmpp = (
  hes_op
  .select('APPTDATE')
  .withColumn('_APPTDATE', f.when(f.col('APPTDATE').isNotNull(), 1).otherwise(0))
)
tmpt = tab(tmpp, '_APPTDATE', var2_unstyled=1); print()
# => ADMIDATE is always null when EPISTART is null

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Diag

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1.1 Create

# COMMAND ----------

# reshape twice, tidy, and remove records with missing code

hes_op_long = (
  reshape_wide_to_long_multi(_hes_op, i=['PERSON_ID', 'APPTDATE'], j='POSITION', stubnames=['DIAG_4_', 'DIAG_3_'])
  .withColumn('_tmp', f.substring(f.col('DIAG_4_'), 1, 3))
  .withColumn('_chk', udf_null_safe_equality('DIAG_3_', '_tmp').cast(t.IntegerType()))
  .withColumn('_DIAG_4_len', f.length(f.col('DIAG_4_')))
  .withColumn('_chk2', f.when((f.col('_DIAG_4_len').isNull()) | (f.col('_DIAG_4_len') <= 4), 1).otherwise(0))
)

# check
tmpt = tab(hes_op_long, '_chk'); print()
assert hes_op_long.where(f.col('_chk') == 0).count() == 0
tmpt = tab(hes_op_long, '_DIAG_4_len'); print()
tmpt = tab(hes_op_long, '_chk2'); print()
assert hes_op_long.where(f.col('_chk2') == 0).count() == 0

# tidy
hes_op_long = (
  hes_op_long
  .drop('_tmp', '_chk')
)

hes_op_long = reshape_wide_to_long_multi(hes_op_long, i=['PERSON_ID', 'APPTDATE', 'POSITION'], j='DIAG_DIGITS', stubnames=['DIAG_'])\
  .withColumnRenamed('POSITION', 'DIAG_POSITION')\
  .withColumn('DIAG_POSITION', f.regexp_replace('DIAG_POSITION', r'^[0]', ''))\
  .withColumn('DIAG_DIGITS', f.regexp_replace('DIAG_DIGITS', r'[_]', ''))\
  .withColumn('DIAG_', f.regexp_replace('DIAG_', r'X$', ''))\
  .withColumn('DIAG_', f.regexp_replace('DIAG_', r'[.,\-\s]', ''))\
  .withColumnRenamed('DIAG_', 'CODE')\
  .where((f.col('CODE').isNotNull()) & (f.col('CODE') != ''))\
  .orderBy(['PERSON_ID', 'APPTDATE', 'DIAG_DIGITS', 'DIAG_POSITION'])

# COMMAND ----------

display(hes_op_long)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1.2 Save

# COMMAND ----------

# save before checks for efficiency
outName = f'{proj}_cur_hes_op_all_years_archive_long'.lower()  
hes_op_long.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
hes_op_long = spark.table(f'{dbc}.{outName}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1.3 Check

# COMMAND ----------

# check
count_var(hes_op_long, 'PERSON_ID'); print()

# check removal of trailing X
tmpp = hes_op_long\
  .where(f.col('CODE').rlike('X'))\
  .withColumn('flag', f.when(f.col('CODE').rlike('^X.*'), 1).otherwise(0))
tmpt = tab(tmpp, 'flag'); print()
tmpt = tab(tmpp.where(f.col('CODE').rlike('X')), 'CODE', 'flag', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md # 4 Deaths

# COMMAND ----------

# MAGIC %md ## 4.1 Create

# COMMAND ----------

# check
count_var(deaths, 'DEC_CONF_NHS_NUMBER_CLEAN_DEID'); print()
assert dict(deaths.dtypes)['REG_DATE'] == 'string'
assert dict(deaths.dtypes)['REG_DATE_OF_DEATH'] == 'string'

# define window for the purpose of creating a row number below as per the skinny patient table
_win = Window\
  .partitionBy('PERSON_ID')\
  .orderBy(f.desc('REG_DATE'), f.desc('REG_DATE_OF_DEATH'), f.desc('S_UNDERLYING_COD_ICD10'))

# rename ID
# remove records with missing IDs
# reformat dates
# reduce to a single row per individual as per the skinny patient table
# select columns required
# rename column ahead of reshape below
# sort by ID
deaths_out = deaths\
  .withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'PERSON_ID')\
  .where(f.col('PERSON_ID').isNotNull())\
  .withColumn('REG_DATE', f.to_date(f.col('REG_DATE'), 'yyyyMMdd'))\
  .withColumn('REG_DATE_OF_DEATH', f.to_date(f.col('REG_DATE_OF_DEATH'), 'yyyyMMdd'))\
  .withColumn('_rownum', f.row_number().over(_win))\
  .where(f.col('_rownum') == 1)\
  .select(['PERSON_ID', 'REG_DATE', 'REG_DATE_OF_DEATH', 'S_UNDERLYING_COD_ICD10'] + [col for col in list(deaths.columns) if re.match(r'^S_COD_CODE_\d(\d)*$', col)])\
  .withColumnRenamed('S_UNDERLYING_COD_ICD10', 'S_COD_CODE_UNDERLYING')\
  .orderBy('PERSON_ID')

# check
count_var(deaths_out, 'PERSON_ID'); print()
count_var(deaths_out, 'REG_DATE_OF_DEATH'); print()
count_var(deaths_out, 'S_COD_CODE_UNDERLYING'); print()

# single row deaths 
deaths_out_sing = deaths_out

# remove records with missing DOD
deaths_out = deaths_out\
  .where(f.col('REG_DATE_OF_DEATH').isNotNull())\
  .drop('REG_DATE')

# check
count_var(deaths_out, 'PERSON_ID'); print()

# reshape
# add 1 to diagnosis position to start at 1 (c.f., 0) - will avoid confusion with HES long, which start at 1
# rename 
# remove records with missing cause of death
deaths_out_long = reshape_wide_to_long(deaths_out, i=['PERSON_ID', 'REG_DATE_OF_DEATH'], j='DIAG_POSITION', stubname='S_COD_CODE_')\
  .withColumn('DIAG_POSITION', f.when(f.col('DIAG_POSITION') != 'UNDERLYING', f.concat(f.lit('SECONDARY_'), f.col('DIAG_POSITION'))).otherwise(f.col('DIAG_POSITION')))\
  .withColumnRenamed('S_COD_CODE_', 'CODE4')\
  .where(f.col('CODE4').isNotNull())\
  .withColumnRenamed('REG_DATE_OF_DEATH', 'DATE')\
  .withColumn('CODE3', f.substring(f.col('CODE4'), 1, 3))
deaths_out_long = reshape_wide_to_long(deaths_out_long, i=['PERSON_ID', 'DATE', 'DIAG_POSITION'], j='DIAG_DIGITS', stubname='CODE')\
  .withColumn('CODE', f.regexp_replace('CODE', r'[.,\-\s]', ''))
  
# check
count_var(deaths_out_long, 'PERSON_ID'); print()  
tmpt = tab(deaths_out_long, 'DIAG_POSITION', 'DIAG_DIGITS', var2_unstyled=1); print() 
tmpt = tab(deaths_out_long, 'CODE', 'DIAG_DIGITS', var2_unstyled=1); print()   
# TODO - add valid ICD-10 code checker...

# COMMAND ----------

# MAGIC %md ## 4.2 Check

# COMMAND ----------

display(deaths_out_sing)

# COMMAND ----------

display(deaths_out_long)

# COMMAND ----------

# MAGIC %md ## 4.3 Save

# COMMAND ----------

outName = f'{proj}_cur_deaths_{db}_archive_sing'.lower()
deaths_out_sing.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

outName = f'{proj}_cur_deaths_{db}_archive_long'.lower()
deaths_out_long.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')