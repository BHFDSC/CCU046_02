# Databricks notebook source
# MAGIC %md # CCU046_01-D06-inclusion_exclusion
# MAGIC  
# MAGIC **Description** This notebook applies the inclusion/exclusion criteria.
# MAGIC  
# MAGIC **Authors** Tom Bolton, John Nolan
# MAGIC  
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on CCU002_07
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

spark.sql(f'REFRESH TABLE {dbc}.{proj}_tmp_skinny')
spark.sql(f'REFRESH TABLE {path_cur_deaths_sing}')
spark.sql(f'REFRESH TABLE {dbc}.{proj}_tmp_quality_assurance')

skinny     = spark.table(f'{dbc}.{proj}_tmp_skinny')
deaths     = spark.table(path_cur_deaths_sing)
qa         = spark.table(f'{dbc}.{proj}_tmp_quality_assurance')

minap      = extract_batch_from_archive(parameters_df_datasets, 'minap')
# sgss     = extract_batch_from_archive(parameters_df_datasets, 'sgss')

# COMMAND ----------

display(skinny)

# COMMAND ----------

display(deaths)

# COMMAND ----------

display(qa)

# COMMAND ----------

display(minap)

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Prepare MINAP Data

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print('minap')
print('---------------------------------------------------------------------------------')
# reduce
_minap = minap.select('1_03_NHS_NUMBER_DEID', 'ARRIVAL_AT_HOSPITAL', '_AGE_AT_ADMISSION', 'DISCHARGE_DIAGNOSIS')\
  .withColumnRenamed('1_03_NHS_NUMBER_DEID', 'PERSON_ID')\
  .withColumn('_AGE_AT_ADMISSION', f.col('_AGE_AT_ADMISSION').cast("integer"))\
  .where(~f.col('PERSON_ID').isNull())

# check
count_var(_minap, 'PERSON_ID'); print()
# check discharge diagnosis
tab(_minap, 'DISCHARGE_DIAGNOSIS')
# check discharge diagnosis
tab(_minap, '_AGE_AT_ADMISSION')

# COMMAND ----------

# restrict to STEMI/NSTEMI MI events
# restrict to records within the study period
# restrict aged 18 at event
_minap = _minap\
  .where(f.col('DISCHARGE_DIAGNOSIS').isin(['1. Myocardial infarction (ST elevation)', '4. Acute coronary syndrome (troponin positive)/ nSTEMI']))\
  .where(f.col('_AGE_AT_ADMISSION') >= 18)\
  .where(f.col('ARRIVAL_AT_HOSPITAL') >= f.to_date(f.lit(study_start_date)))\
  .where(f.col('ARRIVAL_AT_HOSPITAL') <= f.to_date(f.lit(study_end_date)))
  
# check
count_var(_minap, 'PERSON_ID'); print()


# COMMAND ----------

# assign admission order field to patient MINAP records

# create window
_win_rownum = Window\
  .partitionBy('PERSON_ID')\
  .orderBy(f.col('ARRIVAL_AT_HOSPITAL').asc_nulls_last())

# create admission order field
_minap = _minap\
  .withColumn('adm_order', f.row_number().over(_win_rownum))\
  .sort('PERSON_ID', 'ARRIVAL_AT_HOSPITAL')

display(_minap)

# COMMAND ----------

# restrict to first admission per patient
_minap = _minap\
  .where(f.col('adm_order') == 1)\
  .drop('adm_order')\
  .withColumn('in_minap', f.lit(1))

# check
count_var(_minap, 'PERSON_ID'); print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Prepare Skinny, Deaths and QA

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print('skinny')
print('---------------------------------------------------------------------------------')
# reduce
_skinny = skinny.select('PERSON_ID', 'DOB', 'SEX', 'ETHNIC', 'ETHNIC_DESC', 'ETHNIC_CAT', 'in_gdppr')

# check
count_var(_skinny, 'PERSON_ID'); print()


print('---------------------------------------------------------------------------------')
print('deaths')
print('---------------------------------------------------------------------------------')
# reduce
_deaths = (
  deaths
  .select('PERSON_ID', f.col('REG_DATE_OF_DEATH').alias('DOD'), 'S_COD_CODE_UNDERLYING')
  .withColumn('in_deaths', f.lit(1))
)

# check
count_var(_deaths, 'PERSON_ID'); print()


print('---------------------------------------------------------------------------------')
print('quality assurance')
print('---------------------------------------------------------------------------------')
_qa = (
  qa
  .withColumn('in_qa', f.lit(1))
)

# check
count_var(_qa, 'PERSON_ID'); print()
tmpt = tab(_qa, '_rule_concat', '_rule_total', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Merge

# COMMAND ----------

print('---------------------------------------------------------------------------------')
print('merged')
print('---------------------------------------------------------------------------------')
# merge skinny and deaths
_merged = merge(_skinny, _deaths, ['PERSON_ID'], validate='1:1', keep_results=['both', 'left_only'], indicator=0); print()

# COMMAND ----------

# merge in qa
_merged = merge(_merged, _qa, ['PERSON_ID'], validate='1:1', assert_results=['both'], indicator=0); print()

# COMMAND ----------

# merge in minap
_merged = merge(_merged, _minap, ['PERSON_ID'], validate='1:1', indicator=0); print()

# COMMAND ----------

# add baseline_date
_merged = _merged.withColumn('baseline_date', f.to_date(f.lit(study_start_date)))

# COMMAND ----------

# check
display(_merged)

# COMMAND ----------

spark.sql(f'{dbc}.ccu046_01_tmp_inc_exc_merged')

# COMMAND ----------

# temp save
_merged = temp_save(df=_merged, out_name=f'{proj}_tmp_inc_exc_merged'); print()

# check
count_var(_merged, 'PERSON_ID'); print()

# COMMAND ----------

# check
tmpt = tab(_merged, 'in_minap'); print()
tmpt = tab(_merged, 'in_gdppr'); print()
tmpt = tab(_merged, 'in_deaths'); print()
tmpt = tab(_merged, 'in_qa'); print()

tmpt = tab(_merged, '_rule_total'); print()
tmpt = tab(_merged, '_rule_concat', '_rule_total', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md # 3 Inclusion / exclusion

# COMMAND ----------

tmp0 = _merged
tmpc = count_var(tmp0, 'PERSON_ID', ret=1, df_desc='original', indx=0); print()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 3.1 Restrict to Patients in our MINAP Selection

# COMMAND ----------

# check
tmpt = tab(tmp0, 'in_minap'); print()

# keep only those patients from the MINAP cohort selection
tmp1 = tmp0.where(f.col('in_minap') == 1)

# check
tmpt = count_var(tmp1, 'PERSON_ID', ret=1, df_desc='post exclusion of patients who are not in MINAP cohort selection', indx=1); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.2 Exclude patients aged < 18 at baseline

# COMMAND ----------

# eliminating this step as we are instead restricting to those that had an MI aged 18 + (i.e. aged 18 at time of MI event rather than at study start)

# COMMAND ----------

# MAGIC %md ## 3.2 Exclude patients aged > 115 at baseline

# COMMAND ----------

# filter out individuals aged > 115 (keep those <= 115 [i.e., born on or before 1904-11-01])

study_start_date_minus_115y = str(int(study_start_date[0:4]) - 115) + study_start_date[4:]
print(f'study_start_date_minus_115y = {study_start_date_minus_115y}'); print()

tmp1 = (
  tmp1
  .withColumn('_age_le_115', 
              f.when(f.col('DOB').isNull(), 0)
              .when(f.col('DOB') >= f.to_date(f.lit(study_start_date_minus_115y)), 1)
              .when(f.col('DOB') <  f.to_date(f.lit(study_start_date_minus_115y)), 2)
              .otherwise(999999)
             )
)

# check 
tmpt = tab(tmp1, '_age_le_115'); print()
assert tmp1.where(~(f.col('_age_le_115').isin([0,1,2]))).count() == 0
tmpt = tabstat(tmp1, 'DOB', byvar='_age_le_115', date=1); print()

# filter out and tidy
tmp2 = (
  tmp1
  .where(f.col('_age_le_115').isin([1]))
  .drop('_age_le_115')
)

# check
tmpt = count_var(tmp2, 'PERSON_ID', ret=1, df_desc='post exclusion of patients aged > 115 at baseline', indx=2); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.3 Exclude patients who died before baseline

# COMMAND ----------

# filter out patients who died before baseline
tmp2 = (
  tmp2
  .withColumn('DOD_flag', 
              f.when(f.col('DOD').isNull(), 1)
              .when(f.col('DOD') <= f.col('baseline_date'), 2)
              .when(f.col('DOD') > f.col('baseline_date'), 3)
              .otherwise(999999)
             )
)

# check 
tmpt = tab(tmp2, 'DOD_flag'); print()
assert tmp2.where(~(f.col('DOD_flag').isin([1,2,3]))).count() == 0
tmpt = tabstat(tmp2, 'DOD', byvar='DOD_flag', date=1); print()

# filter out and tidy
tmp3 = (
  tmp2
  .where(f.col('DOD_flag').isin([1,3]))
  .drop('DOD_flag')
)

# check
tmpt = count_var(tmp3, 'PERSON_ID', ret=1, df_desc='post exlusion of patients who died before baseline', indx=3); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.4 Exclude patients not in GDPPR

# COMMAND ----------

# check
tmpt = tab(tmp3, 'in_gdppr'); print()

# filter out patients not in GDPPR
#tmp4 = tmp3.where(f.col('in_gdppr') == 1)
tmp4 = tmp3

# check
tmpt = count_var(tmp4, 'PERSON_ID', ret=1, df_desc='post exclusion of patients not in GDPPR', indx=4); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md ## 3.5 Exclude patients who failed the quality assurance

# COMMAND ----------

# check
tmpt = tab(tmp4, '_rule_concat', '_rule_total'); print()

# filter out patients who failed the qaulity assurance
tmp5 = tmp4.where(f.col('_rule_total') == 0)
  
# temp save for data checks
tmpj = tmp4.where(f.col('_rule_total') > 1)
#tmpj = temp_save(df=tmpj, out_name=f'{proj}_tmp_inc_exc_qa'); print()
  
# check
tmpt = count_var(tmp5, 'PERSON_ID', ret=1, df_desc='post exclusion of patients who failed the quality assurance', indx=5); print()
tmpc = tmpc.unionByName(tmpt)

# COMMAND ----------

# MAGIC %md # 4 Flow diagram

# COMMAND ----------

# check flow table
tmpp = (
  tmpc
  .orderBy('indx')
  .select('indx', 'df_desc', 'n', 'n_id', 'n_id_distinct')
  .withColumnRenamed('df_desc', 'stage')
  .toPandas())
tmpp['n'] = tmpp['n'].astype(int)
tmpp['n_id'] = tmpp['n_id'].astype(int)
tmpp['n_id_distinct'] = tmpp['n_id_distinct'].astype(int)
tmpp['n_diff'] = (tmpp['n'] - tmpp['n'].shift(1)).fillna(0).astype(int)
tmpp['n_id_diff'] = (tmpp['n_id'] - tmpp['n_id'].shift(1)).fillna(0).astype(int)
tmpp['n_id_distinct_diff'] = (tmpp['n_id_distinct'] - tmpp['n_id_distinct'].shift(1)).fillna(0).astype(int)
for v in [col for col in tmpp.columns if re.search("^n.*", col)]:
  tmpp.loc[:, v] = tmpp[v].map('{:,.0f}'.format)
for v in [col for col in tmpp.columns if re.search(".*_diff$", col)]:  
  tmpp.loc[tmpp['stage'] == 'original', v] = ''
# tmpp = tmpp.drop('indx', axis=1)
print(tmpp.to_string()); print()

# COMMAND ----------

# MAGIC %md # 5 Save

# COMMAND ----------

# MAGIC %md ## 5.1 Cohort

# COMMAND ----------

tmpf = tmp5.select('PERSON_ID', 'DOB', 'SEX', 'ETHNIC', 'ETHNIC_DESC', 'ETHNIC_CAT', 'DOD', 'S_COD_CODE_UNDERLYING', 'in_gdppr', 'baseline_date', 'ARRIVAL_AT_HOSPITAL', 'DISCHARGE_DIAGNOSIS')

# check
count_var(tmpf, 'PERSON_ID')

# COMMAND ----------

# check 
display(tmpf)

# COMMAND ----------

#spark.sql(f'drop table dsa_391419_j3w9t_collab.ccu046_01_tmp_inc_exc_cohort')

# COMMAND ----------

#spark.sql(f'drop table dsa_391419_j3w9t_collab.ccu046_01_tmp_inc_exc_cohort')
outName = f'{proj}_tmp_inc_exc_cohort'.lower()
tmpf.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')

# COMMAND ----------

# save name
outName = f'{proj}_tmp_inc_exc_cohort'.lower()

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
tmpf.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

spark.sql(f'drop table dsa_391419_j3w9t_collab.ccu046_01_)
tmpf.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')

# COMMAND ----------

# MAGIC %md ## 5.2 Flow

# COMMAND ----------

display(tmpc)

# COMMAND ----------

# save name
outName = f'{proj}_tmp_inc_exc_flow'.lower()

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
tmpc.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

display(spark.table(f'{dbc}.ccu046_01_tmp_inc_exc_flow'))

# COMMAND ----------

display(tmpc)