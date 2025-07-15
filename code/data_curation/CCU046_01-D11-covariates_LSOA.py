# Databricks notebook source
# MAGIC %md # CCU046_01-D11b-covariates_LSOA
# MAGIC  
# MAGIC **Description** This notebook creates the covariates based on LSOA. LSOA will be used to derive MSOA, region and index of multiple deprivation. As there is no LSOA variable in MINAP, LSOA will be taken from a HES APC record corresponding to the MINAP record, or from the GDPPR data if not available from HES APC.
# MAGIC  
# MAGIC **Authors** Tom Bolton, John Nolan
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on previous work for CCU018_01 and the earlier CCU002 sub-projects.
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

spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_cohort')

lsoa_region  = spark.table(path_cur_lsoa_region)
lsoa_imd     = spark.table(path_cur_lsoa_imd)
cohort       = spark.table(path_out_cohort)
cohort_hes_apc = spark.table(f'dsa_391419_j3w9t_collab.{proj}_out_hes_apc')
gdppr        = extract_batch_from_archive(parameters_df_datasets, 'gdppr')

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

display(cohort_hes_apc)

# COMMAND ----------

count_var(cohort, 'PERSON_ID'); print()
count_var(cohort_hes_apc, 'PERSON_ID'); print()

# COMMAND ----------

# get LSOA from hes_apc
tmp1 = (cohort_hes_apc
        .join(cohort.select('PERSON_ID', 'mi_event_date'), on = 'PERSON_ID', how = 'left')
        .select('PERSON_ID', 'LSOA11_HES', 'mi_event_date')
        .withColumnRenamed('LSOA11_HES', 'LSOA')
        .withColumnRenamed('mi_event_date', 'LSOA_date')
        .where(f.col('LSOA').isNotNull())
        .withColumn('HES', f.lit(1)))

# COMMAND ----------

count_var(cohort_hes_apc.where(f.col('LSOA11_HES').isNotNull()), 'PERSON_ID')

# COMMAND ----------

display(tmp1)

# COMMAND ----------

# get distinct LSOA from the batch of GDPPR
# keep practice for inspection of non-distinct LSOA by PERSON_ID and REPORTING_PERIOD_END_DATE
tmp2 = (
  gdppr
  .select(f.col('NHS_NUMBER_DEID').alias('PERSON_ID'), 'PRACTICE', f.col('REPORTING_PERIOD_END_DATE').alias('RPED'), 'LSOA')
  .where(f.col('PERSON_ID').isNotNull())
  .where(f.col('RPED').isNotNull())
  .where(f.col('LSOA').isNotNull())
  .dropDuplicates(['PERSON_ID', 'RPED', 'LSOA'])
  .withColumn('HES', f.lit(0))
  .withColumnRenamed('RPED', 'LSOA_date')
)

# filter to the cohort of interest
# merge(tmp3, cohort.select('PERSON_ID'), ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0); print()
tmp2 = (
  tmp2
  .join(cohort.select('PERSON_ID'), on=['PERSON_ID'], how='inner')
)

display(tmp2)

# COMMAND ----------

# union
tmp3 = (
  tmp1
  .unionByName(tmp2, allowMissingColumns=True)
  .dropDuplicates(['PERSON_ID', 'LSOA', 'HES', 'LSOA_date'])
)

# COMMAND ----------

#display(tmp3.orderBy('PERSON_ID'))

# COMMAND ----------

#spark.sql(f'drop table dsa_391419_j3w9t_collab.ccu046_01_tmp_lsoa_gdppr')

# COMMAND ----------

spark.sql(f'drop table dsa_391419_j3w9t_collab.ccu046_01_tmp_lsoa_gdppr')

# COMMAND ----------

# temp save (checkpoint)
tmp3 = temp_save(df=tmp3, out_name=f'{proj}_tmp_lsoa_gdppr'); print()

# check
count_var(cohort, 'PERSON_ID'); print()
count_var(tmp3, 'PERSON_ID'); print()

# COMMAND ----------

# MAGIC %md # 3 Filter

# COMMAND ----------

# Filter to the HES Record (if available)
# If HES not available filter to the earliest record(s)
# this will include multiple rows per individual where there are more than one LSOA relating to the earliest date from GDPPR
# we keep these for now and see if we they actually result in conflicts for region and IMD
# e.g., an individual may have two different LSOA's on the same date, but these both may have region == London and IMD == 1, so are not conflicting
# on the otherhand if an individual had two different LSOA's on the same date, but these related to different regions and IMD then we have a conflict and should not use region or IMD for this individual
win_denserank = Window\
  .partitionBy('PERSON_ID')\
  .orderBy(f.desc('HES'), 'LSOA_date')      
win_rownum = Window\
  .partitionBy('PERSON_ID')\
  .orderBy(f.desc('HES'), 'LSOA_date', 'LSOA')
win_rownummax = Window\
  .partitionBy('PERSON_ID')
tmp4 = (
  tmp3
  .withColumn('dense_rank', f.dense_rank().over(win_denserank))
  .where(f.col('dense_rank') == 1)
  .drop('dense_rank')
  .withColumn('rownum', f.row_number().over(win_rownum))
  .withColumn('rownummax', f.count('PERSON_ID').over(win_rownummax))
  .withColumn('LSOA_1', f.substring(f.col('LSOA'), 1, 1))
)

# check
count_var(tmp4, 'PERSON_ID'); print()
tmpt = tab(tmp4.where(f.col('rownum') == 1), 'rownummax'); print()
tmpt = tab(tmp4, 'LSOA_1'); print()

# COMMAND ----------

# MAGIC %md # 5 Add region

# COMMAND ----------

# prepare mapping
lsoa_region_1 = lsoa_region\
  .select('lsoa_code', 'lsoa_name', 'region_code', 'region_name')\
  .withColumnRenamed('lsoa_code','LSOA')

# check
tmpt = tab(lsoa_region_1, 'region_name'); print()

# map
tmp5 = merge(tmp4, lsoa_region_1, ['LSOA'], validate='m:1', keep_results=['both', 'left_only']); print()
        
# check
count_var(tmp5, 'PERSON_ID'); print()
tmpt = tab(tmp5, 'LSOA_1', '_merge'); print()

# COMMAND ----------

# MAGIC %md # 6 Add IMD

# COMMAND ----------

# check
tmpt = tab(lsoa_imd, 'IMD_2019_DECILES', 'IMD_2019_QUINTILES', var2_unstyled=1); print()

# map
tmp6 = merge(tmp5, lsoa_imd.drop('IMD_2019_QUINTILES'), ['LSOA'], validate='m:1', keep_results=['both', 'left_only']); print()
  
# check
count_var(tmp6, 'PERSON_ID'); print()
tmpt = tab(tmp6, 'LSOA_1', '_merge', var2_unstyled=1); print()  

# tidy
tmp6 = (
  tmp6
  .drop('_merge')    
)

# COMMAND ----------

# MAGIC %md # 7 Collapse

# COMMAND ----------

# collapse to 1 row per individual
win_egen = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('rownum')\
  .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
win_lag = Window\
  .partitionBy('PERSON_ID')\
  .orderBy('rownum')

# note - intentionally not using null safe equality for region_lag1_diff
tmp7 = (
  tmp6
  .orderBy('PERSON_ID', 'rownum')  
  .withColumn('LSOA_conflict', f.when(f.col('rownummax') > 1, 1).otherwise(0))  
  .withColumn('IMD_min', f.min(f.col('IMD_2019_DECILES')).over(win_egen))
  .withColumn('IMD_max', f.max(f.col('IMD_2019_DECILES')).over(win_egen))
  .withColumn('IMD_conflict', f.lit(1) - udf_null_safe_equality('IMD_min', 'IMD_max').cast(t.IntegerType()))
  .withColumn('region_lag1', f.lag(f.col('region_name'), 1).over(win_lag))
  .withColumn('region_lag1_diff', f.when(f.col('region_name') != f.col('region_lag1'), 1).otherwise(0)) 
  .withColumn('region_conflict', f.max(f.col('region_lag1_diff')).over(win_egen))
)

# check
tmpt = tab(tmp7, 'HES'); print()
tmpt = tab(tmp7, 'LSOA_conflict', 'IMD_conflict'); print()
tmpt = tab(tmp7, 'LSOA_conflict', 'region_conflict'); print()
tmpt = tab(tmp7, 'rownummax', 'IMD_conflict'); print()
tmpt = tab(tmp7, 'rownummax', 'region_conflict'); print()
tmpt = tab(tmp7.where(f.col('rownum') == 1), 'rownummax', 'IMD_conflict'); print()
tmpt = tab(tmp7.where(f.col('rownum') == 1), 'rownummax', 'region_conflict'); print()

# COMMAND ----------

# check
display(tmp7.where(f.col('rownummax') > 1))

# COMMAND ----------

# finalise
tmp8 = (
  tmp7
  .withColumn('IMD_2019_DECILES', f.when((f.col('rownummax') > 1) & (f.col('IMD_conflict') == 1), f.lit(None)).otherwise(f.col('IMD_2019_DECILES')))       
  .withColumn('region_name', f.when((f.col('rownummax') > 1) & (f.col('region_conflict') == 1), f.lit(None)).otherwise(f.col('region_name')))       
  .withColumn('LSOA_source', f.when(f.col('HES')==1, 'HES').otherwise('GDPPR'))
  .select('PERSON_ID', 'LSOA', 'LSOA_source',  'LSOA_date',  'LSOA_conflict', 'region_name', 'region_conflict',  'IMD_2019_DECILES', 'IMD_conflict', 'rownum', 'rownummax')
  .where(f.col('rownum') == 1)
)
        
# check
tmpt = tab(tmp8, 'rownummax', 'LSOA_source'); print()
tmpt = tab(tmp8, 'rownummax', 'LSOA_conflict'); print()
tmpt = tab(tmp8, 'rownummax', 'region_conflict'); print()
tmpt = tab(tmp8, 'rownummax', 'IMD_conflict'); print()
tmpt = tab(tmp8, 'region_name', 'region_conflict'); print()
tmpt = tab(tmp8, 'IMD_2019_DECILES', 'IMD_conflict'); print()
tmpt = tab(tmp8, 'region_conflict', 'IMD_conflict'); print()
tmpt = tab(tmp8.where(f.col('rownummax') > 1), 'region_conflict', 'IMD_conflict'); print()
tmpt = tabstat(tmp8, 'LSOA_date', date=1); print()

# tidy
tmp9 = (
  tmp8
  .drop('rownum', 'rownummax')
)

# COMMAND ----------

# check
display(tmp9)

# COMMAND ----------

count_var(tmp9, 'PERSON_ID')

# COMMAND ----------

# MAGIC %md # 8 Save

# COMMAND ----------

# save name
outName = f'{proj}_tmp_covariates_lsoa'.lower()

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

# save
print(f'saving {dbc}.{outName}')
tmp9.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
print(f'  saved')