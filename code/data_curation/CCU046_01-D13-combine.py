# Databricks notebook source
# MAGIC %md
# MAGIC # CCU046_01-D13-combine
# MAGIC
# MAGIC **Description** This notebook combines the cohort, covariates, exposures and minap out tables into a single analysis ready table.
# MAGIC
# MAGIC **Authors** Tom Bolton, John Nolan
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on CCU003_05
# MAGIC
# MAGIC **Notes**

# COMMAND ----------

spark.sql('CLEAR CACHE')

# COMMAND ----------

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

# MAGIC %run "/Repos/jn453@medschl.cam.ac.uk/shds/common/functions"

# COMMAND ----------

# MAGIC %md # 0 Parameters

# COMMAND ----------

# MAGIC %run "./CCU046_01-D01-parameters" 

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

# cohort
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_cohort')
cohort = spark.table(f'{dbc}.{proj}_out_cohort')

# covariates comorbidities
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_comorbidities')
covariates = spark.table(f'{dbc}.{proj}_out_comorbidities')

# covariates LSOA
spark.sql(f'REFRESH TABLE {dbc}.{proj}_tmp_covariates_lsoa')
covariates_lsoa = spark.table(f'{dbc}.{proj}_tmp_covariates_lsoa')

# HES APC - MI
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_gdppr_hes_apc_mi_event')
mi_event = spark.table(f'{dbc}.{proj}_out_gdppr_hes_apc_mi_event')

# exposure - SMI
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_exposure')
exposure = spark.table(f'{dbc}.{proj}_out_exposure')

# exposure - COVID
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_inf_exposures_covid')
covid = spark.table(f'{dbc}.{proj}_out_inf_exposures_covid')

# MINAP
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_minap')
minap = spark.table(f'{dbc}.{proj}_out_minap')

 # HES APC /CC
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_hes_apc')
hes_apc = spark.table(f'{dbc}.{proj}_out_hes_apc')


# COMMAND ----------

# MAGIC %md # 2 Check

# COMMAND ----------

display(cohort)

# COMMAND ----------

display(covariates)

# COMMAND ----------

display(covariates_lsoa)

# COMMAND ----------

display(exposure)

# COMMAND ----------

display(covid)

# COMMAND ----------

display(minap)

# COMMAND ----------

display(hes_apc)

# COMMAND ----------

# MAGIC %md # 3 Create

# COMMAND ----------

tmp1 = merge(cohort, covariates, ['PERSON_ID'], validate='1:1', assert_results=['both'], indicator=0); print()

# COMMAND ----------

tmp2 = merge(tmp1, exposure, ['PERSON_ID'], validate='1:1', assert_results=['both', 'left_only'], indicator=0); print()

# COMMAND ----------

tmp3 = merge(tmp2, covid, ['PERSON_ID', 'mi_event_date'], validate='1:1', assert_results=['both', 'left_only'], indicator=0); 

# COMMAND ----------

tmp4 = merge(tmp3, minap, ['PERSON_ID'], validate='1:1', assert_results=['both', 'left_only'], indicator=0); 

# COMMAND ----------

tmp5 = merge(tmp4, hes_apc, ['PERSON_ID'], validate='1:1', assert_results=['both', 'left_only'], indicator=0); 

# COMMAND ----------

tmp6 = merge(tmp5, mi_event, ['PERSON_ID'], validate='1:1', assert_results=['both', 'left_only'], indicator=0); 

# COMMAND ----------

tmp7 = merge(tmp6, covariates_lsoa, ['PERSON_ID'], validate='1:1', assert_results=['both', 'left_only'], indicator=0); 

# COMMAND ----------

# MAGIC %md # 4 Check

# COMMAND ----------

# check
count_var(tmp7, 'PERSON_ID'); print()
print(len(tmp7.columns)); print()
print(pd.DataFrame({f'_cols': tmp7.columns}).to_string()); print()
#tmpt = tab(tmp7, 'CHUNK'); print()

# COMMAND ----------

display(tmp7)

# COMMAND ----------

# MAGIC %md # 5 Save

# COMMAND ----------

#spark.sql(f'drop table dsa_391419_j3w9t_collab.ccu046_01_out_cohort_combined')

# COMMAND ----------

# save name
outName = f'{proj}_out_cohort_combined'.lower()

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
#  #spark.sql(f'ALTER TABLE {dbc}.{outName_pre} OWNER TO {dbc}')

# save
spark.sql(f'drop table dsa_391419_j3w9t_collab.ccu046_01_out_cohort_combined')
tmp7.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

count_var(tmp7, 'PERSON_ID')