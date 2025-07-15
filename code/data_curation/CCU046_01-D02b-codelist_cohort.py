# Databricks notebook source
# MAGIC %md # CCU046_01-D02b-codelist_cohort
# MAGIC
# MAGIC **Description** This notebook creates the codelist for Myocardial infarction.
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

# gdppr refset
gdppr_refset = spark.table(path_ref_gdppr_refset)

# icd lookup table
icd = spark.table(path_ref_icd10)

# COMMAND ----------

# create icd-10 lookup table
icd_lkp = icd\
  .select('ALT_CODE', 'ICD10_DESCRIPTION')\
  .withColumnRenamed('ALT_CODE', 'code')\
  .withColumnRenamed('ICD10_DESCRIPTION', 'term')\
  .withColumn('code', f.regexp_replace(f.col('code'), r'X$', ''))\
  .distinct()\
  .orderBy('code')
display(icd_lkp)

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

# ICD10 codelist:
codelist_icd10_mi = """
name,code
ami,I21
ami,I22
"""
codelist_icd10_mi = (
  spark.createDataFrame(
    pd.DataFrame(pd.read_csv(io.StringIO(codelist_icd10_mi)))
    .fillna('')
    .astype(str)
  )
)

# merge to ICD10 lookup
codelist_icd10_mi = codelist_icd10_mi\
  .join(icd_lkp
        .withColumn('inICD10', f.lit(1)),
        on = 'code',
        how = 'left')\
  .withColumn('inICD10', f.when(f.col('inICD10').isNull(), f.lit(0)).otherwise(f.col('inICD10')))\
  .select('name', 'code', 'term', 'inICD10')

# check
tab(codelist_icd10_mi, 'inICD10')

# final
codelist_icd10_mi = codelist_icd10_mi\
  .drop('inICD10')\
  .withColumn('terminology', f.lit('ICD10'))

# display
display(codelist_icd10_mi)

# COMMAND ----------

codelist_sct_mi = (spark.table(path_codelist_sct_myocardial_infarction)
                   .withColumn('condition', f.lit('ami'))
                   .withColumnRenamed('condition', 'name')
                   .withColumnRenamed('description', 'term')
                   .withColumn('terminology', f.lit('SNOMED')))

# COMMAND ----------

codelist_mi = (codelist_icd10_mi.unionByName(codelist_sct_mi))
display(codelist_mi)

# COMMAND ----------

# check 
tmpt = tab(codelist_mi, 'name', 'terminology')

# COMMAND ----------

# remove trailing X's, decimal points, dashes, and spaces
codelist_mi = (
  codelist_mi
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'X$', '')).otherwise(f.col('code')))\
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'[\.\-\s]', '')).otherwise(f.col('code')))
)

# COMMAND ----------

# MAGIC %md # 3 Check

# COMMAND ----------

# check 
tmpt = tab(codelist_mi, 'name', 'terminology')

# COMMAND ----------

# check
display(codelist_mi)

# COMMAND ----------

# MAGIC %md # 4 Save

# COMMAND ----------

# save name
outName = f'{proj}_out_codelist_cohort'

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

# save
codelist_mi.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')