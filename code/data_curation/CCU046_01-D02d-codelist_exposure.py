# Databricks notebook source
# MAGIC %md # CCU046_01-D02b-codelist_exposure
# MAGIC
# MAGIC **Description** This notebook creates the codelist for the SMI exposures: Schizophrenia, Bipolar Disorder and Depression.
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

# =======================================================================================
# NOTES
# =======================================================================================

# 3 conditions: Schizophrenia, Bipolar Disorder and Depression

# ---------------------------------------------------------------------------------------
# Schizophrenia
# ---------------------------------------------------------------------------------------
# 


# ---------------------------------------------------------------------------------------
# Bipolar Disorder
# ---------------------------------------------------------------------------------------



# ---------------------------------------------------------------------------------------
# Depression
# ---------------------------------------------------------------------------------------


# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

# create ICD10 lookup table
icd10 = spark.table(path_ref_icd10)

# restrict to latest available version of ICD10
icd10 = icd10\
  .where(f.col('VERSION') == "5th ed")\
  .select('ALT_CODE', 'ICD10_DESCRIPTION')\
  .withColumnRenamed('ALT_CODE', 'code')\
  .withColumnRenamed('ICD10_DESCRIPTION', 'term')

# remove trailing X's, decimal points, dashes, and spaces
icd10 = icd10\
  .withColumn('code', f.regexp_replace('code', r'X$', ''))\
  .withColumn('code', f.regexp_replace('code', r'[\.\-\s]', ''))\
  .withColumn('code_3', f.substring('code', 1, 3))

# check for duplicates
#count_var(icd10, 'code')

# COMMAND ----------

# create SNOMED lookup table
sct = spark.table(path_ref_gdppr_refset)

# get single code term per code
w = Window.partitionBy('ConceptId').orderBy(f.col('Code_Status_Date').desc())
sct = sct\
  .withColumn('rownum', f.row_number().over(w))\
  .where(f.col('rownum') == 1)\
  .select('ConceptId', 'ConceptId_Description')\
  .withColumnRenamed('ConceptId', 'code')\
  .withColumnRenamed('ConceptId_Description', 'term_gdppr')  
#count_var(sct, 'code')

# COMMAND ----------

# MAGIC %md
# MAGIC #2 Create Codelist

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.1 ICD-10

# COMMAND ----------

# ICD10 codelist:

codelist_icd = """
name,code_3
Schizophrenia,F20
Schizophrenia,F25
Bipolar disorder,F30
Bipolar disorder,F31
Depression,F32
Depression,F33

"""
codelist_icd = (
  spark.createDataFrame(
    pd.DataFrame(pd.read_csv(io.StringIO(codelist_icd)))
    .fillna('')
    .astype(str)
  )
)

# merge to ICD10 lookup
codelist_icd = codelist_icd\
  .withColumn('terminology', f.lit('ICD10'))\
  .join(icd10.withColumn('inICD10', f.lit(1)), on = 'code_3', how = 'left')\
  .withColumn('inICD10', f.when(f.col('inICD10').isNull(), f.lit(0)).otherwise(f.col('inICD10')))\
  .select('name', 'code', 'term', 'terminology', 'inICD10')

# check
tab(codelist_icd, 'inICD10')

# final
codelist_icd = codelist_icd\
  .drop('inICD10')

# temp save 
# as this will be accessed by data_check\CCU003_05-D02b-codelist_exposures_and_outcomes_comparison
# saving here so that we only have one version of the import to keep track of
# codelist_icd = temp_save(df=codelist_icd, out_name=f'{proj}_tmp_codelist_exp'); print()

# COMMAND ----------

display(codelist_icd)

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.2 SNOMED-CT

# COMMAND ----------

# SNOMED-CT codelist:
codelist_sct_schizophrenia = spark.table(path_codelist_sct_schizophrenia)
codelist_sct_bipolar = spark.table(path_codelist_sct_bipolar)
codelist_sct_depression = spark.table(path_codelist_sct_depression)

codelist_sct = (
  codelist_sct_schizophrenia
  .unionByName(codelist_sct_bipolar)
  .unionByName(codelist_sct_depression)
  .withColumn('terminology', f.lit('SNOMED'))
)
display(codelist_sct)

# COMMAND ----------

# merge to SCT GDPPR lookup
codelist_sct = codelist_sct\
  .join(sct.withColumn('inSCT', f.lit(1)), on = 'code', how = 'left')\
  .withColumn('inSCT', f.when(f.col('inSCT').isNull(), f.lit(0)).otherwise(f.col('inSCT')))

# check
tab(codelist_sct, 'inSCT')


# COMMAND ----------

tab(codelist_sct, 'condition')

# COMMAND ----------

# re-format and create final Snomed codelist
codelist_sct = codelist_sct\
  .withColumnRenamed('condition', 'name')\
  .withColumnRenamed('description', 'term')\
  .withColumn('terminology', f.lit('SNOMED'))\
  .select('name', 'code', 'term', 'terminology')\
  .where(f.col('name').isin(['Schizophrenia', 'Bipolar disorder', 'Depression']))
display(codelist_sct)

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.3 Combine

# COMMAND ----------

codelist_exp = codelist_icd\
  .union(codelist_sct)

# COMMAND ----------

# MAGIC %md # 3 Check

# COMMAND ----------

# check
tmpt = tab(codelist_exp, 'name'); print()
tmpt = tab(codelist_exp, 'terminology'); print()
tmpt = tab(codelist_exp, 'name', 'terminology'); print()
print(codelist_exp.orderBy('name', 'terminology', 'code').limit(10).toPandas().to_string()); print()

# COMMAND ----------

# check
display(codelist_exp)

# COMMAND ----------

# MAGIC %md # 4 Save

# COMMAND ----------

# save name
outName = f'{proj}_out_codelist_exposure'

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
codelist_exp.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')