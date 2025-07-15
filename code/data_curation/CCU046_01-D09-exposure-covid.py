# Databricks notebook source
# MAGIC %md # CCU046_01-D10a-exposures-covid
# MAGIC  
# MAGIC **Description** This notebook creates the COVID-19 Infection exposures. 
# MAGIC  
# MAGIC **Authors** Tom Bolton
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on previous work by Tom Bolton (John Nolan, Elena Raffetti) for CCU018_01 and the earlier CCU002 sub-projects.
# MAGIC
# MAGIC **Notes**
# MAGIC
# MAGIC **Exposures for analyses on COVID-19 infection:**
# MAGIC Identify any Confirmed Positive COVID-19 Infection within 14 days either side of MI Event Date
# MAGIC Create binary flag to indicate Infection and include date of positive test / diagnosis / admission in output

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

# DBTITLE 1,Functions
# MAGIC %run "/Repos/jn453@medschl.cam.ac.uk/shds/common/functions"

# COMMAND ----------

# MAGIC %md # 0 Parameters

# COMMAND ----------

# MAGIC %run "./CCU046_01-D01-parameters"

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

# cohort = spark.table(path_out_cohort)
spark.sql(f'REFRESH TABLE {dbc}.{proj}_out_cohort')
cohort = spark.table(f'{dbc}.{proj}_out_cohort')

# covid  = spark.table(f'{dbc}.{proj}_cur_covid')
spark.sql(f'REFRESH TABLE {dbc}.{proj}_cur_covid')
covid = spark.table(f'{dbc}.{proj}_cur_covid')

# COMMAND ----------

display(cohort)

# COMMAND ----------

display(covid)

# COMMAND ----------

# MAGIC %md # 2 Prepare

# COMMAND ----------

# check
count_var(cohort, 'PERSON_ID'); print()
count_var(covid, 'PERSON_ID'); print()

# restrict to cohort
_covid = merge(covid, cohort.select('PERSON_ID', 'mi_event_date'), ['PERSON_ID'], validate='m:1', keep_results=['both'], indicator=0); print()

# COMMAND ----------

# check
count_var(_covid, 'PERSON_ID'); print()

# COMMAND ----------

display(_covid)

# COMMAND ----------

# MAGIC %md # 3 Infection

# COMMAND ----------

# MAGIC %md ## 3.1 Check

# COMMAND ----------

# check infection
count_var(_covid, 'PERSON_ID'); print()
tmpt = tabstat(_covid, 'DATE', date=1); print()
tmp1 = _covid.withColumn('source_pheno', f.concat_ws('_', f.col('source'), f.col('covid_phenotype')))
tmpt = tabstat(tmp1, 'DATE', byvar='source_pheno', date=1); print()
tmpt = tab(_covid, 'covid_phenotype', 'covid_status', var2_unstyled=1); print()
tmpt = tab(_covid, 'covid_phenotype', 'source', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md ## 3.2 Prepare

# COMMAND ----------

print('------------------------------------------------------------------------------')
print('confirmed COVID-19 (as defined for CCU002_01)')
print('------------------------------------------------------------------------------')
# note: essentially, we are simply dropping the suspected records, but the verbose where statement below 
# will be needed when the covid table is expanded to include critical care and death records
covid_confirmed = _covid\
  .where(\
    (f.col('covid_phenotype').isin([
      '01_Covid_positive_test'
      , '01_GP_covid_diagnosis'
      , '02_Covid_admission_any_position'
      , '02_Covid_admission_primary_position'
    ]))\
    & (f.col('source').isin(['sgss', 'gdppr', 'hes_apc', 'sus']))\
    & (f.col('covid_status').isin(['confirmed', '']))\
  )

# check
count_var(covid_confirmed, 'PERSON_ID'); print()
print(covid_confirmed.limit(10).toPandas().to_string(max_colwidth=50)); print()
tmpt = tabstat(covid_confirmed, 'DATE', date=1); print()
tmp1 = covid_confirmed.withColumn('source_pheno', f.concat_ws('_', f.col('source'), f.col('covid_phenotype')))
tmpt = tabstat(tmp1, 'DATE', byvar='source_pheno', date=1); print()
tmpt = tab(covid_confirmed, 'covid_phenotype', 'covid_status', var2_unstyled=1); print()
tmpt = tab(covid_confirmed, 'covid_phenotype', 'source', var2_unstyled=1); print()

# COMMAND ----------

# MAGIC %md ## 3.3 Create

# COMMAND ----------

print('------------------------------------------------------------------------------')
print('calculate days difference between COVID-19 Infection Date and MI_event_date')
print('------------------------------------------------------------------------------')
covid_confirmed_days = (covid_confirmed
                        .withColumn('days_diff', f.datediff('DATE','mi_event_date'))
                        .withColumn('abs_days_diff', f.abs(f.col('days_diff'))))                        
display(covid_confirmed_days)
                 

# COMMAND ----------

display(covid_confirmed_days.where(f.col('PERSON_ID') == 'PN8I0GDS15CZ0L4'))

# COMMAND ----------

covid_confirmed_days_event = (covid_confirmed_days
                              .where(f.col('abs_days_diff') <= 14))
display(covid_confirmed_days_event)


# COMMAND ----------

covid_confirmed_days_event_min = (covid_confirmed_days_event
                                  .groupBy('PERSON_ID', 'mi_event_date')
                                  .agg(f.min(f.col('DATE')).alias('exp_covid_date'))
                                  .withColumn('exp_covid_flag', f.lit(1))
                                  .select('PERSON_ID', 'mi_event_date', 'exp_covid_flag', 'exp_covid_date'))

# COMMAND ----------

display(covid_confirmed_days_event_min.where(f.col('PERSON_ID') == 'PN8I0GDS15CZ0L4'))

# COMMAND ----------

# MAGIC %md ## 3.4 Check

# COMMAND ----------

# check 
display(covid_confirmed_days_event_min)

# COMMAND ----------

count_var(covid_confirmed_days_event_min, 'PERSON_ID')

# COMMAND ----------

# MAGIC %md # 4 Save

# COMMAND ----------

# merge 
tmp1 = merge(covid_confirmed_days_event_min, cohort.select('PERSON_ID', 'mi_event_date'), ['PERSON_ID', 'mi_event_date'], validate='1:1', assert_results=['both', 'right_only'], indicator=0); print()

# check
count_var(tmp1, 'PERSON_ID'); print()

# COMMAND ----------

# check final
display(tmp1)

# COMMAND ----------

# save name
outName = f'{proj}_out_inf_exposures_covid'.lower()

# # save previous version for comparison purposes
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
tmp1.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')

# COMMAND ----------

# MAGIC %md # 5 Plots

# COMMAND ----------

_tmpp = (covid_confirmed_days_event
         .groupBy('source', 'days_diff')
         .agg(f.count('PERSON_ID').alias('recs'),
              f.countDistinct('PERSON_ID').alias('pats'))
         .toPandas())

# COMMAND ----------

# set the grid size and style
sns.set(style="whitegrid", rc={'figure.figsize':(15,6), "xtick.bottom" : True, "ytick.left" : False})

#Palette 
chosen_palette = 'Dark2'

dc = sns.barplot(data=_tmpp, x='days_diff', y='pats', hue='source', palette=chosen_palette)
#ylabels = ['{:,.1f}'.format(y) + 'M' for y in dc.get_yticks()/1000000]
#dc.set_yticklabels(ylabels)
#dc.set_title("Covid-19 Infection by Source and Days Difference to MI Event Date")
#dc.set_xlabel("Days")
#dc.set_ylabel("")
#dc.xaxis.set_major_locator(mdates.MonthLocator(bymonth=(4, 8, 12)))
#dc.xaxis.set_major_formatter(myFmt)
#dc.tick_params(axis='x', labelrotation = 75)
display(dc)