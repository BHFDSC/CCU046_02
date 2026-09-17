# Databricks notebook source
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window
import databricks.koalas as ks
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re
import datetime
from matplotlib import dates as mdates

# COMMAND ----------

# MAGIC %run "/Repos/jn453@medschl.cam.ac.uk/shds/common/functions"

# COMMAND ----------

# MAGIC %md # 0 Parameters

# COMMAND ----------

# MAGIC %run "./CCU046_01-D01-parameters"

# COMMAND ----------

# MAGIC %md # 1 Data

# COMMAND ----------

gdppr   = extract_batch_from_archive(parameters_df_datasets, 'gdppr')
hes_apc = extract_batch_from_archive(parameters_df_datasets, 'hes_apc')
deaths  = extract_batch_from_archive(parameters_df_datasets, 'deaths')
minap   = extract_batch_from_archive(parameters_df_datasets, 'minap')

# COMMAND ----------

# MAGIC %md # 2 Coverage

# COMMAND ----------

# MAGIC %md ## 2.1 GDPPR

# COMMAND ----------

# gdppr - monthly
id_name = 'NHS_NUMBER_DEID'
date_name = 'DATE'

tmpg = gdppr\
  .withColumn('date_y', f.date_format(f.col(date_name), 'yyyy'))\
  .withColumn('date_m', f.month(f.col(date_name)))\
  .groupBy('date_y', 'date_m')\
  .agg(\
    f.count(f.lit(1)).alias('n')\
    , f.count(f.col(id_name)).alias('n_id')\
    , f.countDistinct(f.col(id_name)).alias('n_id_distinct')\
    , f.min(f.col(date_name)).alias('date_min')\
    , f.max(f.col(date_name)).alias('date_max')\
  )\
  .orderBy('date_y', 'date_m')\
  .where((f.col('date_y') >= '2019') & (f.col('date_y') <= '2024'))

tmpg1 = tmpg\
  .drop('date_min', 'date_max')\
  .toPandas()\
  .rename(columns={'n': 'N_n', 'n_id': 'N_n_id', 'n_id_distinct': 'N_n_id_distinct'})\

#.drop(['date_y'], axis=1)

tmpg2 = pd.wide_to_long(tmpg1, stubnames='N_', i=['date_y', 'date_m'], j='n_type', suffix='.+')
tmpg2 = tmpg2.reset_index()
# tmp1['date_ym' + '_formatted'] = pd.to_datetime(tmp1['date_ym'], errors='coerce')
# tmp1['d'] = tmp1['date_ym' + '_formatted'].dt.strftime('%d')
# tmp1['m'] = tmp1['date_ym' + '_formatted'].dt.strftime('%m')
# tmp1['y'] = tmp1['date_ym' + '_formatted'].dt.strftime('%Y')
# tmp1['year'] = tmp1['y'].astype(int)
# tmp1['ytmp'] = 2000
# tmp1['tmp'] = tmp1[['ytmp', 'm', 'd']].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns")
# tmp1['Date'] = pd.to_datetime(tmp1['tmp'])
g = sns.relplot(data=tmpg2, x='date_m', y="N_", col="n_type", hue='date_y', kind="line", marker = 'o', col_wrap=3, palette=sns.color_palette("tab10", tmpg2.date_y.nunique()), facet_kws = dict(sharey = False))
# xformatter = mdates.DateFormatter("%b")
# g.axes[0].xaxis.set_major_formatter(xformatter)
display(g.fig)

# COMMAND ----------

# MAGIC %md ## 2.2 HES_APC

# COMMAND ----------

# hes_apc monthly
id_name = 'PERSON_ID_DEID'
date_name = 'EPISTART'

tmph = hes_apc\
  .withColumn('date_y', f.date_format(f.col(date_name), 'yyyy'))\
  .withColumn('date_m', f.month(f.col(date_name)))\
  .groupBy('date_y', 'date_m')\
  .agg(\
    f.count(f.lit(1)).alias('n')\
    , f.count(f.col(id_name)).alias('n_id')\
    , f.countDistinct(f.col(id_name)).alias('n_id_distinct')\
    , f.min(f.col(date_name)).alias('date_min')\
    , f.max(f.col(date_name)).alias('date_max')\
  )\
  .orderBy('date_y', 'date_m')\
  .where((f.col('date_y') >= '2019') & (f.col('date_y') <= '2024'))

tmph1 = tmph\
  .drop('date_min', 'date_max')\
  .toPandas()\
  .rename(columns={'n': 'N_n', 'n_id': 'N_n_id', 'n_id_distinct': 'N_n_id_distinct'})\

#.drop(['date_y'], axis=1)

tmph2 = pd.wide_to_long(tmph1, stubnames='N_', i=['date_y', 'date_m'], j='n_type', suffix='.+')
tmph2 = tmph2.reset_index()
# tmp1['date_ym' + '_formatted'] = pd.to_datetime(tmp1['date_ym'], errors='coerce')
# tmp1['d'] = tmp1['date_ym' + '_formatted'].dt.strftime('%d')
# tmp1['m'] = tmp1['date_ym' + '_formatted'].dt.strftime('%m')
# tmp1['y'] = tmp1['date_ym' + '_formatted'].dt.strftime('%Y')
# tmp1['year'] = tmp1['y'].astype(int)
# tmp1['ytmp'] = 2000
# tmp1['tmp'] = tmp1[['ytmp', 'm', 'd']].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns")
# tmp1['Date'] = pd.to_datetime(tmp1['tmp'])
g = sns.relplot(data=tmph2, x='date_m', y="N_", col="n_type", hue='date_y', kind="line", marker = 'o', col_wrap=3, palette=sns.color_palette("tab10", tmph2.date_y.nunique()), facet_kws = dict(sharey = False))
# xformatter = mdates.DateFormatter("%b")
# g.axes[0].xaxis.set_major_formatter(xformatter)
display(g.fig)

# COMMAND ----------

# MAGIC %md ## 2.3 Deaths

# COMMAND ----------

# deaths - monthly
id_name = 'DEC_CONF_NHS_NUMBER_CLEAN_DEID'
date_name = 'REG_DATE_OF_DEATH'
date_format = 'yyyyMMdd'

tmpd = deaths\
  .withColumn(date_name, f.to_date(f.col(date_name), date_format))\
  .withColumn('date_y', f.date_format(f.col(date_name), 'yyyy'))\
  .withColumn('date_m', f.month(f.col(date_name)))\
  .groupBy('date_y', 'date_m')\
  .agg(\
    f.count(f.lit(1)).alias('n')\
    , f.count(f.col(id_name)).alias('n_id')\
    , f.countDistinct(f.col(id_name)).alias('n_id_distinct')\
    , f.min(f.col(date_name)).alias('date_min')\
    , f.max(f.col(date_name)).alias('date_max')\
  )\
  .orderBy('date_y', 'date_m')\
  .where((f.col('date_y') >= '2019') & (f.col('date_y') <= '2024'))

tmpd1 = tmpd\
  .drop('date_min', 'date_max')\
  .toPandas()\
  .rename(columns={'n': 'N_n', 'n_id': 'N_n_id', 'n_id_distinct': 'N_n_id_distinct'})\

#.drop(['date_y'], axis=1)

tmpd2 = pd.wide_to_long(tmpd1, stubnames='N_', i=['date_y', 'date_m'], j='n_type', suffix='.+')
tmpd2 = tmpd2.reset_index()
# tmp1['date_ym' + '_formatted'] = pd.to_datetime(tmp1['date_ym'], errors='coerce')
# tmp1['d'] = tmp1['date_ym' + '_formatted'].dt.strftime('%d')
# tmp1['m'] = tmp1['date_ym' + '_formatted'].dt.strftime('%m')
# tmp1['y'] = tmp1['date_ym' + '_formatted'].dt.strftime('%Y')
# tmp1['year'] = tmp1['y'].astype(int)
# tmp1['ytmp'] = 2000
# tmp1['tmp'] = tmp1[['ytmp', 'm', 'd']].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns")
# tmp1['Date'] = pd.to_datetime(tmp1['tmp'])
g = sns.relplot(data=tmpd2, x='date_m', y="N_", col="n_type", hue='date_y', kind="line", col_wrap=3, palette=sns.color_palette("tab10", tmpd2.date_y.nunique()), facet_kws = dict(sharey = False))
# xformatter = mdates.DateFormatter("%b")
# g.axes[0].xaxis.set_major_formatter(xformatter)
display(g.fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.4 NICOR MINAP

# COMMAND ----------

# minap - monthly
id_name = '1_03_NHS_NUMBER_DEID'
date_name = 'ARRIVAL_AT_HOSPITAL'

tmph = minap\
  .withColumn('date_y', f.date_format(f.col(date_name), 'yyyy'))\
  .withColumn('date_m', f.month(f.col(date_name)))\
  .groupBy('date_y', 'date_m')\
  .agg(\
    f.count(f.lit(1)).alias('n')\
    , f.count(f.col(id_name)).alias('n_id')\
    , f.countDistinct(f.col(id_name)).alias('n_id_distinct')\
    , f.min(f.col(date_name)).alias('date_min')\
    , f.max(f.col(date_name)).alias('date_max')\
  )\
  .orderBy('date_y', 'date_m')\
  .where(f.col('date_y') <= '2024')

tmph1 = tmph\
  .drop('date_min', 'date_max')\
  .toPandas()\
  .rename(columns={'n': 'N_n', 'n_id': 'N_n_id', 'n_id_distinct': 'N_n_id_distinct'})\

#.drop(['date_y'], axis=1)

tmph2 = pd.wide_to_long(tmph1, stubnames='N_', i=['date_y', 'date_m'], j='n_type', suffix='.+')
tmph2 = tmph2.reset_index()
# tmp1['date_ym' + '_formatted'] = pd.to_datetime(tmp1['date_ym'], errors='coerce')
# tmp1['d'] = tmp1['date_ym' + '_formatted'].dt.strftime('%d')
# tmp1['m'] = tmp1['date_ym' + '_formatted'].dt.strftime('%m')
# tmp1['y'] = tmp1['date_ym' + '_formatted'].dt.strftime('%Y')
# tmp1['year'] = tmp1['y'].astype(int)
# tmp1['ytmp'] = 2000
# tmp1['tmp'] = tmp1[['ytmp', 'm', 'd']].apply(lambda x: '-'.join(x.values.astype(str)), axis="columns")
# tmp1['Date'] = pd.to_datetime(tmp1['tmp'])
g = sns.relplot(data=tmph2, x='date_m', y="N_", col="n_type", hue='date_y', kind="line", marker = 'o', col_wrap=3, palette=sns.color_palette("tab10", tmph2.date_y.nunique()), facet_kws = dict(sharey = False))
# xformatter = mdates.DateFormatter("%b")
# g.axes[0].xaxis.set_major_formatter(xformatter)
display(g.fig)
