# Databricks notebook source
# MAGIC %md # CCU046_01-D12-minap
# MAGIC  
# MAGIC **Description** This notebook gets all of the required variables for covariates and outcomes from the MI event record in MINAP:
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
#hes_apc      = extract_batch_from_archive(parameters_df_datasets, 'hes_apc')
#hes_apc_long = spark.table(path_cur_hes_apc_long)
#hes_op_long = spark.table(path_cur_hes_op_long)
#hes_cc      = extract_batch_from_archive(parameters_df_datasets, 'hes_cc')

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

## 2.2 Prepare MINAP data

print('--------------------------------------------------------------------------------------')
print('minap')
print('--------------------------------------------------------------------------------------')
# reduce, rename and re-format columns
minap_prepared = (minap
                  .select('1_03_NHS_NUMBER_DEID', 
                          'ARRIVAL_AT_HOSPITAL', 'CALL_FOR_HELP',
                          'ADMISSION_WARD', 
                          'GENDER', '_AGE_AT_ADMISSION', 
                          'HEIGHT', 'WEIGHT', 'SMOKING_STATUS', 
                          'HEART_FAILURE', 'PREVIOUS_ANGINA', 'DIABETES', 'CHRONIC_RENAL_FAILURE', 'PREVIOUS_PCI', 'CEREBROVASCULAR_DISEASE',
                          'KILLIP_CLASS', 'LVEF', 'FIRST_CARDIAC_ARREST', 'PEAK_TROPONIN', 'REPERFUSION_TREATMENT', 'INITIAL_REPERFUSION_TREATMENT',
                          'ADDITIONAL_REPERFUSION_TREAT', 'CARDIOLOGICAL_CARE_DURING_ADMI',
                          'HIGH_RISK_NSTEMI', 'LOOP_DIURETIC', 'WHY_NO_INTERVENTION',
                          'REASON_TREATMENT_NOT_GIVEN', 'CORONARY_ANGIO', 'CORONARY_INTERVENTION', 'LOCAL_ANGIO_DATE', 'ECHOCARDIOGRAPHY',
                          'ACE_I_OR_ARB', 'BETA_BLOCKER', 'HEART_RATE', 'SYSTOLIC_BP', 'CREATININE', 'WHERE_CARDIAC_ARREST',
                          'ECG_DETERMINING_TREATMENT', 'ENZYMES_ELEVATED', 'THIAZIDE_DIURETIC', 'LOOP_DIURETIC',
                          'DISCHARGE_DESTINATION', 'DISCHARGED_ON_ASPIRIN',
                          'DISCHARGED_ON_BETA_BLOCKER', 'DISCHARGED_ON_STATIN', 'DISCHARGED_ON_ACE_I',
                          'DISCHARGED_ON_THIENO_INHIBITOR', 'DISCHARGED_ON_TICAGRELOR', 'CARDIAC_REHAB', 'SMOKING_CESSATION_ADVICE')
                  .withColumnRenamed('1_03_NHS_NUMBER_DEID', 'PERSON_ID')
                  .withColumn('_AGE_AT_ADMISSION', f.col('_AGE_AT_ADMISSION').cast("integer"))
                  .withColumn('HEIGHT', f.col('HEIGHT').cast('double'))
                  .withColumn('WEIGHT', f.col('WEIGHT').cast('double'))
                  .withColumn('mi_event_date', f.to_date(f.col('ARRIVAL_AT_HOSPITAL')))
                  .withColumn('ARRIVAL_AT_HOSPITAL', f.to_timestamp(f.col('ARRIVAL_AT_HOSPITAL')))
                  .withColumn('CALL_FOR_HELP', f.to_timestamp(f.col('CALL_FOR_HELP')))
                  .withColumn('REPERFUSION_TREATMENT', f.to_timestamp(f.col('REPERFUSION_TREATMENT')))
                  .withColumn('LOCAL_ANGIO_DATE', f.to_timestamp(f.col('LOCAL_ANGIO_DATE')))
                 )
display(minap_prepared)


# COMMAND ----------

# merge to cohort by PERSON_ID and MI event date to get the relevant admission record for each patient
minap_prepared_2 = (merge(cohort.select('PERSON_ID', 'mi_event_date'),
                          minap_prepared, 
                          ['PERSON_ID', 'mi_event_date'], 
                          validate='1:m', 
                          keep_results=['both'], 
                          indicator=0))
minap_prepared_2 = minap_prepared_2.sort('PERSON_ID', 'ARRIVAL_AT_HOSPITAL')
display(minap_prepared_2)

# COMMAND ----------

# there are some patients with multiple MINAP records on the same date
# need to summarise these down to 1 record per patient
count_var(minap_prepared_2, 'PERSON_ID')

# COMMAND ----------

# what happens if we drop completely duplicate rows
# there are some completely duplicate records, but this does not account for all cases of multiple records on same date
minap_prepared_3 = minap_prepared_2.distinct()
count_var(minap_prepared_3, 'PERSON_ID')

# COMMAND ----------

# assign admission order field to patient MINAP records

# create window to count total number of records for each individual
win = Window\
  .partitionBy('PERSON_ID', 'mi_event_date')
# create window to order admissions
win_order = Window\
  .partitionBy('PERSON_ID', 'mi_event_date')\
  .orderBy(f.col('ARRIVAL_AT_HOSPITAL').asc_nulls_last())


# add total record and admission order fields
minap_prepared_4 = minap_prepared_3\
  .withColumn('adm_order', f.row_number().over(win_order))\
  .withColumn('recs', f.count('PERSON_ID').over(win))\
  .sort('PERSON_ID', 'adm_order')

# frequency of admission order field
tab(minap_prepared_4, 'adm_order')

# COMMAND ----------

# view patient records where  > 1 record per admission date
display(minap_prepared_4.where(f.col('recs') > 1))

# COMMAND ----------

# take the latest record in each admission record set
# see analysis of MINAP admission record sets in Data Checks folder
# for key variables we will take min/max value as described below:
# ARRIVAL_AT_HOSPITAL - take MIN value - this field will be used to calculate door to balloon time
# CALL_FOR_HELP - take MIN value - this field will be used to calculate door to balloon time
# PEAK_TROPONIN - take the max value
minap_prepared_final = (minap_prepared_4
                        .withColumn('ARRIVAL_AT_HOSPITAL', f.min(f.col('ARRIVAL_AT_HOSPITAL')).over(win_order))\
                        .withColumn('CALL_FOR_HELP', f.min(f.col('CALL_FOR_HELP')).over(win_order))\
                        .withColumn('REPERFUSION_TREATMENT', f.min(f.col('REPERFUSION_TREATMENT')).over(win_order))\
                        .withColumn('LOCAL_ANGIO_DATE', f.min(f.col('LOCAL_ANGIO_DATE')).over(win_order))\
                        .withColumn('PEAK_TROPONIN', f.max(f.col('PEAK_TROPONIN')).over(win_order))\
                        .withColumn('HEART_RATE', f.max(f.col('HEART_RATE')).over(win_order))\
                        .where(f.col('adm_order') == f.col('recs'))\
                        .select('PERSON_ID', 'mi_event_date', 
                          'ARRIVAL_AT_HOSPITAL', 'CALL_FOR_HELP',
                          'ADMISSION_WARD', 
                          'GENDER', '_AGE_AT_ADMISSION', 
                          'HEIGHT', 'WEIGHT', 'SMOKING_STATUS', 
                          'HEART_FAILURE', 'PREVIOUS_ANGINA', 'DIABETES', 'CHRONIC_RENAL_FAILURE', 'PREVIOUS_PCI', 'CEREBROVASCULAR_DISEASE',
                          'KILLIP_CLASS', 'LVEF', 'HIGH_RISK_NSTEMI',  'LOOP_DIURETIC', 'WHY_NO_INTERVENTION',
                          'FIRST_CARDIAC_ARREST', 'PEAK_TROPONIN', 'REPERFUSION_TREATMENT', 'INITIAL_REPERFUSION_TREATMENT', 'ADDITIONAL_REPERFUSION_TREAT', 'REASON_TREATMENT_NOT_GIVEN', 'CARDIOLOGICAL_CARE_DURING_ADMI',
                          'CORONARY_ANGIO', 'CORONARY_INTERVENTION', 'LOCAL_ANGIO_DATE', 'ECHOCARDIOGRAPHY', 'HEART_RATE', 'SYSTOLIC_BP', 'CREATININE', 'WHERE_CARDIAC_ARREST',
                          'ECG_DETERMINING_TREATMENT', 'ENZYMES_ELEVATED', 'THIAZIDE_DIURETIC', 'LOOP_DIURETIC',
                          'DISCHARGE_DESTINATION', 'DISCHARGED_ON_ASPIRIN',
                          'DISCHARGED_ON_BETA_BLOCKER', 'DISCHARGED_ON_STATIN', 'DISCHARGED_ON_ACE_I',
                          'DISCHARGED_ON_THIENO_INHIBITOR', 'DISCHARGED_ON_TICAGRELOR', 'CARDIAC_REHAB', 'SMOKING_CESSATION_ADVICE'))

# COMMAND ----------

# check 1 row and event per Patient
minap_prepared_final.cache()
count_var(minap_prepared_final, 'PERSON_ID')

# COMMAND ----------

# MAGIC %md
# MAGIC # 3 MINAP Covariates

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 BMI at admission

# COMMAND ----------

minap_cov_1 = (minap_prepared_final
             .select('PERSON_ID', 'mi_event_date', 'GENDER', 'WEIGHT', 'HEIGHT')
            )
display(minap_cov_1)
# should we apply a valid range to this field - i.e. set BMIs outwith 16-50 as null?

# COMMAND ----------

# adjust these to CM to be consistent with rest of the data
minap_cov_1 = (minap_cov_1
             .withColumn('HEIGHT_CLEAN', 
                         f.when((f.col('HEIGHT') >= 120) & (f.col('HEIGHT') <= 230), f.col('HEIGHT'))
                         .when((f.col('HEIGHT') >= 1.2) & (f.col('HEIGHT') <= 2.3), f.col('HEIGHT')*100)
                         .when((f.col('HEIGHT') >= 1200) & (f.col('HEIGHT') <= 2300) , f.col('HEIGHT')/10)
                         .otherwise(None))
             .withColumn('WEIGHT_CLEAN', 
                         f.when((f.col('WEIGHT') >= 30) & (f.col('WEIGHT') <= 250), f.col('WEIGHT'))
                         .when((f.col('WEIGHT') >= 300) & (f.col('WEIGHT') <= 2500) , f.col('WEIGHT')/10)
                         .otherwise(None))            
            )


# COMMAND ----------

display(minap_cov_1)

# COMMAND ----------

# calculate bmi
minap_cov_1 = (minap_cov_1
            .withColumn('minap_bmi', f.col('WEIGHT_CLEAN')/pow(f.col('HEIGHT_CLEAN')/100, 2)))

# COMMAND ----------

tmpt = tabstat(minap_cov_1, 'minap_bmi'); print()
tmpt = tabstat(minap_cov_1, 'minap_bmi', byvar = ['GENDER']); print()

# COMMAND ----------

# explore missing values and outliers
#display(minap_cov_1.where(f.col('HEIGHT_CLEAN').isNull()))
display(minap_cov_1.where(f.col('WEIGHT_CLEAN').isNull()))
#display(minap_cov_1.where(f.col('minap_bmi').isNull()))
#display(minap_cov_1.where((f.col('minap_bmi') < 10) | (f.col('minap_bmi') > 60)))

# COMMAND ----------

minap_cov_1 = (minap_cov_1
               .select('PERSON_ID', 'minap_bmi', 'HEIGHT', 'WEIGHT')
               )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Day of Week / Overnight

# COMMAND ----------

# add day of week (1 = Sunday, 7 = Saturday)
# and a binary flag to indicate evening admissions (6pm - 6am)
minap_cov_2 = (minap_prepared_final
                      .select('PERSON_ID', 'mi_event_date', 'ARRIVAL_AT_HOSPITAL')
                      .withColumn('mi_event_time', f.date_format(f.col('ARRIVAL_AT_HOSPITAL'), 'HH:mm:ss'))
                      .withColumn('overnight', f.when((f.col('mi_event_time') > '18:00:00') | (f.col('mi_event_time') < '06:00:00'), 1).otherwise(0))
                      .withColumn('day_of_week', f.dayofweek(f.col('mi_event_date')))
                     )
display(minap_cov_2)

# COMMAND ----------

minap_cov_2 = (minap_cov_2
               .select('PERSON_ID', 'overnight', 'day_of_week'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Other Covariates

# COMMAND ----------

minap_cov_3 = (minap_prepared_final
                      .select('PERSON_ID', 'GENDER','_AGE_AT_ADMISSION', 
                          'SMOKING_STATUS',
                          'HEART_RATE', 'SYSTOLIC_BP', 'KILLIP_CLASS', 'LVEF', 'CREATININE', 'CORONARY_INTERVENTION', 'PEAK_TROPONIN',
                          'CARDIOLOGICAL_CARE_DURING_ADMI',
                          'HIGH_RISK_NSTEMI', 'LOOP_DIURETIC', 'WHY_NO_INTERVENTION',
                          'HEART_FAILURE', 'PREVIOUS_ANGINA', 'DIABETES', 'CHRONIC_RENAL_FAILURE', 'PREVIOUS_PCI', 'CEREBROVASCULAR_DISEASE')
              )
display(minap_cov_3)

# COMMAND ----------

# MAGIC %md
# MAGIC # 4 MINAP MI Characteristics

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Cardiogenic Shock

# COMMAND ----------

# see contents of KILLIP_CLASS field that Cardiogenic Shock var is derived from
tab(minap_prepared_final, 'KILLIP_CLASS')

# COMMAND ----------

minap_char_1 = (minap_prepared_final
               .withColumn('cardiogenic_shock', f.when(f.col('KILLIP_CLASS') == '4. Cardiogenic shock', 'Yes')
                               .when(f.col('KILLIP_CLASS') == '8. Not applicable', 'Not applicable')
                               .when((f.col('KILLIP_CLASS').isNull()) | (f.col('KILLIP_CLASS') == '9. Unknown'), None)
                               .otherwise('No'))
               .select('PERSON_ID', 'KILLIP_CLASS', 'cardiogenic_shock'))
minap_char_1.cache()

# COMMAND ----------

#display(minap_out_2)
tab(minap_char_1, 'KILLIP_CLASS', 'cardiogenic_shock')

# COMMAND ----------

minap_char_1 = (minap_char_1
                .drop('KILLIP_CLASS'))
display(minap_char_1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 ST-segment changes

# COMMAND ----------

minap_char_2 = (minap_prepared_final
              .select('PERSON_ID', 'ECG_DETERMINING_TREATMENT'))

# COMMAND ----------

# MAGIC %md ## 4.3 Cardiac Arrest

# COMMAND ----------

# see contents of WHERE_CARDIAC_ARREST field that Cardiogenic shock var is derived from
tab(minap_prepared_final, 'WHERE_CARDIAC_ARREST')

# COMMAND ----------

minap_char_3 = (minap_prepared_final
               .withColumn('cardiac_arrest', f.when(f.col('WHERE_CARDIAC_ARREST') == '1. No arrest', 'No')
                               .when(f.col('WHERE_CARDIAC_ARREST').isNull(), None)
                               .otherwise('Yes'))
               .select('PERSON_ID', 'WHERE_CARDIAC_ARREST', 'cardiac_arrest'))
minap_char_3.cache()

# COMMAND ----------

tab(minap_char_3, 'WHERE_CARDIAC_ARREST', 'cardiac_arrest')

# COMMAND ----------

minap_char_3 = (minap_char_3
                .drop('WHERE_CARDIAC_ARREST'))
display(minap_char_3)

# COMMAND ----------

# MAGIC %md ## 4.4 Cardiac Enzyme Elevation

# COMMAND ----------

# see contents of WHERE_CARDIAC_ARREST field that Cardiogenic shock var is derived from
tmpt = tab(minap_prepared_final, 'ENZYMES_ELEVATED'); print()

# COMMAND ----------

minap_char_4 = (minap_prepared_final
               .withColumn('cardiac_enzymes_elevated', f.when(f.col('ENZYMES_ELEVATED') == '1. Yes', 'Yes')
                               .when(f.col('ENZYMES_ELEVATED') == '0. No', 'No')
                               .otherwise(None))
               .select('PERSON_ID', 'ENZYMES_ELEVATED', 'cardiac_enzymes_elevated'))
minap_char_4.cache()

# COMMAND ----------

tab(minap_char_4, 'ENZYMES_ELEVATED', 'cardiac_enzymes_elevated')

# COMMAND ----------

# MAGIC %md
# MAGIC # 5 MINAP Outcomes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Call to Balloon Time / Door to Balloon Time

# COMMAND ----------

# call_to_balloon and door_to_balloon both presented in seconds
minap_out_1 = (minap_prepared_final
               .select('PERSON_ID', 'mi_event_date', 'CALL_FOR_HELP', 'ARRIVAL_AT_HOSPITAL', 'REPERFUSION_TREATMENT')
               .withColumn('call_to_balloon', f.col('REPERFUSION_TREATMENT').cast('long') - f.col('CALL_FOR_HELP').cast('long'))
               .withColumn('door_to_balloon', f.col('ARRIVAL_AT_HOSPITAL').cast('long') - f.col('CALL_FOR_HELP').cast('long'))
              )
display(minap_out_1)

# COMMAND ----------

minap_out_1 = (minap_out_1
               .drop('mi_event_date'))
display(minap_out_1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Receipt of Thrombolysis

# COMMAND ----------

minap_out_2 = (minap_prepared_final
               .withColumn('receipt_of_thrombolysis', f.when(f.col('INITIAL_REPERFUSION_TREATMENT') == '1. Thrombolytic treatment', 'Yes')
                               .when((f.col('INITIAL_REPERFUSION_TREATMENT').isNull()) | (f.col('INITIAL_REPERFUSION_TREATMENT') == '9. Unknown'), None)
                               .otherwise('No'))
               .select('PERSON_ID', 'INITIAL_REPERFUSION_TREATMENT', 'receipt_of_thrombolysis', 'ADDITIONAL_REPERFUSION_TREAT'))

# COMMAND ----------

tab(minap_out_2, 'INITIAL_REPERFUSION_TREATMENT', 'receipt_of_thrombolysis', 'ADDITIONAL_REPERFUSION_TREATMENT')

# COMMAND ----------

#minap_out_2 = (minap_out_2
#              .drop('INITIAL_REPERFUSION_TREATMENT'))
# *KF* edit (suppressed above code in order to keep initial reperfusion treatment variable)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Reason for no reperfusion

# COMMAND ----------

tab(minap_prepared_final, 'REASON_TREATMENT_NOT_GIVEN')

# COMMAND ----------

minap_out_3 = (minap_prepared_final
              .select('PERSON_ID', 'REASON_TREATMENT_NOT_GIVEN'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.4 Receipt of echocardiography

# COMMAND ----------

tab(minap_prepared_final, 'ECHOCARDIOGRAPHY')

# COMMAND ----------

minap_out_4 = (minap_prepared_final
              .select('PERSON_ID', 'ECHOCARDIOGRAPHY'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.5 Admission to a Cardiac Ward

# COMMAND ----------

minap_out_5 = (minap_prepared_final
               .withColumn('admission_to_cardiac_ward', f.when(f.col('ADMISSION_WARD').isin (['1. Cardiac care unit', '4. Intensive therapy unit']), 'Yes')
                               .when(f.col('ADMISSION_WARD').isin (['6. Died in A&E']), 'Died_in_A&E')
                               .when((f.col('ADMISSION_WARD').isNull()) | (f.col('ADMISSION_WARD') == '9. Unknown'), None)
                               .otherwise('No'))
               .select('PERSON_ID', 'mi_event_date', 'ADMISSION_WARD', 'admission_to_cardiac_ward'))

# COMMAND ----------

tab(minap_out_5, 'ADMISSION_WARD', 'admission_to_cardiac_ward')

# COMMAND ----------

minap_out_5 = (minap_out_5
              .drop('mi_event_date', 'ADMISSION_WARD'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.6 Angiogram Eligibility and Receipt

# COMMAND ----------

minap_out_6 = (minap_prepared_final
               .withColumn('angiogram_eligibility', f.when(f.col('CORONARY_ANGIO').isin (['6. Not applicable']), 'No')
                               .when((f.col('CORONARY_ANGIO').isNull()) | (f.col('CORONARY_ANGIO') == '9. Unknown'), None)
                               .otherwise('Yes'))
               .withColumn('angiogram_receipt', f.when(f.col('CORONARY_ANGIO').isin (['6. Not applicable']), 'Not applicable')
                               .when(f.col('CORONARY_ANGIO').isin (['7. Patient refused']), 'Patient refused')
                               .when(f.col('CORONARY_ANGIO').isin (['5. Planned after discharge', '8. Not performed']), 'No')
                               .when((f.col('CORONARY_ANGIO').isNull()) | (f.col('CORONARY_ANGIO') == '9. Unknown'), None)
                               .otherwise('Yes'))
               .select('PERSON_ID', 'mi_event_date', 'CORONARY_ANGIO', 'angiogram_eligibility', 'angiogram_receipt'))

# COMMAND ----------

tab(minap_out_6, 'CORONARY_ANGIO', 'angiogram_eligibility')

# COMMAND ----------

tab(minap_out_6, 'CORONARY_ANGIO', 'angiogram_receipt')

# COMMAND ----------

# *KF* 'CORONARY_ANGIO' was dropped, but I've excluded it from the list of variables to drop
minap_out_6 = (minap_out_6
               .drop('mi_event_date'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.7 Angiogram within 72 hours

# COMMAND ----------

# calculate time to angiogram (from arrival) and create flag for angio within 72 hours
# some LOCAL_ANGIO_DATE values have a date but a default time of 00:00:00 sometimes resulting in a -ve value - set angio_within_72h flag to "unknown" in these cases
minap_out_7 = (minap_prepared_final
               .select('PERSON_ID', 'mi_event_date', 'ARRIVAL_AT_HOSPITAL', 'LOCAL_ANGIO_DATE')
               .withColumn('arrival_to_angio', f.col('LOCAL_ANGIO_DATE').cast('long') - f.col('ARRIVAL_AT_HOSPITAL').cast('long'))
               .withColumn('angio_within_72h', f.when(f.col('arrival_to_angio') > 259200, 'No')
                                                      .when(f.col('arrival_to_angio') < 0, 'Unknown')
                                                      .when(f.col('arrival_to_angio') < 259200, 'Yes')
                                                      .otherwise(None))
)
display(minap_out_7)

# COMMAND ----------

minap_out_7 =(minap_out_7
             .drop('mi_event_date', 'ARRIVAL_AT_HOSPITAL'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##5.8 Secondary Prevention Medication

# COMMAND ----------

minap_out_8 = (minap_prepared_final
               .select('PERSON_ID', 'mi_event_date', 'DISCHARGE_DESTINATION', 'DISCHARGED_ON_ASPIRIN',
                       'DISCHARGED_ON_BETA_BLOCKER', 'DISCHARGED_ON_STATIN', 'DISCHARGED_ON_ACE_I',
                       'DISCHARGED_ON_THIENO_INHIBITOR', 'DISCHARGED_ON_TICAGRELOR'))

# COMMAND ----------

tab(minap_out_8, 'DISCHARGED_ON_ASPIRIN')

# COMMAND ----------

# define secondary prevention variables
minap_out_8 = (minap_out_8
               .withColumn('prev_eligibility', f.when(f.col('DISCHARGE_DESTINATION').isin(['1. Home']), f.lit(1))
                           .when((f.col('DISCHARGE_DESTINATION').isNull()) | (f.col('DISCHARGE_DESTINATION')== '9. Unknown'), None)
                           .otherwise(f.lit(0)))
               .withColumn('prev_aspirin', f.when(f.col('DISCHARGED_ON_ASPIRIN').isin(['0. No']), f.lit(0))
                           .when((f.col('DISCHARGED_ON_ASPIRIN').isNull()) | (f.col('DISCHARGED_ON_ASPIRIN')== '9. Unknown'), None)
                           .otherwise(f.lit(1)))
               .withColumn('prev_bb', f.when(f.col('DISCHARGED_ON_BETA_BLOCKER').isin(['0. No']), f.lit(0))
                           .when((f.col('DISCHARGED_ON_BETA_BLOCKER').isNull()) | (f.col('DISCHARGED_ON_BETA_BLOCKER')== '9. Unknown'), None)
                           .otherwise(f.lit(1)))
               .withColumn('prev_statin', f.when(f.col('DISCHARGED_ON_STATIN').isin(['0. No']), f.lit(0))
                           .when((f.col('DISCHARGED_ON_STATIN').isNull()) | (f.col('DISCHARGED_ON_STATIN')== '9. Unknown'), None)
                           .otherwise(f.lit(1)))
               .withColumn('prev_ace', f.when(f.col('DISCHARGED_ON_ACE_I').isin(['0. No']), f.lit(0))
                           .when((f.col('DISCHARGED_ON_ACE_I').isNull()) | (f.col('DISCHARGED_ON_ACE_I')== '9. Unknown'), None)
                           .otherwise(f.lit(1)))
               )

# COMMAND ----------

# checks
tab(minap_out_8, 'DISCHARGE_DESTINATION', 'prev_eligibility')
#tab(minap_out_8, 'DISCHARGED_ON_ASPIRIN', 'prev_aspirin')
#tab(minap_out_8, 'DISCHARGED_ON_BETA_BLOCKER', 'prev_bb')
#tab(minap_out_8, 'DISCHARGED_ON_STATIN', 'prev_statin')
#tab(minap_out_8, 'DISCHARGED_ON_ACE_I', 'prev_ace')

# COMMAND ----------

minap_out_8 = (minap_out_8
               .withColumn('prev_thieno_tica', 
                           f.when((f.col('DISCHARGED_ON_THIENO_INHIBITOR').isin(['0. No'])) & (f.col('DISCHARGED_ON_TICAGRELOR').isin(['0. No'])), f.lit(0))
                           .when(f.col('DISCHARGED_ON_THIENO_INHIBITOR').isin(['1. Yes', '2. Contraindicated', '3. Patient declined treatment', '4. Not applicable', '8. Not indicated']), f.lit(1))
                           .when(f.col('DISCHARGED_ON_TICAGRELOR').isin(['1. Yes', '2. Contraindicated', '3. Patient declined treatment', '4. Not applicable', '8. Not indicated']), f.lit(1))
                           .otherwise(None)
                           )
)

# COMMAND ----------

# check
display(minap_out_8.select('PERSON_ID', 'DISCHARGED_ON_THIENO_INHIBITOR', 'DISCHARGED_ON_TICAGRELOR', 'prev_thieno_tica'))
#display(minap_out_8.select('PERSON_ID', 'DISCHARGED_ON_THIENO_INHIBITOR', 'DISCHARGED_ON_TICAGRELOR', 'prev_thieno_tica').where((f.col('DISCHARGED_ON_TICAGRELOR') == 'No') & (f.col('DISCHARGED_ON_THIENO_INHIBITOR').isNull())))

# COMMAND ----------

# all
minap_out_8 = (minap_out_8.
               withColumn('prev_all',
                          f.when((f.col('prev_eligibility').isNull()) | (f.col('prev_aspirin').isNull()) | (f.col('prev_bb').isNull()) | (f.col('prev_statin').isNull()) | (f.col('prev_ace').isNull()) | (f.col('prev_thieno_tica').isNull()), None)
                          .otherwise(f.least('prev_eligibility','prev_aspirin','prev_bb','prev_statin','prev_ace','prev_thieno_tica'))))

# COMMAND ----------

# *KF*: Added in 'DISCHARGE_DESTINATION'
minap_out_8 = (minap_out_8.
               select('PERSON_ID', 'DISCHARGE_DESTINATION', 'prev_eligibility','prev_aspirin','prev_bb','prev_statin','prev_ace','prev_thieno_tica', 'prev_all',
                      'DISCHARGED_ON_ASPIRIN', 'DISCHARGED_ON_BETA_BLOCKER', 'DISCHARGED_ON_STATIN', 'DISCHARGED_ON_ACE_I', 'DISCHARGED_ON_THIENO_INHIBITOR', 'DISCHARGED_ON_TICAGRELOR')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.9 Referral for Cardiac Rehab

# COMMAND ----------

minap_out_9 = (minap_prepared_final
              .select('PERSON_ID', 'CARDIAC_REHAB'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.10 Smoking Cessation Advice

# COMMAND ----------

minap_out_10 = (minap_prepared_final
              .select('PERSON_ID', 'SMOKING_CESSATION_ADVICE'))

# COMMAND ----------

# MAGIC %md # F Save

# COMMAND ----------

# merge minap_cov (covariates), minap_char (MI characteristics), and minap_out (outcomes) tables
# *KF* Added minap_char_3 and minap_char_4 below

minap_prepared_final_out = (
  minap_prepared_final.select('PERSON_ID')
  .join(minap_cov_1, on=['PERSON_ID'], how='left')
  .join(minap_cov_2, on=['PERSON_ID'], how='left')
  .join(minap_cov_3, on=['PERSON_ID'], how='left')
  .join(minap_char_1, on=['PERSON_ID'], how='left')
  .join(minap_char_2, on=['PERSON_ID'], how='left')
  .join(minap_char_3, on=['PERSON_ID'], how='left')
  .join(minap_char_4, on=['PERSON_ID'], how='left')
  .join(minap_out_1, on=['PERSON_ID'], how='left')
  .join(minap_out_2, on=['PERSON_ID'], how='left')
  .join(minap_out_3, on=['PERSON_ID'], how='left')
  .join(minap_out_4, on=['PERSON_ID'], how='left')
  .join(minap_out_5, on=['PERSON_ID'], how='left')
  .join(minap_out_6, on=['PERSON_ID'], how='left')
  .join(minap_out_7, on=['PERSON_ID'], how='left')
  .join(minap_out_8, on=['PERSON_ID'], how='left')
  .join(minap_out_9, on=['PERSON_ID'], how='left')
  .join(minap_out_10, on=['PERSON_ID'], how='left')
)

# check
count_var(minap_prepared_final_out, 'PERSON_ID'); print()
#print(len(tmp3.columns)); print()
#print(pd.DataFrame({f'_cols': tmp3.columns}).to_string()); print()

# COMMAND ----------

display(minap_prepared_final_out)

# COMMAND ----------

# save name
outName = f'{proj}_out_minap'.lower()

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
spark.sql(f'drop table dsa_391419_j3w9t_collab.ccu046_01_out_minap')
minap_prepared_final_out.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
minap_prepared_final_out = spark.table(f'{dbc}.{outName}')

# COMMAND ----------

display(minap_prepared_final_out)

# COMMAND ----------

count_var(minap_prepared_final_out, 'PERSON_ID')