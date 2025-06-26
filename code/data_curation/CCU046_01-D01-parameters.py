# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # CCU046_01-D01-parameters
# MAGIC
# MAGIC **Description** The parameters notebook defines a set of parameters, which is loaded in each notebook in the data curation pipeline, so that helper functions and parameters are consistently available.
# MAGIC
# MAGIC **Authors** Tom Bolton, John Nolan
# MAGIC
# MAGIC **Reviewers** ⚠ UNREVIEWED
# MAGIC
# MAGIC **Acknowledgements** Based on CCU003_05
# MAGIC
# MAGIC **Notes**

# COMMAND ----------

# MAGIC %run "/Repos/jn453@medschl.cam.ac.uk/shds/common/functions"

# COMMAND ----------

import pyspark.sql.functions as f
import pandas as pd
import re

# COMMAND ----------

# -----------------------------------------------------------------------------
# Project
# -----------------------------------------------------------------------------
proj = 'ccu046_01'

# -----------------------------------------------------------------------------
# Databases
# -----------------------------------------------------------------------------
db = 'dars_nic_391419_j3w9t'
dbc_old = f'{db}_collab'
dbc = 'dsa_391419_j3w9t_collab'

# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------
# data frame of datasets
tmp_archived_on = '2024-10-01'
data = [
    ['deaths',  dbc_old, f'deaths_dars_nic_391419_j3w9t_archive',            tmp_archived_on, 'DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'REG_DATE_OF_DEATH']
  , ['gdppr',   dbc_old, f'gdppr_dars_nic_391419_j3w9t_archive',             tmp_archived_on, 'NHS_NUMBER_DEID',                'DATE']
  , ['gdppr_1st',   dbc_old, f'gdppr_dars_nic_391419_j3w9t_archive',             '2020-11-23', 'NHS_NUMBER_DEID',                'DATE']
  , ['hes_apc', dbc_old, f'hes_apc_all_years_archive',      tmp_archived_on, 'PERSON_ID_DEID',                 'EPISTART'] 
  , ['hes_op',  dbc_old, f'hes_op_all_years_archive',       tmp_archived_on, 'PERSON_ID_DEID',                 'APPTDATE'] 
  , ['hes_ae',  dbc_old, f'hes_ae_all_years_archive',       tmp_archived_on, 'PERSON_ID_DEID',                 'ARRIVALDATE'] 
  , ['minap',  dbc_old, f'nicor_minap_{db}_archive',       '2023-12-27', '1_03_NHS_NUMBER_DEID',                 'ARRIVAL_AT_HOSPITAL']   
#  , ['vacc',    dbc, f'vaccine_status_{db}_archive',    tmp_archived_on, 'PERSON_ID_DEID',                 'DATE_AND_TIME']
  , ['sgss',    dbc_old, f'sgss_dars_nic_391419_j3w9t_archive',    '2024-09-02', 'PERSON_ID_DEID',                 'Specimen_Date']
  , ['hes_cc',  dbc_old, f'hes_cc_all_years_archive',       tmp_archived_on, 'PERSON_ID_DEID',                 'CCSTARTDATE']   
  , ['sus',     dbc_old, f'sus_dars_nic_391419_j3w9t_archive',               '2022-09-30',    'NHS_NUMBER_DEID',                'EPISODE_START_DATE']     
  , ['chess',   dbc_old, f'chess_dars_nic_391419_j3w9t_archive',             '2024-09-02', 'PERSON_ID_DEID',                 'InfectionSwabDate']       
#  , ['pmeds',   dbc, f'primary_care_meds_{db}_archive', tmp_archived_on, 'Person_ID_DEID',                 'ProcessingPeriodDate']         
]
parameters_df_datasets = pd.DataFrame(data, columns = ['dataset', 'database', 'table', 'archived_on', 'idVar', 'dateVar'])
print('parameters_df_datasets:\n', parameters_df_datasets.to_string())
  
# note: the below is largely listed in order of appearance within the pipeline:  

# reference tables
path_ref_bhf_phenotypes  = 'bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127'
# path_ref_map_ctv3_snomed = 'dss_corporate.read_codes_map_ctv3_to_snomed'
path_ref_geog            = 'dss_corporate.ons_chd_geo_listings'
path_ref_imd             = 'dss_corporate.english_indices_of_dep_v02'
# path_ref_ethnic_hes      = 'dss_corporate.hesf_ethnicity'
# path_ref_ethnic_gdppr    = 'dss_corporate.gdppr_ethnicity'
path_ref_gp_refset       = 'dss_corporate.gpdata_snomed_refset_full'
path_ref_gdppr_refset    = 'dss_corporate.gdppr_cluster_refset'
path_ref_icd10           = 'dss_corporate.icd10_group_chapter_v01'
path_ref_opcs4           = 'dss_corporate.opcs_codes_v02'

# curated tables
path_cur_hes_apc_long      = f'{dbc}.{proj}_cur_hes_apc_all_years_archive_long'
path_cur_hes_apc_oper_long = f'{dbc}.{proj}_cur_hes_apc_all_years_archive_oper_long'
path_cur_hes_op_long      = f'{dbc}.{proj}_cur_hes_op_all_years_archive_long'
path_cur_deaths_long       = f'{dbc}.{proj}_cur_deaths_{db}_archive_long'
path_cur_deaths_sing       = f'{dbc}.{proj}_cur_deaths_{db}_archive_sing'
path_cur_lsoa_region       = f'{dbc}.{proj}_cur_lsoa_region_lookup'
path_cur_lsoa_imd          = f'{dbc}.{proj}_cur_lsoa_imd_lookup'
# path_cur_vacc_first        = f'{dbc}.{proj}_cur_vacc_first'
path_cur_covid             = f'{dbc}.{proj}_cur_covid'

# # temporary tables
path_tmp_skinny_unassembled             = f'{dbc}.{proj}_tmp_kpc_harmonised'
path_tmp_skinny_assembled               = f'{dbc}.{proj}_tmp_kpc_selected'
path_tmp_skinny                         = f'{dbc}.{proj}_tmp_skinny'

path_tmp_quality_assurance_hx_1st_wide  = f'{dbc}.{proj}_tmp_quality_assurance_hx_1st_wide'
path_tmp_quality_assurance_hx_1st       = f'{dbc}.{proj}_tmp_quality_assurance_hx_1st'
path_tmp_quality_assurance_qax          = f'{dbc}.{proj}_tmp_quality_assurance_qax'
path_tmp_quality_assurance              = f'{dbc}.{proj}_tmp_quality_assurance'

path_tmp_inc_exc_cohort                 = f'{dbc}.{proj}_tmp_inc_exc_cohort'
path_tmp_inc_exc_flow                   = f'{dbc}.{proj}_tmp_inc_exc_flow'

path_tmp_hx_af_hyp_cohort               = f'{dbc}.{proj}_tmp_hx_af_hyp_cohort'
path_tmp_hx_af_hyp_gdppr                = f'{dbc}.{proj}_tmp_hx_af_hyp_gdppr'
path_tmp_hx_af_hyp_hes_apc              = f'{dbc}.{proj}_tmp_hx_af_hyp_hes_apc'
path_tmp_hx_af_hyp                      = f'{dbc}.{proj}_tmp_hx_af_hyp'

path_tmp_inc_exc_2_cohort                 = f'{dbc}.{proj}_tmp_inc_exc_2_cohort'
path_tmp_inc_exc_2_flow                   = f'{dbc}.{proj}_tmp_inc_exc_2_flow'

# path_tmp_covariates_hes_apc             = f'{dbc}.{proj}_tmp_covariates_hes_apc'
# path_tmp_covariates_pmeds               = f'{dbc}.{proj}_tmp_covariates_pmeds'
# path_tmp_covariates_lsoa                = f'{dbc}.{proj}_tmp_covariates_lsoa'
# path_tmp_covariates_lsoa_2              = f'{dbc}.{proj}_tmp_covariates_lsoa_2'
# path_tmp_covariates_lsoa_3              = f'{dbc}.{proj}_tmp_covariates_lsoa_3'
# path_tmp_covariates_n_consultations     = f'{dbc}.{proj}_tmp_covariates_n_consultations'
# path_tmp_covariates_unique_bnf_chapters = f'{dbc}.{proj}_tmp_covariates_unique_bnf_chapters'
# path_tmp_covariates_hx_out_1st_wide     = f'{dbc}.{proj}_tmp_covariates_hx_out_1st_wide'
# path_tmp_covariates_hx_com_1st_wide     = f'{dbc}.{proj}_tmp_covariates_hx_com_1st_wide'

# out tables
path_out_codelist_quality_assurance      = f'{dbc}.{proj}_out_codelist_quality_assurance'
path_out_codelist_exposure               = f'{dbc}.{proj}_out_codelist_exposure'
path_out_codelist_covid               = f'{dbc}.{proj}_out_codelist_covid'
path_out_codelist_comorbidity            = f'{dbc}.{proj}_out_codelist_comorbidity'
path_out_codelist_cohort            = f'{dbc}.{proj}_out_codelist_cohort'
path_codelist_sct_schizophrenia          = f'{dbc}.ccu046_03_codelists_4jul2024_snomed_schizophrenia'
path_codelist_sct_bipolar              = f'{dbc}.ccu046_03_codelists_4jul2024_snomed_bipolar_disorder'
path_codelist_sct_depression             = f'{dbc}.ccu046_03_codelists_4jul2024_snomed_depression'
path_codelist_sct_myocardial_infarction         = f'{dbc}.ccu046_03_codelists_23jul2024_snomed_myocardial_infarction'
path_out_cohort                     = f'{dbc}.{proj}_out_cohort'

# path_out_covariates                 = f'{dbc}.{proj}_out_covariates'
# path_out_exposures                  = f'{dbc}.{proj}_out_exposures'
# path_out_outcomes                   = f'{dbc}.{proj}_out_outcomes'

# -----------------------------------------------------------------------------
# Dates
# -----------------------------------------------------------------------------
study_start_date = '2019-11-01' 
study_end_date   = '2023-03-31'

# -----------------------------------------------------------------------------
# out tables 
# -----------------------------------------------------------------------------
tmp_out_date = '99999999'
data = [
#    ['codelist',  tmp_out_date]
    ['cohort',  tmp_out_date]
#  , ['skinny',    tmp_out_date]  
#  , ['covariates', tmp_out_date]  
#  , ['exposures',  tmp_out_date]      
#  , ['outcomes',   tmp_out_date]  
#  , ['analysis',   tmp_out_date]    
]
df_out = pd.DataFrame(data, columns = ['dataset', 'out_date'])
# df_out

# COMMAND ----------

# function to extract the batch corresponding to the pre-defined archived_on date from the archive for the specified dataset
from pyspark.sql import DataFrame
def extract_batch_from_archive(_df_datasets: DataFrame, _dataset: str):
  
  # get row from df_archive_tables corresponding to the specified dataset
  _row = _df_datasets[_df_datasets['dataset'] == _dataset]
  
  # check one row only
  assert _row.shape[0] != 0, f"dataset = {_dataset} not found in _df_datasets (datasets = {_df_datasets['dataset'].tolist()})"
  assert _row.shape[0] == 1, f"dataset = {_dataset} has >1 row in _df_datasets"
  
  # create path and extract archived on
  _row = _row.iloc[0]
  _path = _row['database'] + '.' + _row['table']  
  _archived_on = _row['archived_on']  
  print(_path + ' (archived_on = ' + _archived_on + ')')
  
  # check path exists # commented out for runtime
#   _tmp_exists = spark.sql(f"SHOW TABLES FROM {_row['database']}")\
#     .where(f.col('tableName') == _row['table'])\
#     .count()
#   assert _tmp_exists == 1, f"path = {_path} not found"

  # extract batch
  _tmp = spark.table(_path)\
    .where(f.col('archived_on') == _archived_on)  
  
  # check number of records returned
  _tmp_records = _tmp.count()
  print(f'  {_tmp_records:,} records')
  assert _tmp_records > 0, f"number of records == 0"

  # return dataframe
  return _tmp

# COMMAND ----------

# DBTITLE 1,Save Table
def save_table(df, out_name:str, save_previous=True, data_base:str=f'dsa_391419_j3w9t_collab'):
  
  # assert that df is a dataframe
  assert isinstance(df, f.DataFrame), 'df must be of type dataframe' #isinstance(df, pd.DataFrame) | 
  # if a pandas df then convert to spark
  #if(isinstance(df, pd.DataFrame)):
    #df = (spark.createDataFrame(df))
  
  # save name check
  if(any(char.isupper() for char in out_name)): 
    print(f'Warning: {out_name} converted to lowercase for saving')
    out_name = out_name.lower()
    print('out_name: ' + out_name)
    print('')
  
  # df name
  df_name = [x for x in globals() if globals()[x] is df][0]
  
  # ------------------------------------------------------------------------------------------------
  # save previous version for comparison purposes
  if(save_previous):
    tmpt = (
      spark.sql(f"""SHOW TABLES FROM {data_base}""")
      .select('tableName')
      .where(f.col('tableName') == out_name)
      .collect()
    )
    if(len(tmpt)>0):
      # save with production date appended
      _datetimenow = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
      out_name_pre = f'{out_name}_pre{_datetimenow}'.lower()
      print(f'saving (previous version):')
      print(f'  {out_name}')
      print(f'  as')
      print(f'  {out_name_pre}')
      spark.table(f'{data_base}.{out_name}').write.mode('overwrite').saveAsTable(f'{data_base}.{out_name_pre}')
      #spark.sql(f'ALTER TABLE {data_base}.{out_name_pre} OWNER TO {data_base}')
      print('saved')
      print('') 
    else:
      print(f'Warning: no previous version of {out_name} found')
      print('')
  # ------------------------------------------------------------------------------------------------  
  
  # save new version
  print(f'saving:')
  print(f'  {df_name}')
  print(f'  as')
  print(f'  {out_name}')
  df.write.mode('overwrite').option("overwriteSchema", "True").saveAsTable(f'{data_base}.{out_name}')
  #spark.sql(f'ALTER TABLE {data_base}.{out_name} OWNER TO {data_base}')
  print('saved')

# COMMAND ----------

print(f'Project:')
print("  {0:<22}".format('proj') + " = " + f'{proj}') 
print(f'')
print(f'Databases:')
print("  {0:<22}".format('db') + " = " + f'{db}') 
print("  {0:<22}".format('dbc') + " = " + f'{dbc}') 
print(f'')
print(f'Paths:')
# with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
#  print(df_paths_raw_data[['dataset', 'database', 'table']])
# print(f'')
# print(f'  df_archive')
# print(df_archive[['dataset', 'database', 'table', 'productionDate']].to_string())
print(f'')
tmp = vars().copy()
for var in list(tmp.keys()):
  if(re.match('^path_.*$', var)):
    print("  {0:<22}".format(var) + " = " + tmp[var])    
print(f'')
print(f'Dates:')    
print("  {0:<22}".format('study_start_date') + " = " + f'{study_start_date}') 
print("  {0:<22}".format('study_end_date') + " = " + f'{study_end_date}') 
print(f'')
#print(f'composite_events:')   
#for i, c in enumerate(composite_events):
  #print('  ', i, c, '=', composite_events[c])
#print(f'')
# print(f'Out dates:')
# with pd.option_context('display.max_rows', None, 'display.max_columns', None): 
#  print(df_paths_raw_data[['dataset', 'database', 'table']])
# print(f'')
# print(f'  df_out')
#print(df_out[['dataset', 'out_date']].to_string())
#print(f'')