# Databricks notebook source
# MAGIC %md # CCU046_01-D02c-codelist_comorbidity
# MAGIC
# MAGIC **Description** This notebook creates the codelist for comorbidities.
# MAGIC
# MAGIC **Author(s)** John Nolan, Tom Bolton
# MAGIC
# MAGIC **Acknowledgements** Based on previous work by Tom Bolton (John Nolan, Elena Raffetti) for CCU018_01 and the earlier CCU002 sub-projects.
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

# connect to BHF phenotypes database table
bhf_phenotypes  = spark.table(path_ref_bhf_phenotypes)

# gdppr refset
gdppr_refset = spark.table(path_ref_gdppr_refset)

# icd lookup table
icd = spark.table(path_ref_icd10)

# opcs lookup table
opcs = spark.table(path_ref_opcs4)

# COMMAND ----------

display(icd)

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

# create opcs-4 lookup table
w = Window.partitionBy('ALT_OPCS_CODE').orderBy(f.col('OPCS_VERSION').desc())
opcs_lkp = opcs\
  .withColumn('rownum', f.row_number().over(w))\
  .where(f.col('rownum') == 1)\
  .select('ALT_OPCS_CODE', 'OPCS_CODE_DESC_FULL')\
  .withColumnRenamed('ALT_OPCS_CODE', 'code')\
  .withColumnRenamed('OPCS_CODE_DESC_FULL', 'term')\
  .orderBy('code')
display(opcs_lkp)

# COMMAND ----------

# MAGIC %md 
# MAGIC # 2 Codelist

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Angina / HF / Diabetes

# COMMAND ----------

# angina / HF / Diabetes codelist
codelist_comorbidity1 = """
"name","code","terminology","term"
"hf","10335000","SNOMED","Chronic right-sided heart failure (disorder)"
"hf","10633002","SNOMED","Acute congestive heart failure"
"hf","128404006","SNOMED","Right heart failure"
"hf","134401001","SNOMED","Left ventricular systolic dysfunction"
"hf","134440006","SNOMED","Referral to heart failure clinic"
"hf","194767001","SNOMED","Benign hypertensive heart disease with congestive cardiac failure"
"hf","194779001","SNOMED","Hypertensive heart and renal disease with (congestive) heart failure"
"hf","194781004","SNOMED","Hypertensive heart and renal disease with both (congestive) heart failure and renal failure"
"hf","195111005","SNOMED","Decompensated cardiac failure"
"hf","195112003","SNOMED","Compensated cardiac failure"
"hf","195114002","SNOMED","Acute left ventricular failure"
"hf","206586007","SNOMED","Congenital cardiac failure"
"hf","233924009","SNOMED","Heart failure as a complication of care (disorder)"
"hf","275514001","SNOMED","Impaired left ventricular function"
"hf","314206003","SNOMED","Refractory heart failure (disorder)"
"hf","367363000","SNOMED","Right ventricular failure"
"hf","407596008","SNOMED","Echocardiogram shows left ventricular systolic dysfunction (finding)"
"hf","420300004","SNOMED","New York Heart Association Classification - Class I (finding)"
"hf","420913000","SNOMED","New York Heart Association Classification - Class III (finding)"
"hf","421704003","SNOMED","New York Heart Association Classification - Class II (finding)"
"hf","422293003","SNOMED","New York Heart Association Classification - Class IV (finding)"
"hf","42343007","SNOMED","Congestive heart failure"
"hf","426263006","SNOMED","Congestive heart failure due to left ventricular systolic dysfunction (disorder)"
"hf","426611007","SNOMED","Congestive heart failure due to valvular disease (disorder)"
"hf","430396006","SNOMED","Chronic systolic dysfunction of left ventricle (disorder)"
"hf","43736008","SNOMED","Rheumatic left ventricular failure"
"hf","446221000","SNOMED","Heart failure with normal ejection fraction (disorder)"
"hf","48447003","SNOMED","Chronic heart failure (disorder)"
"hf","56675007","SNOMED","Acute heart failure"
"hf","698592004","SNOMED","Asymptomatic left ventricular systolic dysfunction (disorder)"
"hf","703272007","SNOMED","Heart failure with reduced ejection fraction (disorder)"
"hf","717491000000102","SNOMED","Excepted from heart failure quality indicators - informed dissent (finding)"
"hf","71892000","SNOMED","Cardiac asthma"
"hf","760361000000100","SNOMED","Fast track heart failure referral for transthoracic two dimensional echocardiogram"
"hf","79955004","SNOMED","Chronic cor pulmonale"
"hf","83105008","SNOMED","Malignant hypertensive heart disease with congestive heart failure"
"hf","84114007","SNOMED","Heart failure"
"hf","85232009","SNOMED","Left heart failure"
"hf","87837008","SNOMED","Chronic pulmonary heart disease"
"hf","88805009","SNOMED","Chronic congestive heart failure"
"hf","92506005","SNOMED","Biventricular congestive heart failure"
"hf","I11.0","ICD10","Hypertensive heart disease with (congestive) heart failure"
"hf","I13.0","ICD10","Hypertensive heart and renal disease with (congestive) heart failure"
"hf","I13.2","ICD10","Hypertensive heart and renal disease with both (congestive) heart failure and renal failure"
"hf","I50","ICD10","Heart failure"
"angina","10971000087107","SNOMED","Myocardial ischemia during surgery (disorder)"
"angina","15960061000119102","SNOMED","Unstable angina co-occurrent and due to coronary arteriosclerosis (disorder)"
"angina","15960141000119102","SNOMED","Angina co-occurrent and due to coronary arteriosclerosis (disorder)"
"angina","15960381000119109","SNOMED","Angina co-occurrent and due to arteriosclerosis of coronary artery bypass graft (disorder)"
"angina","15960581000119102","SNOMED","Angina co-occurrent and due to arteriosclerosis of autologous vein coronary artery bypass graft (disorder)"
"angina","15960661000119107","SNOMED","Unstable angina co-occurrent and due to arteriosclerosis of coronary artery bypass graft (disorder)"
"angina","19057007","SNOMED","Status anginosus (disorder)"
"angina","194823009","SNOMED","Acute coronary insufficiency (disorder)"
"angina","194828000","SNOMED","Angina (disorder)"
"angina","21470009","SNOMED","Syncope anginosa (disorder)"
"angina","225566008","SNOMED","Ischemic chest pain (finding)"
"angina","233819005","SNOMED","Stable angina (disorder)"
"angina","233821000","SNOMED","New onset angina (disorder)"
"angina","233823002","SNOMED","Silent myocardial ischemia (disorder)"
"angina","25106000","SNOMED","Impending infarction (disorder)"
"angina","300995000","SNOMED","Exercise-induced angina (disorder)"
"angina","314116003","SNOMED","Post infarct angina (disorder)"
"angina","315025001","SNOMED","Refractory angina (disorder)"
"angina","35928006","SNOMED","Nocturnal angina (disorder)"
"angina","371806006","SNOMED","Progressive angina (disorder)"
"angina","371807002","SNOMED","Atypical angina (disorder)"
"angina","371808007","SNOMED","Recurrent angina status post percutaneous transluminal coronary angioplasty (disorder)"
"angina","371809004","SNOMED","Recurrent angina status post coronary stent placement (disorder)"
"angina","371810009","SNOMED","Recurrent angina status post coronary artery bypass graft (disorder)"
"angina","371811008","SNOMED","Recurrent angina status post rotational atherectomy (disorder)"
"angina","371812001","SNOMED","Recurrent angina status post directional coronary atherectomy (disorder)"
"angina","394659003","SNOMED","Acute coronary syndrome (disorder)"
"angina","41334000","SNOMED","Angina, class II (disorder)"
"angina","413439005","SNOMED","Acute ischemic heart disease (disorder)"
"angina","413444003","SNOMED","Acute myocardial ischemia (disorder)"
"angina","413838009","SNOMED","Chronic ischemic heart disease (disorder)"
"angina","413844008","SNOMED","Chronic myocardial ischemia (disorder)"
"angina","414545008","SNOMED","Ischemic heart disease (disorder)"
"angina","414795007","SNOMED","Myocardial ischemia (disorder)"
"angina","429559004","SNOMED","Typical angina (disorder)"
"angina","4557003","SNOMED","Preinfarction syndrome (disorder)"
"angina","46109009","SNOMED","Subendocardial ischemia (disorder)"
"angina","59021001","SNOMED","Angina decubitus (disorder)"
"angina","61490001","SNOMED","Angina, class I (disorder)"
"angina","697976003","SNOMED","Microvascular ischemia of myocardium (disorder)"
"angina","703214003","SNOMED","Silent coronary vasospastic disease (disorder)"
"angina","712866001","SNOMED","Resting ischemia co-occurrent and due to ischemic heart disease (disorder)"
"angina","713405002","SNOMED","Subacute ischemic heart disease (disorder)"
"angina","791000119109","SNOMED","Angina associated with type II diabetes mellitus (disorder)"
"angina","85284003","SNOMED","Angina, class III (disorder)"
"angina","87343002","SNOMED","Prinzmetal angina (disorder)"
"angina","89323001","SNOMED","Angina, class IV (disorder)"
"angina","I20","ICD10","Angina"
"diabetes","E10","ICD10","Insulin-dependent diabetes mellitus"
"diabetes","E11","ICD10","Non-insulin-dependent diabetes mellitus"
"diabetes","E12","ICD10","Malnutrition-related diabetes mellitus"
"diabetes","E13","ICD10","Other specified diabetes mellitus"
"diabetes","E14","ICD10","Unspecified diabetes mellitus"
"diabetes","G590","ICD10","Diabetic mononeuropathy"
"diabetes","G632","ICD10","Diabetic polyneuropathy"
"diabetes","H280","ICD10","Diabetic cataract"
"diabetes","H360","ICD10","Diabetic retinopathy"
"diabetes","M142","ICD10","Diabetic arthropathy"
"diabetes","N083","ICD10","Glomerular disorders in diabetes mellitus"
"diabetes","O240","ICD10","Diabetes mellitus in pregnancy: Pre-existing diabetes mellitus, insulin-dependent"
"diabetes","O241","ICD10","Diabetes mellitus in pregnancy: Pre-existing diabetes mellitus, non-insulin-dependent"
"diabetes","O242","ICD10","Diabetes mellitus in pregnancy: Pre-existing malnutrition-related diabetes mellitus"
"diabetes","O243","ICD10","Diabetes mellitus in pregnancy: Pre-existing diabetes mellitus, unspecified"
"diabetes","104941000119109","SNOMED","Retinal ischemia due to type 1 diabetes mellitus"
"diabetes","106281000119103","SNOMED","Pre-existing diabetes mellitus in mother complicating childbirth (disorder)"
"diabetes","10660471000119109","SNOMED","Ulcer of left foot co-occurrent and due to diabetes mellitus type 2 (disorder)"
"diabetes","1066911000000100","SNOMED","Diabetes monitoring short message service text message first invitation (procedure)"
"diabetes","1066921000000106","SNOMED","Diabetes monitoring short message service text message second invitation (procedure)"
"diabetes","1066931000000108","SNOMED","Diabetes monitoring short message service text message third invitation (procedure)"
"diabetes","1067201000000106","SNOMED","Eating disorder co-occurrent with diabetes mellitus type 1 (disorder)"
"diabetes","10754881000119104","SNOMED","Diabetes mellitus in mother complicating childbirth (disorder)"
"diabetes","1083111000000108","SNOMED","Diabetes monitoring invitation email (procedure)"
"diabetes","110181000119105","SNOMED","Peripheral sensory neuropathy due to type 2 diabetes mellitus"
"diabetes","1109921000000106","SNOMED","Quality and Outcomes Framework quality indicator-related care invitation (procedure)"
"diabetes","1110921000000100","SNOMED","Quality and Outcomes Framework diabetes mellitus quality indicator-related care invitation (procedure)"
"diabetes","111307005","SNOMED","Leprechaunism syndrome (disorder)"
"diabetes","111552007","SNOMED","Diabetes mellitus without complication"
"diabetes","111556005","SNOMED","Diabetic ketoacidosis without coma"
"diabetes","112991000000101","SNOMED","Lipoatrophic diabetes mellitus without complication (disorder)"
"diabetes","11530004","SNOMED","Brittle diabetes"
"diabetes","126534007","SNOMED","Diabetic mixed sensory-motor polyneuropathy"
"diabetes","127011001","SNOMED","Diabetic sensory polyneuropathy"
"diabetes","127012008","SNOMED","Lipoatrophic diabetes"
"diabetes","127013003","SNOMED","Diabetic renal disease"
"diabetes","127014009","SNOMED","Diabetic peripheral angiopathy"
"diabetes","134395001","SNOMED","Diabetic retinopathy screening"
"diabetes","140381000119104","SNOMED","Neuropathic toe ulcer due to type 2 diabetes mellitus (disorder)"
"diabetes","140391000119101","SNOMED","Ulcer of toe due to type 2 diabetes mellitus (disorder)"
"diabetes","140521000119107","SNOMED","Ischemic foot ulcer due to type 2 diabetes mellitus (disorder)"
"diabetes","1491000119102","SNOMED","Diabetic vitreous haemorrhage associated with type II diabetes mellitus"
"diabetes","1511000119107","SNOMED","Diabetic peripheral neuropathy associated with type II diabetes mellitus"
"diabetes","157141000119108","SNOMED","Proteinuria due to type 2 diabetes mellitus"
"diabetes","170745003","SNOMED","Diabetic on diet only"
"diabetes","170747006","SNOMED","Diabetic on insulin"
"diabetes","170763003","SNOMED","Diabetic - good control"
"diabetes","170766006","SNOMED","Loss of hypoglycemic warning"
"diabetes","185756006","SNOMED","Diabetes monitoring first letter (procedure)"
"diabetes","185757002","SNOMED","Diabetes monitoring second letter (procedure)"
"diabetes","185758007","SNOMED","Diabetes monitoring third letter (procedure)"
"diabetes","185759004","SNOMED","Diabetes monitoring verbal invite (procedure)"
"diabetes","185760009","SNOMED","Diabetes monitoring telephone invite (procedure)"
"diabetes","190330002","SNOMED","Diabetes mellitus, juvenile type, with hyperosmolar coma"
"diabetes","190331003","SNOMED","Diabetes mellitus, adult onset, with hyperosmolar coma"
"diabetes","190368000","SNOMED","Type I diabetes mellitus with ulcer"
"diabetes","190369008","SNOMED","Type 1 diabetes mellitus with gangrene"
"diabetes","190372001","SNOMED","Type I diabetes mellitus maturity onset"
"diabetes","190388001","SNOMED","Type II diabetes mellitus with multiple complications"
"diabetes","190389009","SNOMED","Type 2 diabetes mellitus with ulcer"
"diabetes","190406000","SNOMED","Malnutrition-related diabetes mellitus with ketoacidosis"
"diabetes","190407009","SNOMED","Malnutrition-related diabetes mellitus with renal complications (disorder)"
"diabetes","190410002","SNOMED","Malnutrition-related diabetes mellitus with peripheral circulatory complications"
"diabetes","190411003","SNOMED","Malnutrition-related diabetes mellitus with multiple complications"
"diabetes","190412005","SNOMED","Malnutrition-related diabetes mellitus without complications (disorder)"
"diabetes","190416008","SNOMED","Steroid-induced diabetes mellitus without complication (disorder)"
"diabetes","190447002","SNOMED","Steroid-induced diabetes (disorder)"
"diabetes","193141005","SNOMED","Diabetic mononeuritis multiplex"
"diabetes","193183000","SNOMED","Acute painful diabetic neuropathy"
"diabetes","193184006","SNOMED","Chronic painful diabetic neuropathy"
"diabetes","193185007","SNOMED","Asymptomatic diabetic neuropathy"
"diabetes","193349004","SNOMED","Preproliferative diabetic retinopathy"
"diabetes","193350004","SNOMED","Advanced diabetic maculopathy"
"diabetes","193489006","SNOMED","Diabetic iritis"
"diabetes","197605007","SNOMED","Nephrotic syndrome in diabetes mellitus"
"diabetes","198121000000103","SNOMED","Hypoglycaemic warning impaired (disorder)"
"diabetes","198131000000101","SNOMED","Hypoglycaemic warning good (disorder)"
"diabetes","199230006","SNOMED","Pre-existing diabetes mellitus, non-insulin-dependent"
"diabetes","199231005","SNOMED","Pre-existing malnutrition-related diabetes mellitus"
"diabetes","200687002","SNOMED","Cellulitis in diabetic foot"
"diabetes","201250006","SNOMED","Ischaemic ulcer diabetic foot"
"diabetes","201251005","SNOMED","Neuropathic diabetic ulcer - foot"
"diabetes","201723002","SNOMED","Diabetic hand syndrome"
"diabetes","201724008","SNOMED","Diabetic Charcot's arthropathy"
"diabetes","23045005","SNOMED","Insulin dependent diabetes mellitus type IA (disorder)"
"diabetes","230572002","SNOMED","Diabetic neuropathy"
"diabetes","230574001","SNOMED","Diabetic acute painful polyneuropathy"
"diabetes","230575000","SNOMED","Diabetic chronic painful polyneuropathy"
"diabetes","230576004","SNOMED","Diabetic asymmetric polyneuropathy"
"diabetes","230577008","SNOMED","Diabetic mononeuropathy"
"diabetes","230579006","SNOMED","Diabetic thoracic radiculopathy"
"diabetes","232020009","SNOMED","Diabetic maculopathy"
"diabetes","232021008","SNOMED","Proliferative diabetic retinopathy new vessels on disc"
"diabetes","232022001","SNOMED","Proliferative diabetic retinopathy with new vessels elsewhere than on disc"
"diabetes","232023006","SNOMED","Diabetic traction retinal detachment"
"diabetes","236499007","SNOMED","Microalbuminuric diabetic nephropathy"
"diabetes","236500003","SNOMED","Clinical diabetic nephropathy"
"diabetes","237599002","SNOMED","Insulin-treated non-insulin-dependent diabetes mellitus"
"diabetes","237601000","SNOMED","Secondary endocrine diabetes mellitus (disorder)"
"diabetes","237604008","SNOMED","Diabetes mellitus autosomal dominant type II"
"diabetes","237608006","SNOMED","Lipodystrophy, partial, with Rieger anomaly, short stature, and insulinopenic diabetes mellitus (disorder)"
"diabetes","237610008","SNOMED","Acrorenal field defect, ectodermal dysplasia, and lipoatrophic diabetes (disorder)"
"diabetes","237612000","SNOMED","Photomyoclonus, diabetes mellitus, deafness, nephropathy and cerebral dysfunction (disorder)"
"diabetes","237613005","SNOMED","Hyperproinsulinemia (disorder)"
"diabetes","237616002","SNOMED","Hypogonadism, diabetes mellitus, alopecia, mental retardation and electrocardiographic abnormalities (disorder)"
"diabetes","237617006","SNOMED","Megaloblastic anemia, thiamine-responsive, with diabetes mellitus and sensorineural deafness (disorder)"
"diabetes","237618001","SNOMED","Insulin-dependent diabetes mellitus secretory diarrhea syndrome (disorder)"
"diabetes","237619009","SNOMED","Diabetes-deafness syndrome maternally transmitted (disorder)"
"diabetes","237620003","SNOMED","Abnormal metabolic state in diabetes mellitus"
"diabetes","237621004","SNOMED","Diabetic severe hyperglycemia"
"diabetes","237622006","SNOMED","Poor glycemic control (disorder)"
"diabetes","237627000","SNOMED","Pregnancy and type 2 diabetes mellitus (disorder)"
"diabetes","237632004","SNOMED","Hypoglycemic event in diabetes (disorder)"
"diabetes","237633009","SNOMED","Hypoglycemic state in diabetes"
"diabetes","237651005","SNOMED","Insulin resistance - type A (disorder)"
"diabetes","237652003","SNOMED","Insulin resistance - type B (disorder)"
"diabetes","238981002","SNOMED","Soft tissue complication of diabetes mellitus"
"diabetes","238982009","SNOMED","Diabetic dermopathy"
"diabetes","238983004","SNOMED","Diabetic thick skin syndrome"
"diabetes","238984005","SNOMED","Diabetic rubeosis"
"diabetes","24471000000103","SNOMED","Type 2 diabetic on insulin (finding)"
"diabetes","24481000000101","SNOMED","Type 2 diabetic on diet only (finding)"
"diabetes","25093002","SNOMED","Diabetic oculopathy"
"diabetes","25412000","SNOMED","Diabetic retinal microaneurysm"
"diabetes","26298008","SNOMED","Diabetic coma with ketoacidosis"
"diabetes","267467004","SNOMED","Diabetes mellitus (& [ketoacidosis]) (disorder)"
"diabetes","267471001","SNOMED","Diabetes + eye manifestation (& [cataract] or [retinopathy]) (disorder)"
"diabetes","267604001","SNOMED","Myasthenic syndrome due to diabetic amyotrophy"
"diabetes","268519009","SNOMED","Diabetic - poor control"
"diabetes","2751001","SNOMED","Fibrocalculous pancreatic diabetes (disorder)"
"diabetes","279291000000109","SNOMED","Diabetes type 1 review (regime/therapy)"
"diabetes","279321000000104","SNOMED","Diabetes type 2 review (regime/therapy)"
"diabetes","280137006","SNOMED","Diabetic foot"
"diabetes","284449005","SNOMED","Congenital total lipodystrophy (disorder)"
"diabetes","290002008","SNOMED","Unstable type I diabetes mellitus"
"diabetes","303059007","SNOMED","Postpancreatectomy hypoinsulinemia (disorder)"
"diabetes","308105005","SNOMED","O/E - Right diabetic foot at risk"
"diabetes","308106006","SNOMED","O/E - Left diabetic foot at risk"
"diabetes","309426007","SNOMED","Diabetic glomerulopathy"
"diabetes","310425007","SNOMED","Diabetes monitoring invitation (procedure)"
"diabetes","310505005","SNOMED","Diabetic hyperosmolar non-ketotic state"
"diabetes","311782002","SNOMED","Advanced diabetic retinal disease"
"diabetes","31211000119101","SNOMED","Peripheral vascular disease due to type I diabetes"
"diabetes","312903003","SNOMED","Mild non proliferative diabetic retinopathy"
"diabetes","312904009","SNOMED","Moderate non proliferative diabetic retinopathy"
"diabetes","312905005","SNOMED","Severe non proliferative diabetic retinopathy"
"diabetes","312906006","SNOMED","Proliferative diabetic retinopathy - non high risk"
"diabetes","312907002","SNOMED","Proliferative diabetic retinopathy - high risk"
"diabetes","312908007","SNOMED","Proliferative diabetic retinopathy - quiescent"
"diabetes","312909004","SNOMED","Proliferative diabetic retinopathy - iris neovascularisation"
"diabetes","312910009","SNOMED","Diabetic vitreous hemorrhage"
"diabetes","312912001","SNOMED","Diabetic macular edema"
"diabetes","313435000","SNOMED","Type I diabetes mellitus without complication"
"diabetes","313436004","SNOMED","Non-insulin-dependent diabetes mellitus without complication"
"diabetes","314010006","SNOMED","Diffuse diabetic maculopathy"
"diabetes","314011005","SNOMED","Focal diabetic maculopathy"
"diabetes","314014002","SNOMED","Ischaemic diabetic maculopathy"
"diabetes","314015001","SNOMED","Mixed diabetic maculopathy"
"diabetes","314537004","SNOMED","Diabetic optic papillopathy"
"diabetes","314771006","SNOMED","Type 1 diabetes mellitus with hypoglycaemic coma"
"diabetes","314893005","SNOMED","Type 1 diabetes mellitus with arthropathy"
"diabetes","314902007","SNOMED","Type II diabetes mellitus with peripheral angiopathy"
"diabetes","314903002","SNOMED","Non-insulin dependent diabetes mellitus with arthropathy"
"diabetes","314904008","SNOMED","Type II diabetes mellitus with neuropathic arthropathy"
"diabetes","33559001","SNOMED","Pineal hyperplasia AND diabetes mellitus syndrome (disorder)"
"diabetes","335621000000101","SNOMED","Maternally inherited diabetes mellitus (disorder)"
"diabetes","361216007","SNOMED","Diabetic femoral mononeuropathy"
"diabetes","367261000119100","SNOMED","Hyperosmolarity co-occurrent and due to drug induced diabetes mellitus (disorder)"
"diabetes","367991000119101","SNOMED","Hyperglycemia due to type 1 diabetes mellitus"
"diabetes","368601000119102","SNOMED","Hyperosmolar coma due to secondary diabetes mellitus"
"diabetes","371087003","SNOMED","Diabetic foot ulcer (disorder)"
"diabetes","385041000000108","SNOMED","Diabetes mellitus with multiple complications (disorder)"
"diabetes","385051000000106","SNOMED","Pre-existing diabetes mellitus (disorder)"
"diabetes","39058009","SNOMED","Diabetic amyotrophy"
"diabetes","390834004","SNOMED","Non proliferative diabetic retinopathy (disorder)"
"diabetes","390850007","SNOMED","O/E - no right diabetic retinopathy (context-dependent category)"
"diabetes","390853009","SNOMED","O/E - no left diabetic retinopathy (context-dependent category)"
"diabetes","390854003","SNOMED","O/E - diabetic maculopathy present both eyes (context-dependent category)"
"diabetes","390855002","SNOMED","O/E - diabetic maculopathy absent both eyes (context-dependent category)"
"diabetes","395204000","SNOMED","Hyperosmolar non-ketotic state in type 2 diabetes mellitus (disorder)"
"diabetes","398140007","SNOMED","Somogyi phenomenon (disorder)"
"diabetes","399864000","SNOMED","Diabetic macular edema not clinically significant (disorder)"
"diabetes","399865004","SNOMED","Very severe proliferative diabetic retinopathy (disorder)"
"diabetes","399866003","SNOMED","Diabetic retinal venous beading (disorder)"
"diabetes","399870006","SNOMED","Non-high-risk proliferative diabetic retinopathy with no macular edema (disorder)"
"diabetes","399871005","SNOMED","Visually threatening diabetic retinopathy (disorder)"
"diabetes","401110002","SNOMED","Type 1 diabetes mellitus with persistent microalbuminuria (disorder)"
"diabetes","401191002","SNOMED","Diabetic foot examination (regime/therapy)"
"diabetes","408397002","SNOMED","Diabetic foot examination not indicated (context-dependent category)"
"diabetes","408409007","SNOMED","O/E - right eye background diabetic retinopathy (context-dependent category)"
"diabetes","408410002","SNOMED","O/E - left eye background diabetic retinopathy (context-dependent category)"
"diabetes","408411003","SNOMED","O/E - right eye preproliferative diabetic retinopathy (context-dependent category)"
"diabetes","408412005","SNOMED","O/E - left eye preproliferative diabetic retinopathy (context-dependent category)"
"diabetes","408413000","SNOMED","O/E - right eye proliferative diabetic retinopathy (context-dependent category)"
"diabetes","408414006","SNOMED","O/E - left eye proliferative diabetic retinopathy (context-dependent category)"
"diabetes","408539000","SNOMED","Insulin autoimmune syndrome (disorder)"
"diabetes","408540003","SNOMED","Diabetes mellitus caused by non-steroid drugs (disorder)"
"diabetes","412752009","SNOMED","Diabetic foot examination declined (context-dependent category)"
"diabetes","413183008","SNOMED","Diabetes mellitus caused by non-steroid drugs without complication (disorder)"
"diabetes","413184002","SNOMED","Fibrocalculous pancreatopathy without complication (disorder)"
"diabetes","414894003","SNOMED","O/E - left eye stable treated proliferative diabetic retinopathy (context-dependent category)"
"diabetes","414910007","SNOMED","O/E - right eye stable treated proliferative diabetic retinopathy (context-dependent category)"
"diabetes","417677008","SNOMED","O/E - sight threatening diabetic retinopathy (context-dependent category)"
"diabetes","419100001","SNOMED","Infection of foot associated with diabetes (disorder)"
"diabetes","420270002","SNOMED","Ketoacidosis in type I diabetes mellitus (disorder)"
"diabetes","420279001","SNOMED","Renal disorder associated with type II diabetes mellitus (disorder)"
"diabetes","420422005","SNOMED","Ketoacidosis in diabetes mellitus (disorder)"
"diabetes","420436000","SNOMED","Mononeuropathy associated with type II diabetes mellitus (disorder)"
"diabetes","420486006","SNOMED","Exudative maculopathy associated with type I diabetes mellitus (disorder)"
"diabetes","420514000","SNOMED","Persistent proteinuria associated with type I diabetes mellitus (disorder)"
"diabetes","420662003","SNOMED","Coma associated with diabetes mellitus (disorder)"
"diabetes","420683009","SNOMED","Neurological disorder associated with malnutrition-related diabetes mellitus (disorder)"
"diabetes","420715001","SNOMED","Persistent microalbuminuria associated with type II diabetes mellitus (disorder)"
"diabetes","420756003","SNOMED","Diabetic cataract associated with type II diabetes mellitus (disorder)"
"diabetes","420789003","SNOMED","Diabetic retinopathy associated with type I diabetes mellitus (disorder)"
"diabetes","420825003","SNOMED","Gangrene associated with type I diabetes mellitus (disorder)"
"diabetes","420868002","SNOMED","Disorder associated with type I diabetes mellitus (disorder)"
"diabetes","420918009","SNOMED","Mononeuropathy associated with type I diabetes mellitus (disorder)"
"diabetes","420996007","SNOMED","Coma associated with malnutrition-related diabetes mellitus (disorder)"
"diabetes","421075007","SNOMED","Ketoacidotic coma in type I diabetes mellitus (disorder)"
"diabetes","421256007","SNOMED","Ophthalmic complication of malnutrition-related diabetes mellitus (disorder)"
"diabetes","421326000","SNOMED","Neurologic disorder associated with type II diabetes mellitus (disorder)"
"diabetes","421365002","SNOMED","Peripheral circulatory disorder associated with type I diabetes mellitus (disorder)"
"diabetes","421468001","SNOMED","Neurological disorder associated with type I diabetes mellitus (disorder)"
"diabetes","421631007","SNOMED","Gangrene associated with type II diabetes mellitus (disorder)"
"diabetes","421750000","SNOMED","Ketoacidosis in type II diabetes mellitus (disorder)"
"diabetes","421779007","SNOMED","Exudative maculopathy associated with type II diabetes mellitus (disorder)"
"diabetes","421847006","SNOMED","Ketoacidotic coma in type II diabetes mellitus (disorder)"
"diabetes","421893009","SNOMED","Renal disorder associated with type I diabetes mellitus (disorder)"
"diabetes","421895002","SNOMED","Peripheral circulatory disorder associated with diabetes mellitus (disorder)"
"diabetes","421920002","SNOMED","Diabetic cataract associated with type I diabetes mellitus (disorder)"
"diabetes","421986006","SNOMED","Persistent proteinuria associated with type II diabetes mellitus (disorder)"
"diabetes","422014003","SNOMED","Disorder associated with type II diabetes melliltus (disorder)"
"diabetes","422034002","SNOMED","Diabetic retinopathy associated with type II diabetes mellitus (disorder)"
"diabetes","422088007","SNOMED","Neurologic disorder associated with diabetes mellitus (disorder)"
"diabetes","422099009","SNOMED","Diabetic oculopathy associated with type II diabetes mellitus (disorder)"
"diabetes","422126006","SNOMED","Hyperosmolar coma associated with diabetes mellitus (disorder)"
"diabetes","422166005","SNOMED","Peripheral circulatory disorder associated with type II diabetes mellitus (disorder)"
"diabetes","422183001","SNOMED","Skin ulcer associated with diabetes mellitus (disorder)"
"diabetes","422228004","SNOMED","Multiple complications of type I diabetes mellitus (disorder)"
"diabetes","422275004","SNOMED","Gangrene associated with diabetes mellitus (disorder)"
"diabetes","424736006","SNOMED","Diabetic peripheral neuropathy (disorder)"
"diabetes","426705001","SNOMED","Diabetes mellitus associated with cystic fibrosis (disorder)"
"diabetes","426875007","SNOMED","Latent autoimmune diabetes mellitus in adult (disorder)"
"diabetes","429729007","SNOMED","Diabetic education completed (situation)"
"diabetes","43959009","SNOMED","Diabetic cataract"
"diabetes","44054006","SNOMED","Diabetes mellitus type II"
"diabetes","441656006","SNOMED","Hyperglycaemic crisis in diabetes mellitus"
"diabetes","443694000","SNOMED","Type II diabetes mellitus uncontrolled (finding)"
"diabetes","444073006","SNOMED","Type I diabetes mellitus uncontrolled (finding)"
"diabetes","445353002","SNOMED","Brittle type II diabetes mellitus (finding)"
"diabetes","46635009","SNOMED","Insulin dependent diabetes mellitus"
"diabetes","472969004","SNOMED","History of diabetes mellitus type 2 (situation)"
"diabetes","472970003","SNOMED","History of diabetes mellitus type 1 (situation)"
"diabetes","472972006","SNOMED","History of autosomal dominant diabetes mellitus"
"diabetes","4855003","SNOMED","Diabetic retinopathy"
"diabetes","48951005","SNOMED","Bullosis diabeticorum"
"diabetes","49455004","SNOMED","Diabetic polyneuropathy"
"diabetes","50620007","SNOMED","Diabetic autonomic neuropathy"
"diabetes","51002006","SNOMED","Diabetes mellitus associated with pancreatic disease"
"diabetes","530558861000132104","SNOMED","Atypical diabetes mellitus (disorder)"
"diabetes","532411000000102","SNOMED","Diabetes mellitus, adult onset, with no mention of complication (disorder)"
"diabetes","5368009","SNOMED","Drug-induced diabetes mellitus (disorder)"
"diabetes","59276001","SNOMED","Proliferative diabetic retinopathy"
"diabetes","5969009","SNOMED","Diabetes mellitus associated with genetic syndrome (disorder)"
"diabetes","609561005","SNOMED","Maturity-onset diabetes of the young (disorder)"
"diabetes","609562003","SNOMED","Maturity onset diabetes of the young, type 1 (disorder)"
"diabetes","609563008","SNOMED","Pre-existing diabetes mellitus in pregnancy (disorder)"
"diabetes","609572000","SNOMED","Maturity-onset diabetes of the young, type 5 (disorder)"
"diabetes","62260007","SNOMED","Pretibial pigmental patches in diabetes"
"diabetes","658011000000104","SNOMED","Diabetes mellitus with other specified manifestation (disorder)"
"diabetes","703136005","SNOMED","Diabetes mellitus in remission (disorder)"
"diabetes","703137001","SNOMED","Type I diabetes mellitus in remission (disorder)"
"diabetes","703138006","SNOMED","Type II diabetes mellitus in remission (disorder)"
"diabetes","705072004","SNOMED","Diabetes monitoring invitation by short message service text messaging (procedure)"
"diabetes","70694009","SNOMED","Diabetes mellitus AND insipidus with optic atrophy AND deafness (disorder)"
"diabetes","707221002","SNOMED","Diabetic glomerulosclerosis"
"diabetes","713702000","SNOMED","Gastroparesis due to type 1 diabetes mellitus"
"diabetes","713703005","SNOMED","Gastroparesis due to type 2 diabetes mellitus (disorder)"
"diabetes","713704004","SNOMED","Gastroparesis due to diabetes mellitus (disorder)"
"diabetes","713705003","SNOMED","Polyneuropathy due to type 1 diabetes mellitus"
"diabetes","713706002","SNOMED","Polyneuropathy due to type 2 diabetes mellitus"
"diabetes","71771000119100","SNOMED","Type 1 diabetes mellitus with neuropathic arthropathy"
"diabetes","719216001","SNOMED","Hypoglycemic coma co-occurrent and due to diabetes mellitus type II (disorder)"
"diabetes","722161008","SNOMED","Diabetic retinal eye exam (procedure)"
"diabetes","724136006","SNOMED","Diabetic mastopathy"
"diabetes","724810001","SNOMED","Radiculoplexoneuropathy due to diabetes mellitus (disorder)"
"diabetes","724997001","SNOMED","Lumbosacral plexopathy co-occurrent and due to diabetes mellitus (disorder)"
"diabetes","731000119105","SNOMED","Chronic kidney disease stage 3 associated with type 2 diabetes mellitus"
"diabetes","73211009","SNOMED","Diabetes mellitus"
"diabetes","735200002","SNOMED","Absence of lower limb due to diabetes mellitus (disorder)"
"diabetes","735538002","SNOMED","Lactic acidosis co-occurrent and due to diabetes mellitus (disorder)"
"diabetes","739681000","SNOMED","Diabetic oculopathy due to type I diabetes mellitus"
"diabetes","74627003","SNOMED","Diabetic complication"
"diabetes","754101000000103","SNOMED","Type I diabetic dietary review (regime/therapy)"
"diabetes","754121000000107","SNOMED","Type II diabetic dietary review (regime/therapy)"
"diabetes","754461000000105","SNOMED","Referral to type I diabetes structured education programme (procedure)"
"diabetes","75524006","SNOMED","Malnutrition related diabetes mellitus"
"diabetes","75682002","SNOMED","Diabetes mellitus caused by insulin receptor antibodies (disorder)"
"diabetes","761000119102","SNOMED","Diabetic dyslipidemia associated with type 2 diabetes mellitus"
"diabetes","762489000","SNOMED","Acute complication with diabetes mellitus"
"diabetes","763325000","SNOMED","Insulin resistance (disorder)"
"diabetes","768792007","SNOMED","Cataract of right eye co-occurrent and due to diabetes mellitus"
"diabetes","768794008","SNOMED","Bilateral diabetic cataracts"
"diabetes","769181007","SNOMED","Preproliferative retinopathy of right eye co-occurrent and due to diabetes mellitus (disorder)"
"diabetes","769182000","SNOMED","Preproliferative diabetic retinopathy of left eye"
"diabetes","769183005","SNOMED","Mild nonproliferative diabetic retinopathy of right eye"
"diabetes","769184004","SNOMED","Mild nonproliferative retinopathy of left eye"
"diabetes","769185003","SNOMED","Moderate non-proliferative diabetic retinopathy of right eye"
"diabetes","769186002","SNOMED","Moderate nonproliferative diabetic retinopathy of left eye"
"diabetes","769217008","SNOMED","Diabetic macular edema of right eye"
"diabetes","769218003","SNOMED","Macular oedema of left eye co-occurrent and due to diabetes mellitus"
"diabetes","769219006","SNOMED","Macular edema due to type 1 diabetes mellitus"
"diabetes","769221001","SNOMED","Clinically significant macular edema of right eye co-occurrent and due to diabetes mellitus (disorder)"
"diabetes","769222008","SNOMED","Clinically significant macular oedema of left eye co-occurrent and due to diabetes mellitus"
"diabetes","769244003","SNOMED","Diabetic maculopathy of right eye"
"diabetes","769245002","SNOMED","Disorder of left macula co-occurrent and due to diabetes mellitus (disorder)"
"diabetes","770097006","SNOMED","Clinically significant macular edema co-occurrent and due to diabetes mellitus (disorder)"
"diabetes","771571000000102","SNOMED","History of secondary diabetes mellitus (situation)"
"diabetes","773001000000103","SNOMED","Symptomatic diabetic peripheral neuropathy (disorder)"
"diabetes","775841000000109","SNOMED","Diabetic retinopathy detected by national screening programme (disorder)"
"diabetes","791000119109","SNOMED","Angina associated with type II diabetes mellitus"
"diabetes","80660001","SNOMED","Mauriac's syndrome (disorder)"
"diabetes","81531005","SNOMED","Diabetes mellitus type 2 in obese (disorder)"
"diabetes","83728000","SNOMED","Polyglandular autoimmune syndrome, type 2 (disorder)"
"diabetes","87451000119102","SNOMED","Heel AND/OR midfoot ulcer due to type 2 diabetes mellitus"
"diabetes","8801005","SNOMED","Secondary diabetes mellitus"
"diabetes","894741000000107","SNOMED","Hypoglycaemic warning absent (disorder)"

"""
codelist_comorbidity1 = (
  spark.createDataFrame(
    pd.DataFrame(pd.read_csv(io.StringIO(codelist_comorbidity1)))
    .fillna('')
    .astype(str)
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 PCI

# COMMAND ----------

# angina / HF / Diabetes codelist
codelist_pci = (opcs_lkp
                         .where(f.substring(f.col('code'), 1, 3) == 'K75')
                         .withColumn('terminology', f.lit('OPCS4'))
                         .withColumn('name', f.lit('pci'))
                         .select('name', 'code', 'terminology', 'term'))
display(codelist_pci)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Cerebrovascular Disease

# COMMAND ----------

codelist_cevd_icd = (icd_lkp
                    .where(f.substring(f.col('code'), 1, 3).isin(['I60', 'I61', 'I62', 'I63', 'I64', 'I65', 'I66', 'I67', 'I68', 'I69', 'G45']))
                    .withColumn('terminology', f.lit('ICD10'))
                    .withColumn('name', f.lit('cevd'))
                    .select('name', 'code', 'terminology', 'term'))
display(codelist_cevd_icd)

# COMMAND ----------

codelist_cevd_sct = (gdppr_refset
                    .where((f.col('Cluster_ID').isin(['HSTRK_COD', 'OSTR_COD', 'STRK_COD', 'TIA_COD'])) | (f.col('ConceptId').isin(['195163003', '67992007', '73192008'])))
                    .withColumn('terminology', f.lit('SNOMED'))
                    .withColumn('name', f.lit('cevd'))
                    .withColumnRenamed('ConceptId', 'code')
                    .withColumnRenamed('ConceptId_Description', 'term')
                    .select('name', 'code', 'terminology', 'term')
                   )
display(codelist_cevd_sct)

# COMMAND ----------

codelist_cevd = (codelist_cevd_icd
                .unionByName(codelist_cevd_sct))
display(codelist_cevd)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 End Stage Renal Disease

# COMMAND ----------

codelist_esrd_icd = (icd_lkp
                    .where((f.col('code').isin(['N165', 'N180', 'N185', 'T824', 'T861', 'Y602', 'Y612', 'Y622', 'Y841', 'Z940', 'Z992'])) | (f.substring(f.col('code'), 1, 3).isin(['Z49'])))
                    .withColumn('terminology', f.lit('ICD10'))
                    .withColumn('name', f.lit('esrd'))
                    .select('name', 'code', 'terminology', 'term')                  
                   )
display(codelist_esrd_icd)

# COMMAND ----------

codelist_esrd_sct = """
"name","code","term"
esrd,324501000000107,Chronic kidney disease stage 5 with proteinuria (disorder)
esrd,324541000000105,Chronic kidney disease stage 5 without proteinuria (disorder)
esrd,433146000,Chronic kidney disease stage 5 (disorder)
esrd,714152005,Chronic kidney disease stage 5 on dialysis (disorder)
esrd,950251000000106,Chronic kidney disease with glomerular filtration rate category G5 and albuminuria category A1 (disorder)
esrd,950291000000103,Chronic kidney disease with glomerular filtration rate category G5 and albuminuria category A2 (disorder)
esrd,950311000000102,Chronic kidney disease with glomerular filtration rate category G5 and albuminuria category A3 (disorder)
esrd,105502003,Dependence on renal dialysis (finding)
esrd,11000700000000000,Dependence on continuous ambulatory peritoneal dialysis (finding)
esrd,11000800000000000,Dependence on continuous cycling peritoneal dialysis (finding)
esrd,1110500000000000,Dialysis adequacy using dialyser clearance of urea multiplied by treatment time divided by volume of distribution of urea formula by combined peritoneal dialysate and urine method (observable entity)
esrd,1110510000000000,Dialysis adequacy using dialyser clearance of urea multiplied by treatment time divided by volume of distribution of urea formula for haemodialysis (observable entity)
esrd,11932001,Stabilizing hemodialysis (procedure)
esrd,127991000000000,Hypertension concurrent and due to end stage renal disease on dialysis due to type 2 diabetes mellitus (disorder)
esrd,128001000000000,Hypertension concurrent and due to end stage renal disease on dialysis due to type 1 diabetes mellitus (disorder)
esrd,129161000000000,Chronic kidney disease stage 5 due to hypertension (disorder)
esrd,14684005,Peritoneal dialysis excluding cannulation (procedure)
esrd,153851000000000,Malignant hypertensive chronic kidney disease stage 5 (disorder)
esrd,161665007,History of renal transplant (situation)
esrd,1721000000000,Patient awaiting renal transplant (finding)
esrd,175901007,Live donor renal transplant (procedure)
esrd,175902000,Cadaveric renal transplant (procedure)
esrd,180272001,Insertion of chronic ambulatory peritoneal dialysis catheter (procedure)
esrd,213150003,Kidney transplant failure and rejection (disorder)
esrd,225230008,Chronic peritoneal dialysis (procedure)
esrd,225231007,Stab peritoneal dialysis (procedure)
esrd,233575001,Intermittent hemodialysis (procedure)
esrd,233576000,Intermittent hemodialysis with sequential ultrafiltration (procedure)
esrd,233577009,Intermittent hemodialysis with continuous ultrafiltration (procedure)
esrd,233578004,Continuous hemodialysis (procedure)
esrd,233579007,CAVD - continuous arteriovenous hemodialysis
esrd,233580005,CVVD - continuous venovenous hemodialysis
esrd,233581009,Hemofiltration (procedure)
esrd,233582002,Intermittent hemofiltration (procedure)
esrd,233583007,Continuous hemofiltration (procedure)
esrd,233584001,CAVH - continuous arteriovenous hemofiltration
esrd,233585000,CVVHF - continuous venovenous hemofiltration
esrd,233586004,Hemodiafiltration (procedure)
esrd,233587008,Intermittent hemodiafiltration (procedure)
esrd,233588003,Continuous hemodiafiltration (procedure)
esrd,233589006,CAVHD - continuous arteriovenous hemodiafiltration
esrd,233590002,CVVHD - continuous venovenous hemodiafiltration
esrd,236138007,Xenograft renal transplant (procedure)
esrd,236434000,End stage renal failure untreated by renal replacement therapy (disorder)
esrd,236435004,End stage renal failure on dialysis (disorder)
esrd,236436003,End stage renal failure with renal transplant (disorder)
esrd,236569000,Primary non-function of renal transplant (disorder)
esrd,236570004,Renal transplant rejection (disorder)
esrd,236571000,Hyperacute rejection of renal transplant (disorder)
esrd,236572007,Accelerated rejection of renal transplant (disorder)
esrd,236573002,Very mild acute rejection of renal transplant (disorder)
esrd,236574008,Acute rejection of renal transplant (disorder)
esrd,236575009,Acute rejection of renal transplant - grade I (disorder)
esrd,236576005,Acute rejection of renal transplant - grade II (disorder)
esrd,236577001,Acute rejection of renal transplant - grade III (disorder)
esrd,236578006,Chronic rejection of renal transplant (disorder)
esrd,236579003,Chronic rejection of renal transplant - grade I (disorder)
esrd,236580000,Chronic rejection of renal transplant - grade II (disorder)
esrd,236581001,Chronic rejection of renal transplant - grade III (disorder)
esrd,236582008,Acute-on-chronic rejection of renal transplant (disorder)
esrd,236583003,Failed renal transplant (disorder)
esrd,236584009,Perfusion injury of renal transplant (disorder)
esrd,236587002,Transplant glomerulopathy (disorder)
esrd,236588007,Transplant glomerulopathy - early form (disorder)
esrd,236589004,Transplant glomerulopathy - late form (disorder)
esrd,236614007,Perirenal and periureteric post-transplant lymphocele (disorder)
esrd,238314006,Renewal of chronic ambulatory peritoneal dialysis catheter (procedure)
esrd,238315007,Adjustment of chronic ambulatory peritoneal dialysis catheter (procedure)
esrd,238316008,Aspiration of chronic ambulatory peritoneal dialysis catheter (procedure)
esrd,238317004,Flushing of chronic ambulatory peritoneal dialysis catheter (procedure)
esrd,238318009,Continuous ambulatory peritoneal dialysis (procedure)
esrd,238319001,Continuous cycling peritoneal dialysis (procedure)
esrd,238321006,Intermittent peritoneal dialysis (procedure)
esrd,238322004,Tidal peritoneal dialysis (procedure)
esrd,238323009,Night-time intermittent peritoneal dialysis (procedure)
esrd,265764009,Renal dialysis (procedure)
esrd,271418008,Chronic ambulatory peritoneal dialysis catheter procedure (procedure)
esrd,277010001,Unexplained episode of renal transplant dysfunction (disorder)
esrd,277011002,Pre-existing disease in renal transplant (disorder)
esrd,285011000000000,Chronic kidney disease stage 5 due to benign hypertension (disorder)
esrd,286371000000000,Malignant hypertensive end stage renal disease on dialysis (disorder)
esrd,288182009,Extracorporeal kidney (procedure)
esrd,302497006,Hemodialysis (procedure)
esrd,313030004,Donor renal transplantation (procedure)
esrd,324501000000000,Chronic kidney disease stage 5 with proteinuria (disorder)
esrd,324541000000000,Chronic kidney disease stage 5 without proteinuria (disorder)
esrd,3257008,Empty and measure peritoneal dialysis fluid (procedure)
esrd,34897002,Hemodialysis maintenance in hospital (procedure)
esrd,366961000000000,Renal transplant recipient (finding)
esrd,381000000000,Increase dialyzer size for hemodialysis (procedure)
esrd,398887003,Renal replacement (procedure)
esrd,426136000,Delayed renal graft function (disorder)
esrd,427053002,Extracorporeal albumin hemodialysis (procedure)
esrd,428575007,Hypertension secondary to kidney transplant (disorder)
esrd,428648006,Automated peritoneal dialysis (procedure)
esrd,428937001,Dependence on peritoneal dialysis due to end stage renal disease (finding)
esrd,428982002,Dependence on hemodialysis due to end stage renal disease (finding)
esrd,429075005,Dependence on dialysis due to end stage renal disease (finding)
esrd,429451003,Disorder related to renal transplantation (disorder)
esrd,433146000,Chronic kidney disease stage 5 (disorder)
esrd,443143006,Dependence on hemodialysis (finding)
esrd,443596009,Dependence on peritoneal dialysis (finding)
esrd,46177005,End-stage renal disease (disorder)
esrd,52213001,Renal homotransplantation excluding donor and recipient nephrectomy (procedure)
esrd,57274006,Initial hemodialysis (procedure)
esrd,58797008,Complication of transplanted kidney (disorder)
esrd,6471000000000,Transplantation of kidney and pancreas (procedure)
esrd,676002,Peritoneal dialysis including cannulation (procedure)
esrd,67970008,Hemodialysis maintenance at home (procedure)
esrd,68341005,Hemodialysis supervision at home (procedure)
esrd,698074000,Sustained low-efficiency dialysis (procedure)
esrd,703048006,Vesicoureteric reflux after renal transplant (disorder)
esrd,704667004,Hypertension concurrent and due to end stage renal disease on dialysis (disorder)
esrd,70536003,Transplant of kidney (procedure)
esrd,707148007,Recurrent post-transplant renal disease (disorder)
esrd,708930002,Maintenance hemodiafiltration (procedure)
esrd,708931003,Maintenance hemodialysis (procedure)
esrd,708932005,Emergency hemodialysis (procedure)
esrd,708933000,Emergency hemodiafiltration (procedure)
esrd,708934006,Maintenance hemofiltration (procedure)
esrd,711411006,Allotransplantation of kidney from beating heart cadaver (procedure)
esrd,711413009,Allotransplantation of kidney from non-beating heart cadaver (procedure)
esrd,71192002,Peritoneal dialysis (procedure)
esrd,713825007,Renal artery stenosis of transplanted kidney (disorder)
esrd,714152005,Chronic kidney disease stage 5 on dialysis (disorder)
esrd,714153000,Chronic kidney disease stage 5 with transplant (disorder)
esrd,714749008,Continuous renal replacement therapy (procedure)
esrd,715743002,Emergency hemofiltration (procedure)
esrd,719352000,Kidney transplant recipient (finding)
esrd,73257006,Peritoneal dialysis catheter maintenance (procedure)
esrd,736919006,Insertion of hemodialysis catheter (procedure)
esrd,736922008,Insertion of tunneled hemodialysis catheter (procedure)
esrd,737295003,Transplanted kidney present (finding)
esrd,765478004,Allotransplantation of left kidney (procedure)
esrd,765479007,Allotransplantation of right kidney (procedure)
esrd,843691000000000,Urological complication of renal transplant (disorder)
esrd,844661000000000,Vascular complication of renal transplant (disorder)
esrd,847791000000000,Rupture of artery of transplanted kidney (disorder)
esrd,847811000000000,Rupture of vein of transplanted kidney (disorder)
esrd,847881000000000,Stenosis of vein of transplanted kidney (disorder)
esrd,852981000000000,Aneurysm of vein of transplanted kidney (disorder)
esrd,853021000000000,Aneurysm of artery of transplanted kidney (disorder)
esrd,864271000000000,Thrombosis of artery of transplanted kidney (disorder)
esrd,864311000000000,Thrombosis of vein of transplanted kidney (disorder)
esrd,90688005,Chronic renal failure syndrome (disorder)
esrd,90771000000000,End stage renal disease on dialysis due to type 1 diabetes mellitus (disorder)
esrd,90791000000000,End stage renal disease on dialysis due to type 2 diabetes mellitus (disorder)
esrd,950251000000000,Chronic kidney disease with glomerular filtration rate category G5 and albuminuria category A1 (disorder)
esrd,950291000000000,Chronic kidney disease with glomerular filtration rate category G5 and albuminuria category A2 (disorder)
esrd,950311000000000,Chronic kidney disease with glomerular filtration rate category G5 and albuminuria category A3 (disorder)
esrd,96711000000000,Hypertensive heart AND chronic kidney disease stage 5 (disorder)
esrd,1035470000000000,Dialysis therapy started by renal service (finding)
esrd,1095230000000000,Transplant of kidney using robotic assistance (procedure)
esrd,153891000000000,End stage renal disease on dialysis due to hypertension (disorder)
esrd,197747000,Renal tubulo-interstitial disorders in transplant rejection (disorder)
esrd,224967001,Revision of cannula for dialysis (procedure)
esrd,225892009,Revision of arteriovenous shunt for renal dialysis (procedure)
esrd,234046009,Transplant renal vein thrombosis (disorder)
esrd,236610003,Escape of urine from transplanted ureter (disorder)
esrd,278905001,Continuous ambulatory peritoneal dialysis diet (finding)
esrd,385971003,Dialysis care (regime/therapy)
esrd,406168002,Dialysis access maintenance (regime/therapy)
esrd,428645009,Examination of recipient after kidney transplant (procedure)
esrd,432018009,Replacement of dialysis catheter using fluoroscopic guidance (procedure)
esrd,432654009,Insertion of peritoneal dialysis catheter using fluoroscopy guidance (procedure)
esrd,438342006,Replacement of peritoneal dialysis catheter (procedure)
esrd,439073006,Peritoneal dialysis catheter in situ (finding)
esrd,440084005,Assessment of adequacy of dialysis (procedure)
esrd,442326005,Cloudy peritoneal dialysis effluent (finding)
esrd,473195006,Normal renal function of transplanted kidney (finding)
esrd,473397008,Dialysis catheter in situ usable (finding)
esrd,710071003,Management of peritoneal dialysis (procedure)
esrd,711446003,Transplantation of kidney regime (regime/therapy)
esrd,712754003,Replacement of continuous ambulatory peritoneal dialysis transfer set (procedure)
esrd,717738008,Peritoneal dialysis care assessment (procedure)
esrd,718308002,Peritoneal dialysis care (regime/therapy)
esrd,718330001,Hemodialysis care (regime/therapy)
esrd,96701000000000,Hypertensive heart AND chronic kidney disease on dialysis (disorder)
esrd,16320600000000000,Dependence on continuous ambulatory peritoneal dialysis due to end stage renal disease (finding)
esrd,782655004,Laparoscopic transplant of kidney using robotic assistance (procedure)
esrd,140101000000000,Hypertension in chronic kidney disease stage 5 due to type 2 diabetes mellitus (disorder)
esrd,691411000000000,Anemia co-occurrent and due to chronic kidney disease stage 5 (disorder)
esrd,711000000000,Chronic kidney disease stage 5 due to type 2 diabetes mellitus (disorder)
esrd,90761000000000,Chronic kidney disease stage 5 due to type 1 diabetes mellitus (disorder)
esrd,698306007,Awaiting transplantation of kidney (situation)
esrd,285841000000000,Malignant hypertensive end stage renal disease (disorder)
esrd,368461000000000,Chronic kidney disease stage 5 due to drug induced diabetes mellitus (disorder)
esrd,895382009,Prolonged intermittent renal replacement therapy (procedure)
esrd,898185002,Dependence on prolonged intermittent renal replacement therapy due to renal failure (finding)
esrd,111411000000000,End stage renal disease due to hypertension (disorder)
esrd,1144941009,Chronic tubulointerstitial nephritis following renal transplantation (disorder)
esrd,197655004,End stage renal failure (disorder)
esrd,197755007,End stage renal failure (disorder)
esrd,212001000000000,Chronic kidney disease stage 5
esrd,212011000000000,Chronic kidney disease stage 5
esrd,219601000000000,End stage renal disease
esrd,368471000000000,End stage renal disease on dialysis due to drug induced diabetes mellitus (disorder)
esrd,698810000,Hypertensive renal disease with end stage renal failure (disorder)
esrd,712487000,End stage renal disease due to benign hypertension (disorder)
esrd,834041000000000,Chronic kidney disease stage 5
esrd,1148920008,Inflammation of retina due to chronic kidney disease stage 5 (disorder)
esrd,1153581007,Matrix stone formation during renal dialysis (disorder)
esrd,28281000000000,History of peritoneal dialysis (situation)
"""
codelist_esrd_sct = (
  spark.createDataFrame(
    pd.DataFrame(pd.read_csv(io.StringIO(codelist_esrd_sct)))
    .fillna('')
    .astype(str)
  )
)
codelist_esrd_sct = (codelist_esrd_sct
                     .withColumn('terminology', f.lit('SNOMED'))
                     .select('name', 'code', 'terminology', 'term')
                    )

# COMMAND ----------

codelist_esrd = (codelist_esrd_icd
                 .unionByName(codelist_esrd_sct)
                )
display(codelist_esrd)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 MI

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

# MAGIC %md
# MAGIC # 3 Combine

# COMMAND ----------

codelist_comorbidity = (codelist_comorbidity1
                        .unionByName(codelist_pci)
                        .unionByName(codelist_cevd)
                        .unionByName(codelist_esrd)
                        .unionByName(codelist_mi)
                       )
display(codelist_comorbidity)                        

# COMMAND ----------

# MAGIC %md # 4 Reformat

# COMMAND ----------

# remove trailing X's, decimal points, dashes, and spaces
codelist_comorbidity = (codelist_comorbidity
  .withColumn('name', f.when(f.substring(f.col('name'),1,6) =='stroke', 'stroke').otherwise(f.col('name')))
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'X$', '')).otherwise(f.col('code')))
  .withColumn('code', f.when(f.col('terminology') == 'ICD10', f.regexp_replace('code', r'[\.\-\s]', '')).otherwise(f.col('code'))))

# COMMAND ----------

display(codelist_comorbidity)

# COMMAND ----------

# MAGIC %md # 5 Check

# COMMAND ----------

# check
tmpt = tab(codelist_comorbidity, 'name', 'terminology', var2_unstyled=1)

# COMMAND ----------

# MAGIC %md # 6 Save

# COMMAND ----------

# save name
outName = f'{proj}_out_codelist_comorbidity'
# save
#print(f'saving {dbc}.{outName}')
codelist_comorbidity.write.mode('overwrite').saveAsTable(f'{dbc}.{outName}')
#spark.sql(f'ALTER TABLE {dbc}.{outName} OWNER TO {dbc}')
#print(f'  saved')
