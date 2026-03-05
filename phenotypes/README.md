A list of all phenotypes with links to the HDR UK Phenoptype Library is provided in [phenotypes.csv](https://github.com/BHFDSC/CCU046_02/blob/main/phenotypes/phenotypes.csv). Codelists that the authors developed for this project are available in CSV format in the [CSV](https://github.com/BHFDSC/CCU046_02/tree/main/phenotypes/CSV) folder. A more detailed description of the phenotypes is provided below.

# Mental disorders

## ICD-10 codelists
For schizophrenia, bipolar disorder and depression we used codelists previously published by the authors in Jackson CA, Kerssens J, Fleetwood K, Smith DJ, Mercer SW, Wild SH. Incidence of ischaemic heart disease and stroke among people with psychiatric disorders: retrospective cohort study. The British Journal of Psychiatry. 2020;217(2):442-449. doi:10.1192/bjp.2019.250.

## SNOMED codelists
The General Practice Extraction Service Data for Pandemic Planning and Research (GDPPR) dataset includes a subset of all SNOMED codes used in the United Kingdom. The subset of codes included is defined by the primary care domain (PCD) reference set [1]. The PCD reference set is also used by the Quality and Outcomes Framework (QOF) and current and historical versions of the reference set are available from NHS England [2]. The available SNOMED codes are grouped into code clusters [1].

To develop our SNOMED codelists for mental disorders we extracted codes from all of the clusters related to relevant mental disorder diagnoses: DEPR_COD (depression diagnosis codes), DEPRES_COD (depression resolved codes) and MH_COD (psychosis and schizophrenia and bipolar affective disease codes) from version 47.1 of the QOF cluster list (which was the most recent version at the time of developing the SMI codelists) [3]. Additionally, we mapped existing Read v2 and Clinical Terms Version 3 codelists [4] for schizophrenia, bipolar disorder and depression to SNOMED, and identified codes available within the GDPPR dataset. We collated all SNOMED codes identified from the clusters, and from the mapping process and then identified codes for each of schizophrenia, bipolar disorder or depression.

# Medical history

In this study, some of our analyses adjusted for medical history. We used existing codelists to identify angina, diabetes and heart failure in the HES APC (ICD-10) and GDPPR (SNOMED) datasets. Specifically we used the following codelists from the HDR UK Phenotype Library:

* Angina: http://phenotypes.healthdatagateway.org/phenotypes/PH956/version/2134/detail/
* Diabetes: http://phenotypes.healthdatagateway.org/phenotypes/PH945/version/2123/detail/
* Heart failure: http://phenotypes.healthdatagateway.org/phenotypes/PH968/version/2146/detail/

The following sections describes how we developed codelists to identify cerebrovascular disease, chronic renal failure, myocardial infarction and percutaneous coronary intervention (PCI).

## Cerebrovascular disease

### ICD-10 codelist
We identified diagnoses using ICD-10 codes I60-I69 and G45.

### SNOMED codelist
We identified diagnoses using SNOMED codes in the haemorrhagic stroke (HSTRK_COD), non-haemorrhagic stroke (OSTR_COD), stroke diagnosis (STRK_COD) and transient ischaemic attack (TIA_COD) code clusters and additionally the SNOMED codes 195163003, 67992007 and 73192008. 

## Chronic renal failure

### ICD-10 codelist
We developed the codelist by identifying chronic renal failure codes from an existing chronic kidney disease codelist (http://phenotypes.healthdatagateway.org/phenotypes/PH950/version/2128/detail/) and existing end stage renal disease codelists (http://phenotypes.healthdatagateway.org/phenotypes/PH574/version/1148/detail/, https://github.com/rprigge-uoe/mltc-codelists/blob/master/ICD-10/ICD_ESRD.csv). 

### SNOMED codelist
We developed the codelist by identifying chronic renal failure codes from an existing chronic kidney disease codelist (http://phenotypes.healthdatagateway.org/phenotypes/PH950/version/2128/detail/) and the GDPPR code cluster for chronic kidney disease stage 4 and 5 (CKDATRISK1_COD). 

## Myocardial infarction

### ICD-10 codelist
We used a codelist previously published by the authors in Fleetwood, K., Wild, S.H., Smith, D.J. et al. Severe mental illness and mortality and coronary revascularisation following a myocardial infarction: a retrospective cohort study. BMC Med 19, 67 (2021). https://doi.org/10.1186/s12916-021-01937-2.

### SNOMED codelist
We adapted an existing MI SNOMED codelist (http://phenotypes.healthdatagateway.org/phenotypes/PH942/version/2120/detail/). We reviewed the codes from the existing codelist, excluding codes that were not relevant to our study and adding additional relevant codes from the CHD_COD (coronary heart disease codes) cluster from version 49.1 of the QOF cluster list (which was the most recent version at the time of developing the CVD codelists) (https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-collections/quality-and-outcomes-framework-qof/business-rules/quality-and-outcomes-framework-qof-business-rules-v49-2024-25).

## PCI

### OPCS-4 codelist
We identified PCI using the OPCS-4 procedure code K75, in line with previous studies from the authors: Fleetwood K, Wild SH, Smith DJ, Mercer SW, Licence K, Sudlow CLM, et al. Severe mental illness and mortality and coronary revascularisation following a myocardial infarction: a retrospective cohort study. BMC Med 2021;19:67. doi: https://doi.org/10.1186/s12916-021-01937-2

# HDR UK Phenotype Library
All of our codelists are also available via the HDR UK Phenotype Library:

* Schizophrenia: https://phenotypes.healthdatagateway.org/phenotypes/PH1718
* Bipolar disorder: https://phenotypes.healthdatagateway.org/phenotypes/PH1719
* Depression: https://phenotypes.healthdatagateway.org/phenotypes/PH1720
* Cerebrovascular disease: https://phenotypes.healthdatagateway.org/phenotypes/PH1791/
* Chronic renal failure: https://phenotypes.healthdatagateway.org/phenotypes/PH1792/
* Myocardial infarction: https://phenotypes.healthdatagateway.org/phenotypes/PH1722
* PCI: https://phenotypes.healthdatagateway.org/phenotypes/PH1793/version/3735/detail/


# References
1.	NHS Digital. General Practice Extraction Service (GPES) Data for Pandemic Planning and Research (GDPPR): a guide for analysts and users of the data. 2024. Available: https://digital.nhs.uk/coronavirus/gpes-data-for-pandemic-planning-and-research/guide-for-analysts-and-users-of-the-data (accessed 13 Jan 2025).
2.	NHS Digital. Quality and Outcomes Framework (QOF) business rules. 2024. Available: https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-collections/quality-and-outcomes-framework-qof/business-rules (accessed 13 Jan 2025)
3.	NHS Digital. Quality and Outcomes Framework (QOF) business rules v47.0 2022-2023. 2023. Available: https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-collections/quality-and-outcomes-framework-qof/quality-and-outcome-framework-qof-business-rules/quality-and-outcomes-framework-qof-business-rules-v47.0-2022-2023 (accessed 13 Jan 2025).
4.	Prigge R, Fleetwood KJ, Jackson CA, et al. Robustly Measuring Multiple Long-Term Health Conditions Using Disparate Linked Datasets in UK Biobank. Available at SSRN: https://ssrn.com/abstract=4863974.

