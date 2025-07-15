#
# Prepare data for receipt of care and mortality studies
#
# K. Fleetwood
# 

#
# 1. Set-up -------------------------------------------------------------------
#

## required packages
library(tidyverse)
library(DBI)
library(dbplyr)
library(dplyr)
library(janitor)
library(knitr)
library(arsenal)
library(lubridate)

ccu046_folder <- "~/collab/CCU046"
ccu046_02_folder <- file.path(ccu046_folder, "CCU046_02")
pd_folder <- file.path(ccu046_02_folder, "processed_data")
bc_folder <- file.path(ccu046_02_folder, "baseline_characteristics")

r_folder <- file.path(ccu046_folder, "r")
# Additional tableby functions to help with rounding to 5
source(file.path(r_folder, "tableby_functions_19Jan24.R"))  
# Additional functions to help with rounding to 5
source(file.path(r_folder, "output_functions_11Jul24.R")) 

# connect to databricks
con <- 
  dbConnect(
    odbc::odbc(),
    dsn = 'databricks',
    HTTPPath = 'sql/protocolv1/o/847064027862604/0622-162121-dts9kxvy',
    PWD = rstudioapi::askForPassword('Please enter Databricks PAT')
  )

#
# 2. Load data ---------------------------------------------------------------
#

# Load flow table 
flow_tbl <- 
  tbl(
    con, 
    in_schema("dsa_391419_j3w9t_collab", "ccu046_01_tmp_inc_exc_flow")
  )

flow <- flow_tbl %>% collect()

# Load cohort table
cohort_tbl <- 
  tbl(
    con, 
    in_schema("dsa_391419_j3w9t_collab", "ccu046_01_out_cohort_combined")
  )

cohort <- 
  cohort_tbl %>%
  collect() %>%
  rename_with(tolower)

#
# 3. Create flow diagram ------------------------------------------------------
#

# Check final row of flow diagram, matches number of rows in cohort
flow$n[flow$df_desc %in% "post exclusion of patients who failed the quality assurance"]
dim(cohort)

# Create version of flow table for output
flow_out <- 
  flow %>%
  arrange(indx) %>%
  filter(
    !df_desc %in% c("original")
  ) %>%
  dplyr::select(df_desc, n) %>%
  mutate(n = as.numeric(n))

# Identify inconsistent MI date and date of death
cohort <- 
  cohort %>%
  mutate(
    mal_mi_dod = !is.na(dod) & dod < mi_event_date
  )

sum_mal_mi_dod <- sum(cohort$mal_mi_dod)

# Count people not in GDPPR
not_in_gdppr <- sum(!cohort$mal_mi_dod & !cohort$in_gdppr)

# Post-hoc exclusion of people with MI date in March 2023
sum_mar_2023 <- 
  sum(
    cohort$in_gdppr & 
      !cohort$mal_mi_dod & 
      cohort$mi_event_date >= as.Date("2023-03-01") & 
      cohort$mi_event_date <= as.Date("2023-03-31")
  )

# Add additional exclusions to flow diagram
# round to nearest 5
flow_out <- 
  flow_out %>%
  filter(
    !df_desc %in% "post exclusion of patients not in GDPPR"
  ) %>%
  bind_rows(
    data.frame(
      df_desc = c("post exclusion of patients with date of death before mi date", "post exclusion of patients not in GDPPR", "post exlusion of patients with MI in March 2023"),
      n = c(nrow(cohort) - sum_mal_mi_dod, nrow(cohort) - sum_mal_mi_dod - not_in_gdppr, nrow(cohort) - not_in_gdppr - sum_mal_mi_dod - sum_mar_2023) 
    )
  ) %>%
  mutate(
    diff = lag(n) - n
  )

# Create flow_out_sum to collapse all QA checks

flow_out_sum <- 
  flow_out %>%
  slice(c(1, 5:7)) %>%
  mutate(
    df_desc = 
      case_when(
        df_desc %in% "post exclusion of patients with date of death before mi date" ~ "post exclusion of patients who failed any qa check",
        TRUE ~ df_desc
      )
  ) %>%
  dplyr::select(-diff) %>%
  mutate(
    diff = lag(n) - n
  )

flow_out <- 
  flow_out %>%
  mutate(
    n = 5*round(n/5),
    diff = 5*round(diff/5)
  )

flow_out_sum <- 
  flow_out_sum %>%
  mutate(
    n = 5*round(n/5),
    diff = 5*round(diff/5)
  )

write.csv(flow_out, file.path(bc_folder, "flow_out.csv"))
write.csv(flow_out_sum, file.path(bc_folder, "flow_out_sum.csv"))

#
# 4. Create cohort and define variables ---------------------------------------
#

# 4.1 Create cohort -----------------------------------------------------------

cohort <- 
  cohort %>%
  filter(
    in_gdppr == 1,
    !mal_mi_dod,
    mi_event_date <= as.Date("2023-02-28")
  ) %>%
  mutate(
    age_mi = as.numeric(mi_event_date - dob)/365.25,
    sex = ifelse(sex == 1, "Male", "Female")
  )

# 4.2 Define variables --------------------------------------------------------

# 4.2.1 Severe mental illness -------------------------------------------------

# Exclude SMI diagnoses on or after date of MI
cohort <- 
  cohort %>%
  mutate(
    sch_flag = ifelse(cov_hx_exp_schizophrenia_date < mi_event_date, 1, NA),
    bd_flag  = ifelse(cov_hx_exp_bipolar_disorder_date < mi_event_date, 1, NA),
    dep_flag = ifelse(cov_hx_exp_depression_date < mi_event_date, 1, NA),
    smi = 
      case_when(
        sch_flag %in% 1 ~ "Schizophrenia",
        bd_flag  %in% 1 ~ "Bipolar disorder",
        dep_flag %in% 1 ~ "Depression",
        TRUE            ~ "No SMI"
      ),
    smi = factor(smi, levels = c("Schizophrenia", "Bipolar disorder", "Depression", "No SMI"))
  )

# 4.2.2 Additional sociodemographic characteristics ---------------------------

cohort <- 
  cohort %>%
  mutate(
    ethnic_9 = 
      case_when(
        ethnic_desc %in% c("British (White)", "Irish (White)", "White", "Traveller", "Any other White background") ~ "White",
        ethnic_desc %in% c("White and Asian (Mixed)", "White and Black African (Mixed)", "White and Black Caribbean (Mixed)", "Any other Mixed background") ~ "Mixed",
        ethnic_desc %in% c("Indian (Asian or Asian British)") ~ "Indian",
        ethnic_desc %in% c("Pakistani (Asian or Asian British)", "Pakistani") ~ "Pakistani",
        ethnic_desc %in% c("Bangladeshi (Asian or Asian British)") ~ "Bangladeshi",
        ethnic_desc %in% c("Chinese (other ethnic group)") ~ "Chinese",
        ethnic_desc %in% c("Caribbean (Black or Black British)") ~ "Black Caribbean",
        ethnic_desc %in% c("African (Black or Black British)") ~ "Black African",
        ethnic_desc %in% c("Arab", "Any other Asian background", "Any other Black background", "Any other ethnic group") ~ "Any other ethnic group",
        ethnic_desc %in% "Not stated" ~ as.character(NA)
      ),
    ethnic_9 = factor(ethnic_9, levels = c("White", "Bangladeshi", "Black African", "Black Caribbean", "Chinese", "Indian", "Pakistani", "Mixed", "Any other ethnic group")),
    ethnic_5 = 
      case_when(
        ethnic_9 %in% "White" ~ "White",
        ethnic_9 %in% "Mixed" ~ "Mixed",
        ethnic_9 %in% c("Bangladeshi","Indian", "Pakistani") ~ "South Asian",
        (ethnic_9 %in% c("Black African", "Black Caribbean")| ethnic_desc %in% "Any other Black background") ~ "Black",
        ethnic_9 %in% c("Chinese", "Any other ethnic group") ~ "Any other ethnic group"
      ),
    ethnic_5 = factor(ethnic_5, levels = c("White", "Black", "South Asian", "Mixed", "Any other ethnic group")),
    imd_2019 = 
      case_when(
        imd_2019_deciles %in% 1 ~ "1 (most deprived)",
        imd_2019_deciles %in% 10 ~ "10 (least deprived)",
        TRUE ~ as.character(imd_2019_deciles)
      ),
    imd_2019 = factor(imd_2019, levels = c("1 (most deprived)", "2", "3", "4", "5", "6", "7", "8", "9", "10 (least deprived)"))
  )

# 4.2.3 Location (provider) ---------------------------------------------------

# Add a flag to identify whether procode3 is present or not
cohort <- 
  cohort %>%
  mutate(
    procode3_miss = ifelse(is.na(procode3), "Missing", "Available")
  )

# 4.3.4 Timing of admission (period, weekday/weekend, daytime/overnight) ------

# Divide time period into 4 month periods
cohort <- 
  cohort %>%
  mutate(
    period_4 = 
      case_when(
        # 2019
        mi_event_date >= as.Date("2019-11-01") & mi_event_date <= as.Date("2020-02-29") ~ "Nov 2019 -\nFeb 2020",
        # 2020
        mi_event_date >= as.Date("2020-03-01") & mi_event_date <= as.Date("2020-06-30") ~ "Mar 2020 -\nJun 2020",
        mi_event_date >= as.Date("2020-07-01") & mi_event_date <= as.Date("2020-10-31") ~ "Jul 2020 -\nOct 2020",
        mi_event_date >= as.Date("2020-11-01") & mi_event_date <= as.Date("2021-02-28") ~ "Nov 2020 -\nFeb 2021",
        # 2021
        mi_event_date >= as.Date("2021-03-01") & mi_event_date <= as.Date("2021-06-30") ~ "Mar 2021 -\nJun 2021",
        mi_event_date >= as.Date("2021-07-01") & mi_event_date <= as.Date("2021-10-31") ~ "Jul 2021 -\nOct 2021",
        mi_event_date >= as.Date("2021-11-01") & mi_event_date <= as.Date("2022-02-28") ~ "Nov 2021 -\nFeb 2022",
        # 2022
        mi_event_date >= as.Date("2022-03-01") & mi_event_date <= as.Date("2022-06-30") ~ "Mar 2022 -\nJun 2022",
        mi_event_date >= as.Date("2022-07-01") & mi_event_date <= as.Date("2022-10-31") ~ "Jul 2022 -\nOct 2022",
        mi_event_date >= as.Date("2022-11-01") & mi_event_date <= as.Date("2023-02-28") ~ "Nov 2022 -\nFeb 2023",
      ),
    period_4 = 
      factor(
        period_4, 
        levels = c(
          "Nov 2019 -\nFeb 2020", 
          "Mar 2020 -\nJun 2020", "Jul 2020 -\nOct 2020", "Nov 2020 -\nFeb 2021", 
          "Mar 2021 -\nJun 2021", "Jul 2021 -\nOct 2021", "Nov 2021 -\nFeb 2022",
          "Mar 2022 -\nJun 2022", "Jul 2022 -\nOct 2022", "Nov 2022 -\nFeb 2023"
        )
      )
  )    

cohort <- 
  cohort %>%
  mutate(
    mi_event_year = as.factor(year(mi_event_date)),
    mi_on = ifelse(overnight %in% 1, "Overnight", "Daytime"),
    mi_wd = wday(mi_event_date, label = TRUE),
    mi_wd = 
      case_when(
        mi_wd %in% c("Sat", "Sun") ~ "Weekend",
        TRUE ~ "Week day"
      )
  )

# 4.2.5 Lifestyle factors (smoking and BMI) -----------------------------------

# As per previous projects exclude BMIs < 15 and > 70 (very few outside of this range)
cohort <- 
  cohort %>%
  mutate(
    bmi = 
      case_when(
        !is.na(minap_bmi) & (minap_bmi < 15 | minap_bmi > 70) ~ as.numeric(NA), 
        TRUE ~ minap_bmi
      ),
    bmi_cat = 
      case_when(
        is.na(bmi)               ~ as.character(NA),
        bmi < 18.5 ~ "<18.5",
        bmi >= 18.5 & bmi < 25   ~ "18.5 - 25",
        bmi >= 25   & bmi < 30   ~ "25 - 30",
        bmi >= 30   & bmi < 40   ~ "30 - 40",
        bmi >= 40                ~ ">=40" 
      ),
    bmi_cat = factor(bmi_cat, levels = c("<18.5", "18.5 - 25", "25 - 30", "30 - 40", ">=40")),
    smoking_cat = 
      case_when(
        is.na(smoking_status)|smoking_status %in% "9. Unknown" ~ as.character(NA),
        TRUE ~ substr(smoking_status, 4, 100),
      ),
    smoking_cat = factor(smoking_cat, levels = c("Never smoked", "Non smoker - smoking history unknown", "Ex smoker", "Current smoker"))
  )

# 4.2.6 Existing CVD (previous MI, angina) ------------------------------------

cohort <- 
  cohort %>%
  mutate(
    comorb_angina = (cov_hx_com_angina_flag %in% 1 | previous_angina %in% "1. Yes")
  )

# Investigate timing of previous MI
cohort <- 
  cohort %>%
  mutate(
    mi_diff = as.numeric(mi_event_date - cov_hx_com_ami_date)
  )
summary(cohort$mi_diff)
cohort %>% tabyl(mi_diff)

# Investigate different cut-offs for previous_mi
cohort <- 
  cohort %>%
  mutate(
    comorb_mi_0  = cov_hx_com_ami_flag %in% 1 & mi_diff > 0,
    comorb_mi_1  = cov_hx_com_ami_flag %in% 1 & mi_diff > 1,
    comorb_mi_3  = cov_hx_com_ami_flag %in% 1 & mi_diff > 3,
    comorb_mi_5  = cov_hx_com_ami_flag %in% 1 & mi_diff > 5,
    comorb_mi_7  = cov_hx_com_ami_flag %in% 1 & mi_diff > 7,
    comorb_mi_10 = cov_hx_com_ami_flag %in% 1 & mi_diff > 10,
    comorb_mi_14 = cov_hx_com_ami_flag %in% 1 & mi_diff > 14,
    comorb_mi_30 = cov_hx_com_ami_flag %in% 1 & mi_diff > 30
  )

days0  <- cohort %>% tabyl(comorb_mi_0,  mi_type) %>% adorn_percentages("col") %>% adorn_pct_formatting() %>% filter(comorb_mi_0)  %>% mutate("Days" =  0) %>% select("Days", "NSTEMI", "STEMI")
days1  <- cohort %>% tabyl(comorb_mi_1,  mi_type) %>% adorn_percentages("col") %>% adorn_pct_formatting() %>% filter(comorb_mi_1)  %>% mutate("Days" =  1) %>% select("Days", "NSTEMI", "STEMI")
days3  <- cohort %>% tabyl(comorb_mi_3,  mi_type) %>% adorn_percentages("col") %>% adorn_pct_formatting() %>% filter(comorb_mi_3)  %>% mutate("Days" =  3) %>% select("Days", "NSTEMI", "STEMI")
days5  <- cohort %>% tabyl(comorb_mi_5,  mi_type) %>% adorn_percentages("col") %>% adorn_pct_formatting() %>% filter(comorb_mi_5)  %>% mutate("Days" =  5) %>% select("Days", "NSTEMI", "STEMI")
days7  <- cohort %>% tabyl(comorb_mi_7,  mi_type) %>% adorn_percentages("col") %>% adorn_pct_formatting() %>% filter(comorb_mi_7)  %>% mutate("Days" =  7) %>% select("Days", "NSTEMI", "STEMI")
days10 <- cohort %>% tabyl(comorb_mi_10, mi_type) %>% adorn_percentages("col") %>% adorn_pct_formatting() %>% filter(comorb_mi_10) %>% mutate("Days" = 10) %>% select("Days", "NSTEMI", "STEMI")
days14 <- cohort %>% tabyl(comorb_mi_14, mi_type) %>% adorn_percentages("col") %>% adorn_pct_formatting() %>% filter(comorb_mi_14) %>% mutate("Days" = 14) %>% select("Days", "NSTEMI", "STEMI")
days30 <- cohort %>% tabyl(comorb_mi_30, mi_type) %>% adorn_percentages("col") %>% adorn_pct_formatting() %>% filter(comorb_mi_30) %>% mutate("Days" = 30) %>% select("Days", "NSTEMI", "STEMI")

previous_mi_days <- 
  bind_rows(
    days0, days1, days3, days5, days7, days10, days14, days30
  )

# Agreed with CJ 28/10/24 via email to use a window of 3 days
cohort <- 
  cohort %>%
  mutate(
    comorb_mi = comorb_mi_3
  ) %>%
  select(
    -comorb_mi_0, -comorb_mi_1, -comorb_mi_3, -comorb_mi_5, -comorb_mi_7,
    -comorb_mi_10, -comorb_mi_14, -comorb_mi_30
  )

# 4.2.7 Previous PCI ----------------------------------------------------------

cohort <- 
  cohort %>%
  mutate(
    hx_pci = cov_hx_com_pci_flag %in% 1 | previous_pci %in% "1. Yes"
  )

# 4.2.8 Comorbidities ---------------------------------------------------------
# - chronic cardiac failure, diabetes, chronic renal failure, 
#   cerebrovascular disease

has_diabetes <- 
  c(
    "1. Diabetes (dietary control)",
    "2. Diabetes (oral medicine)",
    "3. Diabetes (insulin)",
    "5. Insulin plus oral medication"
  )

cohort <- 
  cohort %>%
  mutate(
    # Combine MINAP data and HES data
    comorb_hf = (cov_hx_com_hf_flag %in% 1 | heart_failure %in% "1. Yes"), 
    comorb_diab = (cov_hx_com_diabetes_flag %in% 1| diabetes %in% has_diabetes),
    comorb_crf = (cov_hx_com_esrd_flag %in% 1|chronic_renal_failure %in% "1. Yes"), 
    comorb_cevd = (cov_hx_com_cevd_flag %in% 1|cerebrovascular_disease %in% "1. Yes"),
  )

# 4.2.9 MI presentation -------------------------------------------------------
# - cardiogenic shock, ST-segment changes, cardiac arrest, troponin elevation,
#   COVID-19 status, creatinine, heart rate and SBP

cohort <- 
  cohort %>%
  mutate(
    # treat 'not applicable' as missing
    cardiogenic_shock = 
      factor(cardiogenic_shock, levels = c("Yes", "No")),
    ecg = 
      case_when(
        is.na(ecg_determining_treatment)|ecg_determining_treatment %in% "Unknown" ~ as.character(NA),
        TRUE ~ substr(ecg_determining_treatment, 4, 100)
      ),
    ecg = factor(ecg, levels = c("No acute changes", "ST segment elevation", "Left bundle branch block", "ST segment depression", "T wave changes only", "Other acute abnormality")),
    covid = exp_covid_flag %in% 1
  )

# Creatinine, heart rate and SBP
cohort <- 
  cohort %>%
  mutate(
    creatinine_num = as.numeric(creatinine),
    systolic_bp_num = as.numeric(systolic_bp),
    heart_rate_num = as.numeric(heart_rate)
  )

cohort <- 
  cohort %>%
  mutate(
    creatinine = as.numeric(creatinine),
    sbp = as.numeric(systolic_bp),
    heart_rate = as.numeric(heart_rate)
  )

# MINAP defines the following limits
# - creatinine (micromol/L): 30-1000
# - SBP (mmHg): 50-250
# - heart rate (bpm): 30-180

summary(cohort$creatinine)
sum(!is.na(cohort$creatinine))
sum(!is.na(cohort$creatinine) & cohort$creatinine < 30) 
sum(!is.na(cohort$creatinine) & cohort$creatinine > 1000)

summary(cohort$sbp)
sum(!is.na(cohort$sbp))
sum(!is.na(cohort$sbp) & cohort$sbp < 50)
sum(!is.na(cohort$sbp) & cohort$sbp > 250) 

summary(cohort$heart_rate)
sum(!is.na(cohort$heart_rate))
sum(!is.na(cohort$heart_rate) & cohort$heart_rate < 30)
sum(!is.na(cohort$heart_rate) & cohort$heart_rate > 180)

# Remove unneeded columns
# Incorporate upper and lower limits for creatinine, SBP and HR
cohort <- 
  cohort %>%
  select(-systolic_bp, -creatinine_num, -systolic_bp_num, -heart_rate_num) %>%
  mutate(
    creatinine = ifelse(!is.na(creatinine) & creatinine >= 30 & creatinine <= 1000, creatinine, NA),
    sbp = ifelse(!is.na(sbp) & sbp >= 50 & sbp<= 250, sbp, NA),
    heart_rate = ifelse(!is.na(heart_rate) & heart_rate >= 30 & heart_rate <= 180, heart_rate, NA),
  )
summary(cohort$creatinine)
summary(cohort$sbp)
summary(cohort$heart_rate)

# 4.2.10 Add discharge destination --------------------------------------------
cohort <- 
  cohort %>%
  mutate(
    discharge_destination = 
      case_when(
        is.na(discharge_destination)|discharge_destination %in% "9. Unknown" ~ as.character(NA),
        TRUE ~ substring(discharge_destination, 4, 100)
      )
  )

# 4.2.11 NSTEMI care covariates -----------------------------------------------
# - Derive combined angiogram variable
# - Double checked derivation 18/10/2024
cohort <- 
  cohort %>%
  mutate(
    angiogram = 
      case_when(
        angio_within_72h %in% "Yes" ~ "Received within 72 hours",
        angiogram_receipt %in% "Yes" & angio_within_72h %in% "No" ~ "Received outwith 72 hours",
        angiogram_receipt %in% "Yes" ~ "Received, timing unknown",
        angiogram_eligibility %in% "Yes" & angiogram_receipt %in% c("No", "Patient refused") ~ "Eligible but not received",
        angiogram_eligibility %in% "Yes" & (is.na(angiogram_receipt)|angiogram_receipt %in% "Not applicable") ~ "Eligible, receipt unknown",
        angiogram_eligibility %in% "No" ~ "Not eligible", 
        TRUE ~ as.character(NA)
      ),
    prev_all = 
      case_when(
        prev_all %in% 1 ~ "Yes",
        prev_all %in% 0 ~ "No",
        TRUE            ~ as.character(NA)
      ),
    prev_all = factor(prev_all, levels = c("Yes", "No")),
    cardiac_rehab = 
      case_when(
        is.na(cardiac_rehab) | cardiac_rehab %in% "9. Unknown" ~ as.character(NA),
        TRUE ~ substring(cardiac_rehab, 4, 100)
      )
  )

# 4.2.12 STEMI care covariates ------------------------------------------------
# Call to balloon
# - Convert from seconds to minutes
# - Exclude values less than zero or greater than 365 days (31536000 seconds)
# - Define call to balloon summary variable
# Door to balloon
# - Error in derivation: door_to_balloon is actually call to door
# - Convert from seconds to minutes
# - Exclude values less than zero or greater than 365 days (31536000 seconds)
# - Define door to balloon summary variable
cohort <- 
  cohort %>%
  mutate(
    ctb_min = 
      case_when(
        call_to_balloon < 0        ~ as.numeric(NA),
        call_to_balloon > 31536000 ~ as.numeric(NA),
        TRUE ~ call_to_balloon/60
      ),
    ctb_sum = 
      case_when(
        ctb_min < 120 ~ "Call to balloon less than 120 minutes",
        ctb_min >= 120 & ctb_min < 150 ~ "Call to balloon between 120 and 150 minutes",
        ctb_min >= 150 ~ "Call to balloon greater than or equal to 150 mins",
        initial_reperfusion_treatment %in% c("2. pPCI in house", "4. pPCI already was performed at the interventional hospital") ~ "Received pPCI but CTB time unknown",
        initial_reperfusion_treatment %in% c("0. None", "1. Thrombolytic treatment", "3. Referred for consideration for pPCI elsewhere") ~ "Did not receive pPCI",
        TRUE ~ as.character(NA)
      ),
    door_to_balloon = call_to_balloon - door_to_balloon,
    dtb_min = 
      case_when(
        door_to_balloon < 0        ~ as.numeric(NA),
        door_to_balloon > 31536000 ~ as.numeric(NA),
        TRUE ~ door_to_balloon/60
      ),
    dtb_sum = 
      case_when(
        dtb_min < 60 ~ "Door to balloon less than 60 minutes",
        dtb_min >= 60 & dtb_min < 90 ~ "Door to balloon between 60 and 90 minutes",
        dtb_min >= 90 ~ "Door to balloon greater than or equal to 90 mins",
        initial_reperfusion_treatment %in% c("2. pPCI in house", "4. pPCI already was performed at the interventional hospital") ~ "Received pPCI but DTB time unknown",
        initial_reperfusion_treatment %in% c("0. None", "1. Thrombolytic treatment", "3. Referred for consideration for pPCI elsewhere") ~ "Did not receive pPCI",
        TRUE ~ as.character(NA)
      )
  )

# 4.2.13 Outcomes ------------------------------------------------------------- 

# Define outcomes
# - mortality in 30 days
# - one-year all cause mortality
# - one-year CVD mortality
# - time to all-cause mortality within the first year
# - time to CVD mortality within the first year

# Cause of death codes
cohort <- 
  cohort %>%
  rename(
    cod = s_cod_code_underlying
  ) %>%
  mutate(
    cod_3 = substring(cod, 1, 3),
    cod_miss = is.na(cod)
  )

table(is.na(cohort$dod), is.na(cohort$cod))

cohort <- 
  cohort %>%
  mutate(
    days_to_death = as.numeric(dod - mi_event_date),
    mort_30 = !is.na(dod) & days_to_death <= 30,
    mort_yr = !is.na(dod) & days_to_death <= 365,
    mort_yr_days = 
      case_when(
        is.na(dod)|days_to_death > 365 ~ 365,
        !is.na(dod) & days_to_death <= 365 ~ days_to_death
      )
  )

# Top 10 causes of death: 30-day mortality ---
cohort %>% 
  filter(mort_30) %>%
  tabyl(cod_3) %>%
  arrange(desc(n)) %>%
  head(10)

# Check whether this differs by STEMI/NSTEMI
cohort %>% 
  filter(mort_30, mi_type %in% "NSTEMI") %>%
  tabyl(cod_3) %>%
  arrange(desc(n)) %>%
  head(10)

# Check whether this differ by STEMI/NSTEMI
cohort %>% 
  filter(mort_30, mi_type %in% "STEMI") %>%
  tabyl(cod_3) %>%
  arrange(desc(n)) %>%
  head(10) %>% pull(cod_3)

# Top 10 causes of death: 1-year mortality ---
cohort %>% 
  filter(mort_yr) %>%
  tabyl(cod_3) %>%
  arrange(desc(n)) %>%
  head(10)

# Check NSTEMI
cohort %>% 
  filter(mort_yr, mi_type %in% "NSTEMI") %>%
  tabyl(cod_3) %>%
  arrange(desc(n)) %>%
  head(10) 

# Check STEMI
cohort %>% 
  filter(mort_yr, mi_type %in% "STEMI") %>%
  tabyl(cod_3) %>%
  arrange(desc(n)) %>%
  head(10)

# Check U07 codes
# U07.0 is vaping-related disorder
# U07.1 and U07.2 are COVID, and suspected COVID
cohort %>%
  filter(cod_3 %in% "U07") %>%
  tabyl(cod)

# Label CVD death, and top 10 causes of death
cvd_icd10 <- 
  c(
    paste0("I", c(20:25, 60:69)), 
    "G45"
  )

cohort <- 
  cohort %>%
  mutate(
    cod_cvd = 
      case_when(
        !is.na(dod) & cod_3 %in% cvd_icd10 ~ "CVD",
        !is.na(dod) & !is.na(cod_3) ~ "Other cause",
        !is.na(dod) & is.na(cod_3) ~ "Cause unknown"
      ),
    cod_top10_mort_yr_nstemi = 
      case_when(
        !is.na(dod) & cod_3 %in% "I21" ~ "Acute myocardial infarction",
        !is.na(dod) & cod_3 %in% "I25" ~ "Chronic ischaemic heart disease",
        !is.na(dod) & cod %in% c("U071", "U072")  ~ "COVID-19",
        !is.na(dod) & cod_3 %in% "J44" ~ "Other COPD",
        !is.na(dod) & cod_3 %in% "J18" ~ "Pneumonia, organism unspecified",
        !is.na(dod) & cod_3 %in% "C34" ~ "Malignant neoplasm of bronchus and lung",
        !is.na(dod) & cod_3 %in% "F03" ~ "Unspecified dementia",
        !is.na(dod) & cod_3 %in% "I35" ~ "Non rheumatic aortic valve disorders",
        !is.na(dod) & cod_3 %in% "I50" ~ "Heart failure",
        !is.na(dod) & cod_3 %in% "I64" ~ "Stroke, not specified as haemorrhage or infarction",
        !is.na(dod) & cod_3 %in% cvd_icd10 ~ "Other CVD",
        !is.na(dod) & !is.na(cod_3) ~ "Other cause",
        !is.na(dod) & is.na(cod_3) ~ "Cause unknown"
      ),
    cod_top10_mort_yr_nstemi = 
      factor(
        cod_top10_mort_yr_nstemi,
        levels = 
          c("Acute myocardial infarction", "Chronic ischaemic heart disease",
            "COVID-19", "Pneumonia, organism unspecified", "Other COPD",
            "Non rheumatic aortic valve disorders", 
            "Malignant neoplasm of bronchus and lung", "Unspecified dementia",
            "Heart failure", 
            "Stroke, not specified as haemorrhage or infarction", "Other CVD",
            "Other cause", "Cause unknown")
      ),
    cod_top10_mort_yr_stemi = 
      case_when(
        !is.na(dod) & cod_3 %in% "I21" ~ "Acute myocardial infarction",
        !is.na(dod) & cod_3 %in% "I25" ~ "Chronic ischaemic heart disease",
        !is.na(dod) & cod %in% c("U071", "U072")  ~ "COVID-19",
        !is.na(dod) & cod_3 %in% "J44" ~ "Other COPD",
        !is.na(dod) & cod_3 %in% "J18" ~ "Pneumonia, organism unspecified",
        !is.na(dod) & cod_3 %in% "C34" ~ "Malignant neoplasm of bronchus and lung",
        !is.na(dod) & cod_3 %in% "F03" ~ "Unspecified dementia",
        !is.na(dod) & cod_3 %in% "I35" ~ "Non rheumatic aortic valve disorders",
        !is.na(dod) & cod_3 %in% "I64" ~ "Stroke, not specified as haemorrhage or infarction",
        !is.na(dod) & cod_3 %in% "E11" ~ "Type 2 diabetes mellitus",
        !is.na(dod) & cod_3 %in% cvd_icd10 ~ "Other CVD",
        !is.na(dod) & !is.na(cod_3) ~ "Other cause",
        !is.na(dod) & is.na(cod_3) ~ "Cause unknown"
      ),
    cod_top10_mort_yr_stemi = 
      factor(
        cod_top10_mort_yr_stemi,
        levels = 
          c("Acute myocardial infarction", "Chronic ischaemic heart disease",
            "COVID-19", "Pneumonia, organism unspecified",
            "Malignant neoplasm of bronchus and lung", "Other COPD",
            "Stroke, not specified as haemorrhage or infarction",
            "Unspecified dementia","Non rheumatic aortic valve disorders",
            "Type 2 diabetes mellitus", "Other CVD", "Other cause",
            "Cause unknown"
            )
      ),
    cod_top10_mort30_nstemi =
      case_when(
        !is.na(dod) & cod_3 %in% "I21" ~ "Acute myocardial infarction",
        !is.na(dod) & cod_3 %in% "I25" ~ "Chronic ischaemic heart disease",
        !is.na(dod) & cod %in% c("U071", "U072")  ~ "COVID-19",
        !is.na(dod) & cod_3 %in% "J44" ~ "Other COPD",
        !is.na(dod) & cod_3 %in% "J18" ~ "Pneumonia, organism unspecified",
        !is.na(dod) & cod_3 %in% "C34" ~ "Malignant neoplasm of bronchus and lung",
        !is.na(dod) & cod_3 %in% "I35" ~ "Non rheumatic aortic valve disorders",
        !is.na(dod) & cod_3 %in% "I64" ~ "Stroke, not specified as haemorrhage or infarction",
        !is.na(dod) & cod_3 %in% "I24" ~ "Other acute ischaemic heart disease",
        !is.na(dod) & cod_3 %in% "E11" ~ "Type 2 diabetes mellitus",
        !is.na(dod) & cod_3 %in% cvd_icd10 ~ "Other CVD",
        !is.na(dod) & !is.na(cod_3) ~ "Other cause",
        !is.na(dod) & is.na(cod_3) ~ "Cause unknown"
      ),
    cod_top10_mort30_nstemi = 
      factor(
        cod_top10_mort30_nstemi,
        levels = 
          c("Acute myocardial infarction", "Chronic ischaemic heart disease",
        "COVID-19", "Pneumonia, organism unspecified", 
        "Non rheumatic aortic valve disorders", "Other COPD", 
        "Other acute ischaemic heart disease",
        "Malignant neoplasm of bronchus and lung", "Type 2 diabetes mellitus", 
        "Stroke, not specified as haemorrhage or infarction", "Other CVD", 
        "Other cause", "Cause unknown")
      ),
    cod_top10_mort30_stemi =
      case_when(
        !is.na(dod) & cod_3 %in% "I21" ~ "Acute myocardial infarction",
        !is.na(dod) & cod_3 %in% "I25" ~ "Chronic ischaemic heart disease",
        !is.na(dod) & cod %in% c("U071", "U072")  ~ "COVID-19",
        !is.na(dod) & cod_3 %in% "J44" ~ "Other COPD",
        !is.na(dod) & cod_3 %in% "J18" ~ "Pneumonia, organism unspecified",
        !is.na(dod) & cod_3 %in% "C34" ~ "Malignant neoplasm of bronchus and lung",
        !is.na(dod) & cod_3 %in% "I35" ~ "Non rheumatic aortic valve disorders",
        !is.na(dod) & cod_3 %in% "I64" ~ "Stroke, not specified as haemorrhage or infarction",
        !is.na(dod) & cod_3 %in% "E14" ~ "Unspecified diabetes mellitus",
        !is.na(dod) & cod_3 %in% "E11" ~ "Type 2 diabetes mellitus",
        !is.na(dod) & cod_3 %in% cvd_icd10 ~ "Other CVD",
        !is.na(dod) & !is.na(cod_3) ~ "Other cause",
        !is.na(dod) & is.na(cod_3) ~ "Cause unknown"
      ),
    cod_top10_mort30_stemi =
      factor(
        cod_top10_mort30_stemi,
        levels = c("Acute myocardial infarction", "Chronic ischaemic heart disease",
                   "COVID-19", "Pneumonia, organism unspecified", "Other COPD",
                   "Malignant neoplasm of bronchus and lung", 
                   "Non rheumatic aortic valve disorders", 
                   "Unspecified diabetes mellitus", "Type 2 diabetes mellitus",
                   "Stroke, not specified as haemorrhage or infarction",
                   "Other CVD", "Other cause", "Cause unknown")
      ),
    mort_cvd_yr = !is.na(dod) & days_to_death <= 365 & cod_cvd %in% "CVD",
    cod_cvd_yr = 
      case_when(
        !is.na(dod) & cod_3 %in% cvd_icd10 & days_to_death <= 365 ~ "Died from CVD",
        !is.na(dod) & !is.na(cod_3) & days_to_death <= 365 ~ "Died from other cause",
        !is.na(dod) & is.na(cod_3) & days_to_death <= 365 ~ "Died, cause unknown",
        is.na(dod) | days_to_death > 365 ~ "Alive"
      )
  )

# 4.2.14 Add labels -----------------------------------------------------------

labels(cohort) <-
  c(
    sex = "Sex",
    age_mi = "Age at MI (years)",
    ethnic_3 = "Ethnicity",
    ethnic_5 = "Ethnicity",
    imd_2019 = "Area based deprivation (IMD quintile)",
    # Comorbidities
    comorb_mi = "Previous MI",
    comorb_hf = "Heart failure", 
    comorb_angina = "Angina",
    comorb_diab = "Diabetes",
    comorb_crf = "Chronic renal failure", 
    comorb_cevd = "Cerebrovascular disease",
    hx_pci = "Previous PCI", 
    covid = "Covid-19 status",
    # MI characteristics
    mi_on = "Admission timing",
    mi_wd = "Admission day",
    mi_event_year = "Year of MI",
    procode3_miss = "NHS trust",
    bmi_cat = "BMI (kg/m2)",
    smoking_cat = "Smoking status",
    cardiac_arrest = "Cardiac arrest",
    cardiac_enzymes_elevated = "Cardiac enzymes elevated",
    cardiogenic_shock = "Cardiogenic shock",
    ecg = "ECG determining treatment",
    creatinine = "Creatinine (micromol/L)",
    sbp = "SBP (mmHg)",
    heart_rate = "Heart rate (bpm)",
    discharge_destination = "Discharge destination",
    # NSTEMI care
    prev_all = "Discharged on all secondary prevention drugs for which they were judged to be eligible",
    angiogram = "Angiogram",
    admission_to_cardiac_ward = "Admission to cardiac ward",
    cardiac_rehab = "Referred for cardiac rehabilitation",
    # STEMI care
    ctb_sum = "Call to balloon",
    dtb_sum = "Door to balloon",
    # Outcomes
    mort_30 = "Mortality within 30 days",
    mort_yr = "Mortality within 1 year",
    cod_cvd_yr = "CVD mortality within 1 year",
    cod_top10_mort_yr_nstemi = "Top 10 causes of death within 1 year",
    cod_top10_mort_yr_stemi = "Top 10 causes of death within 1 year",
    cod_top10_mort30_nstemi = "Top 10 causes of death within 30 days",
    cod_top10_mort30_stemi = "Top 10 causes of death within 30 days"
  )

#
# 5. Output results -----------------------------------------------------------
#

saveRDS(flow_out,         file = file.path(pd_folder, "01_flow_out.rds"))
saveRDS(previous_mi_days, file = file.path(pd_folder, "01_prev_mi_days.rds"))
saveRDS(cohort,           file = file.path(pd_folder, "01_cohort.rds"))
