#
# NSTEMI: 1-year mortality (amongst people discharged home): Key models 
# - (complete cases on demographics, timing, co-morbidities and care only)
#
# K. Fleetwood
# 14 Jan 2025: Adapted from 11b
#

#
# 1. Set-up -------------------------------------------------------------------
#

## required packages
library(tidyverse)
library(dplyr)
library(janitor)
library(knitr)
library(arsenal)
library(lubridate)
library(lme4)
library(lattice)

library(performance) # includes function for checking co-linearity

ccu046_folder <- "~/collab/CCU046"
ccu046_02_folder <- file.path(ccu046_folder, "CCU046_02")
pd_folder <- file.path(ccu046_02_folder, "processed_data")
outcome_folder <- file.path(ccu046_02_folder, "processed_data_nstemi_1yr_home")

r_folder <- file.path(ccu046_folder, "r")

# Additional functions to help with summarising models
source(file.path(r_folder, "model_summary_functions.R"))  

#
# 2. Load data ----------------------------------------------------------------
#

cohort <- readRDS(file = file.path(pd_folder, "01_cohort.rds"))

# Subset cohort to NSTEMIs only
# Factor covariates for modelling
# Select cc
angio_lvls <- 
  c("Received within 72 hours", "Not eligible", "Eligible but not received",
    "Received, timing unknown" , "Received outwith 72 hours")
rehab_lvls <- 
  c("Yes", "No", "Not indicated", "Patient declined")

cohort <- 
  cohort %>% 
  filter(
    mi_type %in% "NSTEMI",
    discharge_destination %in% "Home"
  ) %>%
  mutate(
    smi = 
      factor(
        smi, 
        levels = c("No SMI", "Schizophrenia", "Bipolar disorder", "Depression")
      ),
    sex = factor(sex, levels = c("Male", "Female")),
    imd_2019 = 
      factor(
        imd_2019, 
        levels = 
          c("10 (least deprived)", "1 (most deprived)", "2", "3", "4", "5", "6",
            "7", "8", "9")
      ),
    angiogram = factor(angiogram, levels = angio_lvls),
    admission_to_cardiac_ward = 
      case_when(
        admission_to_cardiac_ward %in% "Died_in_A&E" ~ "No",
        TRUE ~ admission_to_cardiac_ward
      ),
    cardiac_rehab = factor(cardiac_rehab, levels = rehab_lvls),
    cc = !is.na(imd_2019)  & !is.na(ethnic_5)                  & !is.na(procode3) & 
      !is.na(angiogram) & !is.na(admission_to_cardiac_ward) & !is.na(prev_all) &
      !is.na(cardiac_rehab)
  ) %>%
  filter(cc)

age_mi_mean <- mean(cohort$age_mi)
age_mi_sd <- sd(cohort$age_mi)

cohort <- 
  cohort %>%
  mutate(
    age_mi_scl     = (age_mi - age_mi_mean)/age_mi_sd
  )

# 4. Key models ---------------------------------------------------------------

# 4.1 Model 1 -----------------------------------------------------------------

# Mixed effects logistic regression model on complete cases
# Adjust for age and sex

# Takes approximately 1 minute
mod_1_start <- Sys.time()
mod_1 <-
  glmer(
    mort_yr ~ smi + age_mi_scl + I(age_mi_scl^2) + sex  + (1|procode3),
    family = binomial(link = "logit"),
    data = cohort,
    control = glmerControl(optimizer = "bobyqa")
  )
mod_1_finish <- Sys.time()
mod_1_time <- mod_1_finish - mod_1_start

# Summarise results
mod_1_out <- glmer_out(mod_1, prefix = "mod1_")

# 4.2 Model 2 -----------------------------------------------------------------
# Adjust for age, sex, IMD, ethnicity

# Took 7 minutes to run
mod_2_start <- Sys.time()
mod_2 <-
  glmer(
    mort_yr ~ smi + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5  + (1|procode3),
    family = binomial(link = "logit"),
    data = cohort,
    control = glmerControl(optimizer = "bobyqa")
  )
mod_2_finish <- Sys.time()
mod_2_time <- mod_2_finish - mod_2_start

# Summarise results
mod_2_out <- glmer_out(mod_2, prefix = "mod2_")

# 4.3 Model 3 -----------------------------------------------------------------

# Takes 11 minutes to run
mod_3_start <- Sys.time()
mod_3 <-
  glmer(
    mort_yr ~ smi + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5 +
      period_4 + mi_wd + mi_on + (1|procode3),
    family = binomial(link = "logit"),
    data = cohort,
    control = glmerControl(optimizer = "bobyqa", optCtrl = list(maxfun = 20000))
  )
mod_3_finish <- Sys.time()
mod_3_time <- mod_3_finish - mod_3_start

# Summarise results
mod_3_out <- glmer_out(mod_3, prefix = "mod3_")


# 4.4 Model 4 -----------------------------------------------------------------

# Sociodemographics, timing and comorbidities

# Takes 20 mins to run
mod_4_start <- Sys.time()
mod_4 <-
  glmer(
    mort_yr ~ smi + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5 +
      period_4 + mi_wd + mi_on + comorb_angina + comorb_mi +
      hx_pci +
      comorb_hf +  comorb_diab + 
      comorb_crf + comorb_cevd + (1|procode3),
    family = binomial(link = "logit"),
    data = cohort,
    control = glmerControl(optimizer = "bobyqa", optCtrl = list(maxfun = 20000))
  )
mod_4_finish <- Sys.time()
mod_4_time <- mod_4_finish - mod_4_start

# Summarise results
mod_4_out <- glmer_out(mod_4, prefix = "mod4_")

# 4.5 Model 4 + care -----------------------------------------------------------

# Sociodemographics, timing, comorbidities, care

mod_4_care_start <- Sys.time()
mod_4_care <-
  glmer(
    mort_yr ~ smi + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5 +
      period_4 + mi_wd + mi_on + comorb_angina + comorb_mi +
      hx_pci +
      comorb_hf +  comorb_diab + 
      comorb_crf + comorb_cevd + 
      gram + admission_to_cardiac_ward + prev_all + cardiac_rehab +
      (1|procode3),
    family = binomial(link = "logit"),
    data = cohort,
    control = glmerControl(optimizer = "bobyqa", optCtrl = list(maxfun = 30000))
  )
mod_4_care_finish <- Sys.time()
mod_4_care_time <- mod_4_care_finish - mod_4_care_start

# Summarise results
mod_4_care_out <- glmer_out(mod_4_care, prefix = "mod4_care_")

# 4.5a Model 4 + care (sequential) --------------------------------------------

# 4.5a.1 Model 4 + angiogram --------------------------------------------------

mod_4_angio_start <- Sys.time()
mod_4_angio <-
  glmer(
    mort_yr ~ smi + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5 +
      period_4 + mi_wd + mi_on + comorb_angina + comorb_mi +
      hx_pci +
      comorb_hf +  comorb_diab + 
      comorb_crf + comorb_cevd + 
      angiogram + 
      (1|procode3),
    family = binomial(link = "logit"),
    data = cohort,
    control = glmerControl(optimizer = "bobyqa", optCtrl = list(maxfun = 30000))
  )
mod_4_angio_finish <- Sys.time()
mod_4_angio_time <- mod_4_angio_finish - mod_4_angio_start

# Summarise results
mod_4_angio_out <- glmer_out(mod_4_angio, prefix = "mod4_angio_")

# 4.5a.2 Model 4 + angiogram + rehab ------------------------------------------

mod_4_ang_reh_start <- Sys.time()
mod_4_ang_reh <-
  glmer(
    mort_yr ~ smi + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5 +
      period_4 + mi_wd + mi_on + comorb_angina + comorb_mi +
      hx_pci +
      comorb_hf +  comorb_diab + 
      comorb_crf + comorb_cevd + 
      angiogram + cardiac_rehab +
      (1|procode3),
    family = binomial(link = "logit"),
    data = cohort,
    control = glmerControl(optimizer = "bobyqa", optCtrl = list(maxfun = 30000))
  )
mod_4_ang_reh_finish <- Sys.time()
mod_4_ang_reh_time <- mod_4_ang_reh_finish - mod_4_ang_reh_start

# Summarise results
mod_4_ang_reh_out <- glmer_out(mod_4_ang_reh, prefix = "mod4_ang_reh_")

# 4.5a.3 Model 4 + angiogram + rehab + secondary prevention -------------------

mod_4_ang_reh_prv_start <- Sys.time()
mod_4_ang_reh_prv <-
  glmer(
    mort_yr ~ smi + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5 +
      period_4 + mi_wd + mi_on + comorb_angina + comorb_mi +
      hx_pci +
      comorb_hf +  comorb_diab + 
      comorb_crf + comorb_cevd + 
      angiogram + cardiac_rehab + prev_all +
      (1|procode3),
    family = binomial(link = "logit"),
    data = cohort,
    control = glmerControl(optimizer = "bobyqa", optCtrl = list(maxfun = 30000))
  )
mod_4_ang_reh_prv_finish <- Sys.time()
mod_4_ang_reh_prv_time <- mod_4_ang_reh_prv_finish - mod_4_ang_reh_prv_start

# Summarise results
mod_4_ang_reh_prv_out <- glmer_out(mod_4_ang_reh_prv, prefix = "mod4_ang_reh_prv_")

# 4.6 Model 4 + care (binary SMI) ---------------------------------------------

# Create binary SMI variable
cohort <- 
  cohort %>%
  mutate(
    smi_bin = 
      case_when(
        smi %in% c("Schizophrenia", "Bipolar disorder", "Depression") ~ "Mental illness",
        smi %in% "No SMI" ~ "No mental illness"
      ),
    smi_bin = factor(smi_bin, levels = c("No mental illness", "Mental illness"))
  )

mod_4_care_bin_start <- Sys.time()
mod_4_care_bin <-
  glmer(
    mort_yr ~ smi_bin + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5 +
      period_4 + mi_wd + mi_on + comorb_angina + comorb_mi +
      hx_pci +
      comorb_hf +  comorb_diab + 
      comorb_crf + comorb_cevd + 
      angiogram + admission_to_cardiac_ward + prev_all + cardiac_rehab +
      (1|procode3),
    family = binomial(link = "logit"),
    data = cohort,
    control = glmerControl(optimizer = "bobyqa", optCtrl = list(maxfun = 30000))
  )
mod_4_care_bin_finish <- Sys.time()
mod_4_care_bin_time <- mod_4_care_bin_finish - mod_4_care_bin_start

# Summarise results
mod_4_care_bin_out <- glmer_out(mod_4_care_bin, prefix = "mod4_care_bin_")


# 4.7 Model 4 + care + (binary SMI) + interaction ------------------------------

# Add SMI:period interaction

mod_4_care_int_start <- Sys.time()
mod_4_care_int <-
  glmer(
    mort_yr ~ smi_bin + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5 +
      period_4 + mi_wd + mi_on + comorb_angina + comorb_mi +
      hx_pci +
      comorb_hf +  comorb_diab + 
      comorb_crf + comorb_cevd + 
      angiogram + admission_to_cardiac_ward + prev_all + cardiac_rehab +
      smi_bin:period_4 + (1|procode3),
    family = binomial(link = "logit"),
    data = cohort,
    control = glmerControl(optimizer = "bobyqa", optCtrl = list(maxfun = 40000))
  )
mod_4_care_int_finish <- Sys.time()
mod_4_care_int_time <- mod_4_care_int_finish - mod_4_care_int_start

# Summarise results
mod_4_care_int_out <- glmer_out(mod_4_care_int, prefix = "mod_4_care_int_")

# 5. Combine results ----------------------------------------------------------

mod_out <- 
  mod_4_care_out %>%
  left_join(mod_4_ang_reh_prv_out) %>%
  left_join(mod_4_ang_reh_out) %>%
  left_join(mod_4_angio_out) %>%
  left_join(mod_4_out) %>%
  left_join(mod_3_out) %>%
  left_join(mod_2_out) %>%
  left_join(mod_1_out) %>%
  select(
    covariate, 
    mod1_or_fmt, mod2_or_fmt, mod3_or_fmt, mod4_or_fmt, 
    mod4_angio_or_fmt, mod4_ang_reh_or_fmt, mod4_ang_reh_prv_or_fmt,
    mod4_care_or_fmt,
    #
    mod1_or,             mod1_or_low,             mod1_or_upp,
    mod2_or,             mod2_or_low,             mod2_or_upp,
    mod3_or,             mod3_or_low,             mod3_or_upp,
    mod4_or,             mod4_or_low,             mod4_or_upp,
    mod4_angio_or,       mod4_angio_or_low,       mod4_angio_or_upp,
    mod4_ang_reh_or,     mod4_ang_reh_or_low,     mod4_ang_reh_or_upp,
    mod4_ang_reh_prv_or, mod4_ang_reh_prv_or_low, mod4_ang_reh_prv_or_upp,
    mod4_care_or,        mod4_care_or_low,        mod4_care_or_upp
  )

dim(mod_out)

# ANOVA
anova_cm_care <- 
  anova(
    mod_1, mod_2, mod_3, mod_4, 
    mod_4_angio, mod_4_ang_reh, mod_4_ang_reh_prv, mod_4_care
  )

anova_cm_care <- as.data.frame(anova_cm_care)

write.csv(
  mod_out, 
  file = file.path(outcome_folder, "12b_mort_nstemi_1yr_home_mod1to4_care.csv")
)


write.csv(
  anova_cm_care, 
  file.path(outcome_folder, "12b_mort_nstemi_1yr_home_anova_cm_care.csv")
)

# Combine models 4 (binary) and 4 (interaction)

dim(mod_4_care_int_out)  

mod_out_care_int <- 
  mod_4_care_int_out %>%
  left_join(mod_4_care_bin_out) %>%
  select(
    covariate, 
    mod4_care_bin_or_fmt, mod_4_care_int_or_fmt,
    mod4_care_bin_or, mod4_care_bin_or_low, mod4_care_bin_or_upp,
    mod_4_care_int_or, mod_4_care_int_or_low, mod_4_care_int_or_upp,
  )

dim(mod_out_care_int)

write.csv(
  mod_out_care_int, 
  file = file.path(outcome_folder, "12b_mort_nstemi_1yr_home_mod_int.csv")
)

anova_int <- anova(mod_4_care_bin, mod_4_care_int)

write.csv(
  anova_int,
  file = file.path(outcome_folder, "12b_mort_nstemi_1yr_home_anova_int.csv")
) 
  
