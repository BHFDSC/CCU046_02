#
# STEMI: Time to CVD mortality within the first year
# Key models
# CC on demographics, timing and comorbidities only
#
# K. Fleetwood
# 31 Jan 2025: Adapted from 24b
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
library(lattice)

library(survival)
library(coxme)

library(performance) # includes function for checking co-linearity

home_folder <- "D:\\PhotonUser\\My Files\\Home Folder\\"
ccu046_02_folder <- file.path(home_folder, "CCU046_02")
pd_folder <- file.path(ccu046_02_folder, "Processed data")
outcome_folder <- file.path(ccu046_02_folder, "processed_data_stemi_cvd_surv")

r_folder <- file.path(home_folder, "r")

# Additional functions to help with summarising models
source(file.path(r_folder, "model_summary_functions.R"))  

#
# 2. Load data ----------------------------------------------------------------
#

cohort <- readRDS(file = file.path(pd_folder, "01_cohort.rds"))

# Subset cohort to STEMIs only
# Factor covariates for modelling
# Select cc
cohort <- 
  cohort %>% 
  filter(mi_type %in% "STEMI") %>%
  mutate(
    smi = 
      factor(
        smi, 
        levels = c("No SMI", "Schizophrenia", "Bipolar disorder", "Depression")
      ),
    bmi_cat = 
      factor(
        bmi_cat, 
        levels = c("18.5 - 25", "<18.5", "25 - 30", "30 - 40", ">=40")
      ),
    sex = factor(sex, levels = c("Male", "Female")),
    imd_2019 = 
      factor(
        imd_2019, 
        levels = 
          c("10 (least deprived)", "1 (most deprived)", "2", "3", "4", "5", "6",
            "7", "8", "9")
      ),
    cardiogenic_shock = factor(cardiogenic_shock, levels = c("No", "Yes")),
    cc = !is.na(imd_2019)   & !is.na(ethnic_5) & !is.na(procode3)
  ) %>%
  filter(cc)

# Calculate mean and SD for age
age_mi_mean <- mean(cohort$age_mi)
age_mi_sd <- sd(cohort$age_mi)

# Create binary SMI variable
cohort <- 
  cohort %>%
  mutate(
    age_mi_scl     = (age_mi - age_mi_mean)/age_mi_sd,
    smi_bin = 
      case_when(
        smi %in% c("Schizophrenia", "Bipolar disorder", "Depression") ~ "Mental illness",
        smi %in% "No SMI" ~ "No mental illness"
      ),
    smi_bin = factor(smi_bin, levels = c("No mental illness", "Mental illness"))
  )

# 4. Key models ---------------------------------------------------------------

# 4.1 Model 1 -----------------------------------------------------------------

mod_1_start <- Sys.time()
mod_1 <-
  coxme(
   Surv(mort_yr_days, mort_cvd_yr) ~ smi + age_mi_scl + I(age_mi_scl^2) + sex  + (1|procode3),
    data = cohort
  )
mod_1_finish <- Sys.time()
mod_1_time <- mod_1_finish - mod_1_start
mod_1_time

mod_1_out <- coxme_out(mod_1, prefix = "mod1_")

# 4.2 Model 2 -----------------------------------------------------------------
# Adjust for age, sex, IMD, ethnicity

mod_2_start <- Sys.time()
mod_2 <-
  coxme(
   Surv(mort_yr_days, mort_cvd_yr) ~ smi + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5  + (1|procode3),
    data = cohort,
  )
mod_2_finish <- Sys.time()
mod_2_time <- mod_2_finish - mod_2_start
mod_2_time

mod_2_out <- coxme_out(mod_2, prefix = "mod2_")

# 4.3 Model 3 -----------------------------------------------------------------

mod_3_start <- Sys.time()
mod_3 <-
  coxme(
    Surv(mort_yr_days, mort_cvd_yr) ~ smi + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5 +
      period_4 + mi_wd + mi_on + (1|procode3),
    data = cohort
  )
mod_3_finish <- Sys.time()
mod_3_time <- mod_3_finish - mod_3_start
mod_3_time

mod_3_out <- coxme_out(mod_3, prefix = "mod3_")

# 4.4 Model 4 -----------------------------------------------------------------

# Sociodemographics, timing and comorbidities

mod_4_start <- Sys.time()
mod_4 <-
  coxme(
    Surv(mort_yr_days, mort_cvd_yr) ~ smi + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5 +
      period_4 + mi_wd + mi_on + comorb_angina + comorb_mi +
      hx_pci +
      comorb_hf +  comorb_diab + 
      comorb_crf + comorb_cevd + (1|procode3),
    data = cohort
  )
mod_4_finish <- Sys.time()
mod_4_time <- mod_4_finish - mod_4_start
mod_4_time

mod_4_out <- coxme_out(mod_4, prefix = "mod4_")

# Repeat model 4 with binary smi variable

mod_4_bin_start <- Sys.time()
mod_4_bin <-
  coxme(
    Surv(mort_yr_days, mort_cvd_yr) ~ smi_bin + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5 +
      period_4 + mi_wd + mi_on + comorb_angina + comorb_mi +
      hx_pci +
      comorb_hf +  comorb_diab + 
      comorb_crf + comorb_cevd + 
      (1|procode3),
    data = cohort
  )

mod_4_bin_finish <- Sys.time()
mod_4_bin_time <- mod_4_bin_finish -mod_4_bin_start
mod_4_bin_time

mod_4_bin_out <- coxme_out(mod_4_bin, prefix = "mod4_bin_")

# 4.7 Model 4 (binary SMI) + interaction ---------------------------------------

# Add SMI:period interaction

mod_4_int_start <- Sys.time()
mod_4_int <-
  coxme(
    Surv(mort_yr_days, mort_cvd_yr) ~ smi_bin + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5 +
      period_4 + mi_wd + mi_on + comorb_angina + comorb_mi +
      hx_pci +
      comorb_hf +  comorb_diab + 
      comorb_crf + comorb_cevd + smi_bin:period_4 + (1|procode3),
    data = cohort
  )
mod_4_int_finish <- Sys.time()
mod_4_int_time <- mod_4_int_finish - mod_4_int_start
mod_4_int_time

# Summarise results
mod_4_int_out <- coxme_out(mod_4_int, prefix = "mod_4_int_")

# 5. Combine results ----------------------------------------------------------

# Combine models 1 to 4

mod_out <- 
  mod_4_out %>%
  left_join(mod_3_out) %>%
  left_join(mod_2_out) %>%
  left_join(mod_1_out) %>%
  select(
    covariate, 
    mod1_hr_fmt, mod2_hr_fmt, mod3_hr_fmt, mod4_hr_fmt,
    mod1_hr, mod1_hr_low, mod1_hr_upp,
    mod2_hr, mod2_hr_low, mod2_hr_upp,
    mod3_hr, mod3_hr_low, mod3_hr_upp,
    mod4_hr, mod4_hr_low, mod4_hr_upp
  )

anova_1to4 <- anova(mod_1, mod_2, mod_3, mod_4, test = "Chisq")
anova_1to4 <- cbind(model = c("mod_1", "mod_2", "mod_3", "mod_4"), as.data.frame(anova_1to4))

write.csv(
  mod_out, 
  file = file.path(outcome_folder, "25b_mort_stemi_cvd_surv_mod1to4.csv")
)
write.csv(
  anova_1to4, 
  file.path(outcome_folder, "25b_mort_stemi_cvd_surv_anova1to4.csv")
)

# Combine models 4 (binary) and 4 (interaction)
mod_out_int <- 
  mod_4_int_out %>%
  left_join(mod_4_bin_out) %>%
  select(
    covariate, 
    mod4_bin_hr_fmt, mod_4_int_hr_fmt,
    mod4_bin_hr, mod4_bin_hr_low, mod4_bin_hr_upp,
    mod_4_int_hr, mod_4_int_hr_low, mod_4_int_hr_upp,
  )

write.csv(
  mod_out_int, 
  file = file.path(outcome_folder, "25b_mort_stemi_cvd_surv_mod_int.csv")
)

anova_int <- anova(mod_4_bin, mod_4_int, test = "Chisq")
anova_int <- cbind(model = c("mod_4_bin", "mod_4_int"), as.data.frame(anova_int))

write.csv(
  anova_int,
  file = file.path(outcome_folder, "25b_mort_stemi_cvd_surv_anova_int.csv")
) 