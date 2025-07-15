#
# NSTEMI: Time to CVD mortality within the first year
# Key models 
# - (complete cases on demographics, timing and co-morbidities only)
#
# K. Fleetwood
# 29 Jan 2025: Adapted from 14b
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

library(performance) # includes function for checking co-linearity

library(survival)
library(coxme)

library(multcomp)

ccu046_folder <- "~/collab/CCU046"
ccu046_02_folder <- file.path(ccu046_folder, "CCU046_02")
pd_folder <- file.path(ccu046_02_folder, "processed_data")
outcome_folder <- file.path(ccu046_02_folder, "processed_data_nstemi_cvd_surv")

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
cohort <- 
  cohort %>% 
  filter(mi_type %in% "NSTEMI") %>%
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
    cc = !is.na(imd_2019) & !is.na(ethnic_5) & !is.na(procode3)
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

# Mixed effects Cox model on complete cases
# Adjust for age and sex

# Takes approximately 1 minute
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
    Surv(mort_yr_days, mort_cvd_yr) ~ smi + age_mi_scl + I(age_mi_scl^2) + 
      sex + imd_2019 + ethnic_5  + (1|procode3),
    data = cohort
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


# Repeat model 4 with binary SMI variable

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

mod_4_bin_start <- Sys.time()
mod_4_bin <-
  coxme(
    Surv(mort_yr_days, mort_cvd_yr) ~ smi_bin + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5 +
      period_4 + mi_wd + mi_on + comorb_angina + comorb_mi +
      hx_pci +
      comorb_hf +  comorb_diab + 
      comorb_crf + comorb_cevd + (1|procode3),
    data = cohort
  )
mod_4_bin_finish <- Sys.time()
mod_4_bin_time <- mod_4_bin_finish - mod_4_bin_start
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

mod_4_int_out <- coxme_out(mod_4_int, prefix = "mod_4_int_")

# Estimate MI HR for each period

coefeq <- 
  matrix(
    data = 0,
    nrow = 10, 
    ncol = nrow(summary(mod_4_int)$coef)
  )
colnames(coefeq) <- row.names(summary(mod_4_int)$coef)

# Fill in main results
coefeq[1:10, "smi_binMental illness"] <- 1

# Severe mental illness
coefeq[ 2, "smi_binMental illness:period_4Mar 2020 -\nJun 2020"] <- 1
coefeq[ 3, "smi_binMental illness:period_4Jul 2020 -\nOct 2020"] <- 1
coefeq[ 4, "smi_binMental illness:period_4Nov 2020 -\nFeb 2021"] <- 1
coefeq[ 5, "smi_binMental illness:period_4Mar 2021 -\nJun 2021"] <- 1
coefeq[ 6, "smi_binMental illness:period_4Jul 2021 -\nOct 2021"] <- 1
coefeq[ 7, "smi_binMental illness:period_4Nov 2021 -\nFeb 2022"] <- 1
coefeq[ 8, "smi_binMental illness:period_4Mar 2022 -\nJun 2022"] <- 1
coefeq[ 9, "smi_binMental illness:period_4Jul 2022 -\nOct 2022"] <- 1
coefeq[10, "smi_binMental illness:period_4Nov 2022 -\nFeb 2023"] <- 1

mod_4_int_mi_period <- 
  glht(
    mod_4_int,
    linfct = coefeq
  )

mod_4_int_mi_period_ci <-
  confint(
    mod_4_int_mi_period,
    calpha = univariate_calpha()
  )

mod_4_int_mi_period_ci_out <-
  as.data.frame(mod_4_int_mi_period_ci$confint) %>%
  mutate(
    period = levels(cohort$period_4),
    hr = exp(Estimate),
    hr_low = exp(lwr),
    hr_upp = exp(upr),
    hr_fmt =
      paste0(
        format(round(hr, digits = 2), nsmall = 2), " (",
        format(round(hr_low, digits = 2), nsmall = 2), ", ",
        format(round(hr_upp, digits = 2), nsmall = 2), ")"
      )
  ) %>%
  dplyr::select(period, starts_with("hr"))

write.csv(
  mod_4_int_mi_period_ci_out, 
  file = file.path(outcome_folder, "15b_mort_nstemi_cvd_surv_mod_int_lc.csv")
)

# 5. Combine results ----------------------------------------------------------

dim(mod_4_out)

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

dim(mod_out)

anova_1to4 <- anova(mod_1, mod_2, mod_3, mod_4, test = "Chisq")
anova_1to4 <- cbind(model = c("mod_1", "mod_2", "mod_3", "mod_4"), as.data.frame(anova_1to4))

write.csv(
  mod_out, 
  file = file.path(outcome_folder, "15b_mort_nstemi_cvd_surv_mod1to4.csv")
)
write.csv(
  anova_1to4, 
  file.path(outcome_folder, "15b_mort_nstemi_cvd_surv_anova1to4.csv")
)

# Combine models 4 (binary) and 4 (interaction)

dim(mod_4_int_out)  

mod_out_int <- 
  mod_4_int_out %>%
  left_join(mod_4_bin_out) %>%
  select(
    covariate, 
    mod4_bin_hr_fmt, mod_4_int_hr_fmt,
    mod4_bin_hr, mod4_bin_hr_low, mod4_bin_hr_upp,
    mod_4_int_hr, mod_4_int_hr_low, mod_4_int_hr_upp,
  )

dim(mod_out_int)

write.csv(
  mod_out_int, 
  file = file.path(outcome_folder, "15b_mort_nstemi_cvd_surv_mod_int.csv")
)

anova_int <- anova(mod_4_bin, mod_4_int, test = "Chisq")
anova_int <- cbind(model = c("mod_4_bin", "mod_4_int"), as.data.frame(anova_int))

write.csv(
  anova_int,
  file = file.path(outcome_folder, "15b_mort_nstemi_cvd_surv_anova_int.csv")
) 
  
