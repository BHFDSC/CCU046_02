#
# NSTEMI: Time to CVD mortality within the first year
# Key models (complete cases)
#
# K. Fleetwood
# 29 Jan 2025: Adapted from 14a
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
st_deviation_no <- 
  c("No acute changes", "Left bundle branch block", "T wave changes only",
    "Other acute abnormality")
cohort <- 
  cohort %>% 
  filter(mi_type %in% "NSTEMI") %>%
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
    st_deviation = 
      case_when(
        ecg %in% st_deviation_no  ~ "No",
        ecg %in% c("ST segment elevation", "ST segment depression") ~ "Yes",
        is.na(ecg) ~ as.character(NA)
      ),
    cardiogenic_shock = factor(cardiogenic_shock, levels = c("No", "Yes")),
    cc = !is.na(imd_2019)   & !is.na(ethnic_5)       & !is.na(cardiogenic_shock) &
      !is.na(ecg)        & !is.na(cardiac_arrest) & #!is.na(cardiac_enzymes_elevated) &
      !is.na(creatinine) & !is.na(sbp)            & !is.na(heart_rate) &  
      !is.na(procode3)
  ) %>%
  filter(cc)

age_mi_mean <- mean(cohort$age_mi)
age_mi_sd <- sd(cohort$age_mi)

crt_mean <- mean(cohort$creatinine)
crt_sd <- sd(cohort$creatinine)

crt_log_mean <- mean(log(cohort$creatinine))
crt_log_sd <- sd(log(cohort$creatinine))

sbp_mean <- mean(cohort$sbp)
sbp_sd <- sd(cohort$sbp)

hr_mean <- mean(cohort$heart_rate)
hr_sd <- sd(cohort$heart_rate)

cohort <- 
  cohort %>%
  mutate(
    age_mi_scl     = (age_mi - age_mi_mean)/age_mi_sd,
    creatinine_scl = (creatinine - crt_mean)/crt_sd,
    crt_log_scl = (log(creatinine) - crt_log_mean)/crt_log_sd,
    sbp_scl        = (sbp - sbp_mean)/sbp_sd,
    heart_rate_scl = (heart_rate - hr_mean)/hr_sd
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
    data = cohort,
  )
mod_1_finish <- Sys.time()
mod_1_time <- mod_1_finish - mod_1_start
mod_1_time

# Summarise results
mod_1_out <- coxme_out(mod_1, prefix = "mod1_")


# 4.2 Model 2 -----------------------------------------------------------------
# Adjust for age, sex, IMD, ethnicity

mod_2_start <- Sys.time()
mod_2 <-
  coxme(
    Surv(mort_yr_days, mort_cvd_yr) ~ smi + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5  + (1|procode3),
    data = cohort
  )
mod_2_finish <- Sys.time()
mod_2_time <- mod_2_finish - mod_2_start
mod_2_time

# Summarise results
mod_2_out <- coxme_out(mod_2, prefix = "mod2_")

# 4.3 Model 3 -----------------------------------------------------------------

# Takes 11 minutes to run
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

# Summarise results
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

# Summarise results
mod_4_out <- coxme_out(mod_4, prefix = "mod4_")

# 4.5 Model 5 -----------------------------------------------------------------

# Sociodemographics, timing, comorbidities and MI characteristics

mod_5 <-
  coxme(
    Surv(mort_yr_days, mort_cvd_yr) ~ smi + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5 +
      period_4 + mi_wd + mi_on + comorb_angina + comorb_mi +
      hx_pci +
      comorb_hf +  comorb_diab + 
      comorb_crf + comorb_cevd + cardiac_arrest + #cardiac_enzymes_elevated +
      covid +  st_deviation + cardiogenic_shock + 
      crt_log_scl + I(crt_log_scl^2) +
      sbp_scl + I(sbp_scl^2) +
      heart_rate_scl + I(heart_rate_scl^2) +
      (1|procode3),
    data = cohort
  )

mod_5_finish <- Sys.time()
mod_5_time <- mod_5_finish - mod_5_start
mod_5_time

mod_5_out <- coxme_out(mod_5, prefix = "mod5_")

# Repeat model 5 with binary SMI variable

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

mod_5_bin_start <- Sys.time()

mod_5_bin <-
  coxme(
    Surv(mort_yr_days, mort_cvd_yr) ~ smi_bin + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5 +
      period_4 + mi_wd + mi_on + comorb_angina + comorb_mi +
      hx_pci +
      comorb_hf +  comorb_diab +
      comorb_crf + comorb_cevd + cardiac_arrest + #cardiac_enzymes_elevated +
      covid +  st_deviation + cardiogenic_shock +
      crt_log_scl + I(crt_log_scl^2) +
      sbp_scl + I(sbp_scl^2) +
      heart_rate_scl + I(heart_rate_scl^2) +
      (1|procode3),
    data = cohort
  )

mod_5_bin_finish <- Sys.time()
mod_5_bin_time <- mod_5_bin_finish - mod_5_bin_start

mod_5_bin_out <- coxme_out(mod_5_bin, prefix = "mod5_bin_")


# 4.7 Model 6 -----------------------------------------------------------------

# Add SMI:period interaction

mod_6_start <- Sys.time()
mod_6 <-
  coxme(
    Surv(mort_yr_days, mort_cvd_yr) ~ smi_bin + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5 +
      period_4 + mi_wd + mi_on + comorb_angina + comorb_mi +
      hx_pci +
      comorb_hf +  comorb_diab + 
      comorb_crf + comorb_cevd + cardiac_arrest + 
      covid +  st_deviation + cardiogenic_shock + 
      crt_log_scl + I(crt_log_scl^2) +
      sbp_scl + I(sbp_scl^2) +
      heart_rate_scl + I(heart_rate_scl^2) +
      smi_bin:period_4 + (1|procode3),
    data = cohort
  )

mod_6_finish <- Sys.time()
mod_6_time <- mod_6_finish - mod_6_start

mod_6_out <- coxme_out(mod_6, prefix = "mod6_")

# Estimate MI HR for each period

coefeq <- 
  matrix(
    data = 0,
    nrow = 10, 
    ncol = nrow(summary(mod_6)$coef)
  )
colnames(coefeq) <- row.names(summary(mod_6)$coef)

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

mod_6_mi_period <- 
  glht(
    mod_6,
    linfct = coefeq
  )

mod_6_mi_period_ci <-
  confint(
    mod_6_mi_period,
    calpha = univariate_calpha()
  )

mod_6_mi_period_ci_out <-
  as.data.frame(mod_6_mi_period_ci$confint) %>%
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
  mod_6_mi_period_ci_out, 
  file = file.path(outcome_folder, "15a_mort_nstemi_cvd_surv_mod_int_lc.csv")
)

# 5. Combine results ----------------------------------------------------------

dim(mod_5_out)

mod_out <- 
  mod_5_out %>%
  left_join(mod_4_out) %>%
  left_join(mod_3_out) %>%
  left_join(mod_2_out) %>%
  left_join(mod_1_out) %>%
  select(
    covariate, 
    mod1_hr_fmt, mod2_hr_fmt, mod3_hr_fmt, mod4_hr_fmt, mod5_hr_fmt, 
    mod1_hr, mod1_hr_low, mod1_hr_upp,
    mod2_hr, mod2_hr_low, mod2_hr_upp,
    mod3_hr, mod3_hr_low, mod3_hr_upp,
    mod4_hr, mod4_hr_low, mod4_hr_upp,
    mod5_hr, mod5_hr_low, mod5_hr_upp
  )

dim(mod_out)

anova_1to5 <- anova(mod_1, mod_2, mod_3, mod_4, mod_5, test = "Chisq")
anova_1to5 <- 
    cbind(
      model = c("mod_1", "mod_2", "mod_3", "mod_4", "mod_5"),
      as.data.frame(anova_1to5)
    )

write.csv(
  mod_out, 
  file = file.path(outcome_folder, "15a_mort_nstemi_cvd_surv_mod1to5.csv")
)
write.csv(
  anova_1to5, 
  file.path(outcome_folder, "15a_mort_nstemi_cvd_surv_anova1to5.csv")
)

# Combine models 5 (binary) and 6
dim(mod_6_out)  

mod_out_int <- 
  mod_6_out %>%
  left_join(mod_5_bin_out) %>%
  select(
    covariate, 
    mod5_bin_hr_fmt, mod6_hr_fmt,
    mod5_bin_hr, mod5_bin_hr_low, mod5_bin_hr_upp,
    mod6_hr, mod6_hr_low, mod6_hr_upp
  )

dim(mod_out_int)

write.csv(
  mod_out_int, 
  file = file.path(outcome_folder, "15a_mort_nstemi_cvd_surv_mod_int.csv")
)

anova_int <- anova(mod_5_bin, mod_6, test = "Chisq")
anova_int <- 
  cbind(
    model = c("mod_5_bin", "mod_6"),
    as.data.frame(anova_int)
  )

write.csv(
  anova_int,
  file = file.path(outcome_folder, "15a_mort_nstemi_cvd_surv_anova_int.csv")
) 

