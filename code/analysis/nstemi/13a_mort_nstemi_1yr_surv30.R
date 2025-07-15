#
# NSTEMI: 1-year mortality (amongst people who survived 30 days)
# Key models (complete cases)
#
# K. Fleetwood
# 22 Jan 2025: Adapted from 11a
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
outcome_folder <- file.path(ccu046_02_folder, "processed_data_nstemi_1yr_surv30")

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
  filter(
    mi_type %in% "NSTEMI",
    !mort_30
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
    st_deviation = 
      case_when(
        ecg %in% st_deviation_no  ~ "No",
        ecg %in% c("ST segment elevation", "ST segment depression") ~ "Yes",
        is.na(ecg) ~ as.character(NA)
      ),
    cardiogenic_shock = factor(cardiogenic_shock, levels = c("No", "Yes")),
    cc = !is.na(imd_2019)   & !is.na(ethnic_5)       & !is.na(cardiogenic_shock) &
      !is.na(ecg)        & !is.na(cardiac_arrest) &
      !is.na(creatinine) & !is.na(sbp)            & !is.na(heart_rate) &  
      !is.na(procode3)
  ) %>%
  filter(cc)

age_mi_mean <- mean(cohort$age_mi)
age_mi_sd <- sd(cohort$age_mi)

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
    crt_log_scl = (log(creatinine) - crt_log_mean)/crt_log_sd,
    sbp_scl        = (sbp - sbp_mean)/sbp_sd,
    heart_rate_scl = (heart_rate - hr_mean)/hr_sd
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

# Took 5 minutes to run
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
mod_3_time

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
mod_4_time <- difftime(mod_4_finish, mod_4_start, units = "mins")
mod_4_time

# Summarise results
mod_4_out <- glmer_out(mod_4, prefix = "mod4_")

# 4.5 Model 5 -----------------------------------------------------------------

# Sociodemographics, timing, comorbidities and MI characteristics
mod_5_start <- Sys.time()
mod_5 <-
  glmer(
    mort_yr ~ smi + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5 +
      period_4 + mi_wd + mi_on + comorb_angina + comorb_mi +
      hx_pci +
      comorb_hf +  comorb_diab + 
      comorb_crf + comorb_cevd + cardiac_arrest + #cardiac_enzymes_elevated +
      covid +  st_deviation + cardiogenic_shock + 
      crt_log_scl + I(crt_log_scl^2) +
      sbp_scl + I(sbp_scl^2) +
      heart_rate_scl + I(heart_rate_scl^2) +
      (1|procode3),
    family = binomial(link = "logit"),
    data = cohort,
    control = glmerControl(optimizer = "bobyqa", optCtrl = list(maxfun = 45000))
  )

mod_5_finish <- Sys.time()
mod_5_time <- difftime(mod_5_finish, mod_5_start, units = "mins")
mod_5_time

mod_5_out <- glmer_out(mod_5, prefix = "mod5_")

# Repeat model 5 with binary SMI variable

# Create binary SMI variable --------------
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

# Mod 5 binary -------------

mod_5_bin_start <- Sys.time()

mod_5_bin <-
  glmer(
    mort_yr ~ smi_bin + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5 +
      period_4 + mi_wd + mi_on + comorb_angina + comorb_mi +
      hx_pci +
      comorb_hf +  comorb_diab +
      comorb_crf + comorb_cevd + cardiac_arrest + #cardiac_enzymes_elevated +
      covid +  st_deviation + cardiogenic_shock +
      crt_log_scl + I(crt_log_scl^2) +
      sbp_scl + I(sbp_scl^2) +
      heart_rate_scl + I(heart_rate_scl^2) +
      (1|procode3),
    family = binomial(link = "logit"),
    data = cohort,
    control = glmerControl(optimizer = "bobyqa", optCtrl = list(maxfun = 60000))
  )

mod_5_bin_finish <- Sys.time()
mod_5_bin_time <- difftime(mod_5_bin_finish, mod_5_bin_start, units = "mins")
mod_5_bin_time

mod_5_bin_out <- glmer_out(mod_5_bin, prefix = "mod5_bin_")

# Initially failed to converge: try restart

mod_5_bin_v2_start <- Sys.time()

start_vals <- getME(mod_5_bin, c("theta", "fixef"))
start_vals$theta <- runif(1, start_vals$theta - 0.05*abs(start_vals$theta), start_vals$theta + 0.05*abs(start_vals$theta))
start_vals$fixef <- runif(length(start_vals$fixef), start_vals$fixef - 0.05*abs(start_vals$fixef), start_vals$fixef + 0.05*abs(start_vals$fixef))

mod_5_bin_v2 <-
  update(
    mod_5_bin,
    start = start_vals,
    control = glmerControl(optimizer = "bobyqa", optCtrl = list(maxfun = 60000))
  )

mod_5_bin_v2_finish <- Sys.time()
mod_5_bin_v2_time <- difftime(mod_5_bin_v2_finish, mod_5_bin_v2_start, units = "mins")
# Failed again

mod_5_bin_v2_out <- glmer_out(mod_5_bin_v2, prefix = "mod5_bin_")

summary(mod_5)
summary(mod_5_bin_v2)

check_convergence(mod_5_bin)
check_convergence(mod_5_bin_v2)
# check convergence function concludes that convergence is sufficient

# Identify problematic parameter
m5b_hes  <- mod_5_bin_v2@optinfo$derivs$Hessian
m5b_grad <- mod_5_bin_v2@optinfo$derivs$gradient
m5b_scgrad <- solve(chol(m5b_hes), m5b_grad)

m5b_mingrad <- pmin(abs(m5b_scgrad), abs(m5b_grad))
m5b_gradsum <- cbind(abs(m5b_scgrad), abs(m5b_grad), m5b_mingrad)
row.names(m5b_gradsum) <- c(names(ranef(mod_5_bin_v2)), names(fixef(mod_5_bin_v2)))

as.data.frame(m5b_gradsum) %>% filter(m5b_mingrad > 0.002)
# Cardiogenic shock is the problem variable

mod_5_bin_v3_start <- Sys.time()

start_vals <- getME(mod_5_bin_v2, c("theta", "fixef"))
start_vals$theta <- runif(1, start_vals$theta - 0.05*abs(start_vals$theta), start_vals$theta + 0.05*abs(start_vals$theta))
start_vals$fixef <- runif(length(start_vals$fixef), start_vals$fixef - 0.05*abs(start_vals$fixef), start_vals$fixef + 0.05*abs(start_vals$fixef))

# Cardiogenic shock is position 40. Try substituting result from mod_5
start_vals$fixef[40] <- -0.1287304

mod_5_bin_v3 <-
  update(
    mod_5_bin_v2,
    start = start_vals,
    control = 
      glmerControl(
        optimizer = "bobyqa",
        optCtrl = list(maxfun = 100000, rhoend = 1e-8)
      )
  )

mod_5_bin_v3_finish <- Sys.time()
mod_5_bin_v3_time <- difftime(mod_5_bin_v3_finish, mod_5_bin_v3_start, units = "mins")
# 131 mins
check_convergence(mod_5_bin_v3)

mod_5_bin_v3_out <- glmer_out(mod_5_bin_v3, prefix = "mod5_bin_")

# Use update to update mod_5
mod_5_bin_v4_start <- Sys.time()

mod_5_bin_v4 <- 
  update(
    mod_5, 
    . ~ . -smi + smi_bin,
    control = 
      glmerControl(
        optimizer = "bobyqa",
        optCtrl = list(maxfun = 100000)
      )
  )

mod_5_bin_v4_finish <- Sys.time()
mod_5_bin_v4_time <- difftime(mod_5_bin_v4_finish, mod_5_bin_v4_start, units = "mins")

mod_5_bin_v4_out <- glmer_out(mod_5_bin_v4, prefix = "mod5_bin_")

# Try allFit
mod_5_bin_af <- 
  allFit(
    mod_5_bin,
    maxfun = 60000,
    verbose = TRUE,
    parallel = "multicore"
  )

af_bobyqa_out  <- glmer_out(mod_5_bin_af$bobyqa)
af_nm_out      <- glmer_out(mod_5_bin_af$Nelder_Mead)
af_nlmin_out   <- glmer_out(mod_5_bin_af$nlminbwrap)
af_nloptnm_out <- glmer_out(mod_5_bin_af$nloptwrap.NLOPT_LN_NELDERMEAD)
af_nloptbb_out <- glmer_out(mod_5_bin_af$nloptwrap.NLOPT_LN_BOBYQA)

extract_or <- function(cvr){
  
  out <- 
  c(
    mod_5_bin_out[mod_5_bin_out$covariate %in% cvr,5],
    mod_5_bin_v2_out[mod_5_bin_v2_out$covariate %in% cvr,5],
    mod_5_bin_v3_out[mod_5_bin_v3_out$covariate %in% cvr,5],
    mod_5_bin_v4_out[mod_5_bin_v4_out$covariate %in% cvr,5],
    af_bobyqa_out[af_bobyqa_out$covariate %in% cvr,5],
    af_nm_out[af_nm_out$covariate %in% cvr,5],
    af_nlmin_out[af_nlmin_out$covariate %in% cvr,5],
    af_nloptnm_out[af_nloptnm_out$covariate %in% cvr,5],
    af_nloptbb_out[af_nloptbb_out$covariate %in% cvr,5]
  )
  
  out
  
}

# Create summary of convergence for all mod_5_bin
mod_5_bin_conv_sum <- 
  data.frame(
    model = c("mod_5_bin", "mod_5_bin_v2", "mod_5_bin_v3", "mod_5_bin_v4",
              "allFit: bobyqa", "allFit: Nelder Mead", "allfit: nlminbwrap",
              "allFit: NLOPT_LN_NELDERMEAD", "allFit: NLOPT_LN_BOBYQA"),
    notes = c("", "restart", "restart with CS estimates from mod_5", 
              "update mod_5", "", "", "", "", ""),
    optimizer = c(rep("bobyqa", 4), names(mod_5_bin_af)),
    maxfun = c(rep(60000, 2), rep(100000, 2), rep(60000, 5)),
    rhoend = c(NA, NA, 1e-8, rep(NA, 6)), 
    convergence_glmer = c(
      mod_5_bin@optinfo$conv$lme4$messages,
      mod_5_bin_v2@optinfo$conv$lme4$messages,
      mod_5_bin_v3@optinfo$conv$lme4$messages,
      mod_5_bin_v4@optinfo$conv$lme4$messages,
      as.character(unlist(summary(mod_5_bin_af)$msgs)[1:2]),
      mod_5_bin_af$nlminbwrap@optinfo$message,
      as.character(unlist(summary(mod_5_bin_af)$msgs)[3:4])
    ),
    convergence_check = 
      c(
        check_convergence(mod_5_bin)[1],
        check_convergence(mod_5_bin_v2)[1],
        check_convergence(mod_5_bin_v3)[1],
        check_convergence(mod_5_bin_v4)[1],
        check_convergence(mod_5_bin_af[[1]])[1],
        check_convergence(mod_5_bin_af[[2]])[1],
        check_convergence(mod_5_bin_af[[3]])[1],
        check_convergence(mod_5_bin_af[[4]])[1],
        check_convergence(mod_5_bin_af[[5]])[1]
      ),
    mi_or_fmt    = extract_or("smi_binMental illness"),
    mar20_or_fmt = extract_or("period_4Mar 2020 -\nJun 2020"),
    jul20_or_fmt = extract_or("period_4Jul 2020 -\nOct 2020"),
    nov20_or_fmt = extract_or("period_4Nov 2020 -\nFeb 2021"),
    #
    mar21_or_fmt = extract_or("period_4Mar 2021 -\nJun 2021"),
    jul21_or_fmt = extract_or("period_4Jul 2021 -\nOct 2021"),
    nov21_or_fmt = extract_or("period_4Nov 2021 -\nFeb 2022"),
    #
    mar22_or_fmt = extract_or("period_4Mar 2022 -\nJun 2022"),
    jul22_or_fmt = extract_or("period_4Jul 2022 -\nOct 2022"),
    nov22_or_fmt = extract_or("period_4Nov 2022 -\nFeb 2023")
  )

mod_5_out <- glmer_out(mod_5_restart)

# 4.7 Model 6 -----------------------------------------------------------------

# Add SMI:period interaction

mod_6_start <- Sys.time()
mod_6 <-
  glmer(
    mort_yr ~ smi_bin + age_mi_scl + I(age_mi_scl^2) + sex + imd_2019 + ethnic_5 +
      period_4 + mi_wd + mi_on + comorb_angina + comorb_mi +
      hx_pci +
      comorb_hf +  comorb_diab + 
      comorb_crf + comorb_cevd + cardiac_arrest + #cardiac_enzymes_elevated +
      covid +  st_deviation + cardiogenic_shock + 
      crt_log_scl + I(crt_log_scl^2) +
      sbp_scl + I(sbp_scl^2) +
      heart_rate_scl + I(heart_rate_scl^2) +
      smi_bin:period_4 + (1|procode3),
    family = binomial(link = "logit"),
    data = cohort,
    control = 
      glmerControl(
        optimizer = "bobyqa", 
        optCtrl = list(maxfun = 100000, rhoend = 1e-9)
    )
  )

mod_6_finish <- Sys.time()
mod_6_time <- difftime(mod_6_finish, mod_6_start, units = "mins")

mod_6_out <- glmer_out(mod_6, prefix = "mod6_")

anova_int  <- anova(mod_5_bin, mod_6) # best convergence
anova_int2 <- anova(mod_5_bin_v2, mod_6)
anova_int3 <- anova(mod_5_bin_v3, mod_6)
anova_int4 <- anova(mod_5_bin_v4, mod_6)
#
anova_bobyqa  <- anova(mod_5_bin_af$bobyqa, mod_6)
anova_nm      <- anova(mod_5_bin_af$Nelder_Mead, mod_6)
anova_nlmin   <- anova(mod_5_bin_af$nlminbwrap, mod_6)
anova_nloptnm <- anova(mod_5_bin_af$nloptwrap.NLOPT_LN_NELDERMEAD, mod_6)
anova_nloptbb <- anova(mod_5_bin_af$nloptwrap.NLOPT_LN_BOBYQA, mod_6)

mod_5_bin_conv_sum$anova_p <-
  c(
    anova_int[2,"Pr(>Chisq)"], anova_int2[2,"Pr(>Chisq)"], 
    anova_int3[2,"Pr(>Chisq)"], anova_int4[2,"Pr(>Chisq)"],
    anova_bobyqa[2,"Pr(>Chisq)"], anova_nm[2,"Pr(>Chisq)"],
    anova_nlmin[2,"Pr(>Chisq)"], anova_nloptnm[2,"Pr(>Chisq)"],
    anova_nloptbb[2,"Pr(>Chisq)"]
  )

write.csv(
  mod_5_bin_conv_sum, 
  file = file.path(outcome_folder, "13a_mort_nstemi_1yr_surv30_mod5bin_convergence.csv")
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
    mod1_or_fmt, mod2_or_fmt, mod3_or_fmt, mod4_or_fmt, mod5_or_fmt, 
    mod1_or, mod1_or_low, mod1_or_upp,
    mod2_or, mod2_or_low, mod2_or_upp,
    mod3_or, mod3_or_low, mod3_or_upp,
    mod4_or, mod4_or_low, mod4_or_upp,
    mod5_or, mod5_or_low, mod5_or_upp
  )

dim(mod_out)

anova_1to5 <- anova(mod_1, mod_2, mod_3, mod_4, mod_5)
anova_1to5 <- as.data.frame(anova_1to5)

write.csv(
  mod_out, 
  file = file.path(outcome_folder, "13a_mort_nstemi_1yr_surv30_mod1to5.csv")
)
write.csv(
  anova_1to5, 
  file.path(outcome_folder, "13a_mort_nstemi_1yr_surv30_anova1to5.csv")
)

# Combine models 5 (binary) and 6

mod_5_bin_out <- glmer_out(mod_5_bin, prefix = "mod5_bin_")
mod_6_out <- glmer_out(mod_6, prefix = "mod6_")

dim(mod_6_out)  

mod_out_int <- 
  mod_6_out %>%
  left_join(mod_5_bin_out) %>%
  select(
    covariate, 
    mod5_bin_or_fmt, mod6_or_fmt,
    mod5_bin_or, mod5_bin_or_low, mod5_bin_or_upp,
    mod6_or, mod6_or_low, mod6_or_upp
  )

dim(mod_out_int)

write.csv(
  mod_out_int, 
  file = file.path(outcome_folder, "13a_mort_nstemi_1yr_surv30_mod_int.csv")
)

anova_int <- anova(mod_5_bin, mod_6)

write.csv(
  anova_int,
  file = file.path(outcome_folder, "13a_mort_nstemi_1yr_surv30_anova_int.csv")
) 

