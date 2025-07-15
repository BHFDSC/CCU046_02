#
# Collate mortality results
#
# K.Fleetwood
# 10 June 2025
# 

#
# 1. Set-up -------------------------------------------------------------------
#

library(tidyverse)

smi_mi_folder <- "U:\\Datastore\\CMVM\\smgphs\\groups\\v1cjack1-CSO-SMI-MI"

res_folder <- file.path(smi_mi_folder, "Results", "Mortality")
nstemi_folder <- file.path(res_folder, "NSTEMI")
stemi_folder <- file.path(res_folder, "STEMI")

#
# 2. Load and wrangle data ----------------------------------------------------
#

wrangle_res <- function(df, outcome_lab, cohort_lab, outcome = "or") {
  
  out <- 
    df %>%
    filter(
      substring(covariate, 1,3) %in% "smi"
    ) %>%
    mutate(
      smi = substring(covariate, 4, 100)
    ) %>%
    select(-X, -covariate, -ends_with("fmt")) %>%
    pivot_longer(
      !smi, 
      names_to = "model_est",
      values_to = "result"
    ) %>%
    mutate(
      model = str_split_i(model_est, paste0("_", outcome), 1),
      est = paste0(outcome, str_split_i(model_est, paste0("_", outcome), 2))
    ) %>%
    select(
      -model_est
    ) %>% 
    pivot_wider(
      names_from = est,
      values_from = result
    ) %>%
    mutate(
      outcome = outcome_lab,
      cohort = cohort_lab
    ) 
  
  if (outcome %in% "or"){
    
    out <- 
      out %>%
      select(outcome, cohort, smi, model, or, or_low, or_upp) %>%
      mutate(
        or_fmt = 
          paste0(
            format(round(or, digits = 2), nsmall = 2), " (", 
            format(round(or_low, digits = 2), nsmall = 2), ", ",
            format(round(or_upp, digits = 2), nsmall = 2), ")"
          )
      )
    
  }
  
  if (outcome %in% "hr"){
    
    out <- 
      out %>%
      select(outcome, cohort, smi, model, hr, hr_low, hr_upp) %>%
      mutate(
        hr_fmt = 
          paste0(
            format(round(hr, digits = 2), nsmall = 2), " (", 
            format(round(hr_low, digits = 2), nsmall = 2), ", ",
            format(round(hr_upp, digits = 2), nsmall = 2), ")"
          )
      )
    
  }
  
  out
  
}

# 2.1 Load NSTEMI results -----------------------------------------------------

# 30-day mortality

nstemi_30day <- read.csv(file.path(nstemi_folder, "10c_mort_nstemi_30day_mod1to5.csv"))
nstemi_30day <- wrangle_res(nstemi_30day, "30-day mortality", "CC")

nstemi_30day_sa <- read.csv(file.path(nstemi_folder, "10f_mort_nstemi_30day_mod1to4.csv"))
nstemi_30day_sa <- wrangle_res(nstemi_30day_sa, "30-day mortality", "CC (SA)")

# 1-year mortality

nstemi_1yr <- read.csv(file.path(nstemi_folder, "11c_mort_nstemi_1yr_mod1to5.csv"))
nstemi_1yr <- wrangle_res(nstemi_1yr, "1-year mortality", "CC")

nstemi_1yr_sa <- read.csv(file.path(nstemi_folder, "11f_mort_nstemi_1yr_mod1to4.csv"))
nstemi_1yr_sa <- wrangle_res(nstemi_1yr_sa, "1-year mortality", "CC (SA)")

# 1-year mortality amongst people surviving 30 days

nstemi_1yr_surv30 <- read.csv(file.path(nstemi_folder, "13c_mort_nstemi_1yr_surv30_mod1to5.csv"))
nstemi_1yr_surv30 <- wrangle_res(nstemi_1yr_surv30, "1-year mortality amongst people surviving 30 days", "CC")

nstemi_1yr_surv30_sa <- read.csv(file.path(nstemi_folder, "13f_mort_nstemi_1yr_surv30_mod1to4.csv"))
nstemi_1yr_surv30_sa <- wrangle_res(nstemi_1yr_surv30_sa, "1-year mortality amongst people surviving 30 days", "CC (SA)")

# Time to mortality within the first year

nstemi_surv <- read.csv(file.path(nstemi_folder, "14c_mort_nstemi_surv_mod1to5.csv"))
nstemi_surv <- wrangle_res(nstemi_surv, "Time to mortality within the first year", "CC", outcome = "hr")

nstemi_surv_sa <- read.csv(file.path(nstemi_folder, "14f_mort_nstemi_surv_mod1to4.csv"))
nstemi_surv_sa <- wrangle_res(nstemi_surv_sa, "Time to mortality within the first year", "CC (SA)", outcome = "hr")

# Time to CVD mortality within the first year

nstemi_cvd_surv <- read.csv(file.path(nstemi_folder, "15c_mort_nstemi_cvd_surv_mod1to5.csv"))
nstemi_cvd_surv <- wrangle_res(nstemi_cvd_surv, "Time to CVD mortality within the first year", "CC", outcome = "hr")

nstemi_cvd_surv_sa <- read.csv(file.path(nstemi_folder, "15f_mort_nstemi_cvd_surv_mod1to4.csv"))
nstemi_cvd_surv_sa <- wrangle_res(nstemi_cvd_surv_sa, "Time to CVD mortality within the first year", "CC (SA)", outcome = "hr")

# 1-year mortality amongst people discharged home

nstemi_1yr_home <- read.csv(file.path(nstemi_folder, "12c_mort_nstemi_1yr_home_mod_care_v2.csv"))
nstemi_1yr_home <- wrangle_res(nstemi_1yr_home, "1-year mortality amongst people discharged home", "CC")

nstemi_1yr_home_sa <- read.csv(file.path(nstemi_folder, "12f_mort_nstemi_1yr_home_mod1to4_care.csv"))
nstemi_1yr_home_sa <- wrangle_res(nstemi_1yr_home_sa, "1-year mortality amongst people discharged home", "CC (SA)")

# 2.2 Load STEMI results ------------------------------------------------------

# 30-day mortality

stemi_30day <- read.csv(file.path(stemi_folder, "30c_mort_stemi_30day_mod1to5.csv"))
stemi_30day <- wrangle_res(stemi_30day, "30-day mortality", "CC")

stemi_30day_sa <- read.csv(file.path(stemi_folder, "30f_mort_stemi_30day_mod1to4.csv"))
stemi_30day_sa <- wrangle_res(stemi_30day_sa, "30-day mortality", "CC (SA)")

# 1-year mortality

stemi_1yr <- read.csv(file.path(stemi_folder, "31c_mort_stemi_1yr_mod1to5.csv"))
stemi_1yr <- wrangle_res(stemi_1yr, "1-year mortality", "CC")

stemi_1yr_sa <- read.csv(file.path(stemi_folder, "31f_mort_stemi_1yr_mod1to4.csv"))
stemi_1yr_sa <- wrangle_res(stemi_1yr_sa, "1-year mortality", "CC (SA)")

# 1-year mortality amongst people surviving 30 days

stemi_1yr_surv30 <- read.csv(file.path(stemi_folder, "33c_mort_stemi_1yr_surv30_mod1to5.csv"))
stemi_1yr_surv30 <- wrangle_res(stemi_1yr_surv30, "1-year mortality amongst people surviving 30 days", "CC")

stemi_1yr_surv30_sa <- read.csv(file.path(stemi_folder, "33f_mort_stemi_1yr_surv_mod1to4.csv"))
stemi_1yr_surv30_sa <- wrangle_res(stemi_1yr_surv30_sa, "1-year mortality amongst people surviving 30 days", "CC (SA)")

# Time to mortality within the first year

stemi_surv <- read.csv(file.path(stemi_folder, "34c_mort_stemi_surv_mod1to5.csv"))
stemi_surv <- wrangle_res(stemi_surv, "Time to mortality within the first year", "CC", outcome = "hr")

stemi_surv_sa <- read.csv(file.path(stemi_folder, "34f_mort_stemi_surv_mod1to4.csv"))
stemi_surv_sa <- wrangle_res(stemi_surv_sa, "Time to mortality within the first year", "CC (SA)", outcome = "hr")

# Time to CVD mortality within the first year

stemi_cvd_surv <- read.csv(file.path(stemi_folder, "35c_mort_stemi_cvd_surv_mod1to5.csv"))
stemi_cvd_surv <- wrangle_res(stemi_cvd_surv, "Time to CVD mortality within the first year", "CC", outcome = "hr")

stemi_cvd_surv_sa <- read.csv(file.path(stemi_folder, "35f_mort_stemi_cvd_surv_mod1to4.csv"))
stemi_cvd_surv_sa <- wrangle_res(stemi_cvd_surv_sa, "Time to CVD mortality within the first year", "CC (SA)", outcome = "hr")

# 1-year mortality amongst people discharged home

stemi_1yr_home <- read.csv(file.path(stemi_folder, "32c_mort_stemi_1yr_home_mod_care_v2.csv"))
stemi_1yr_home <- wrangle_res(stemi_1yr_home, "1-year mortality amongst people discharged home", "CC")

stemi_1yr_home_sa <- read.csv(file.path(stemi_folder, "32f_mort_stemi_1yr_home_mod1to4_care.csv"))
stemi_1yr_home_sa <- wrangle_res(stemi_1yr_home_sa, "1-year mortality amongst people discharged home", "CC (SA)")

#
# 3. Create collated results data frame ---------------------------------------
#

# 3.1 NSTEMI ------------------------------------------------------------------

# Long version for plotting
nstemi_long <- 
  bind_rows(
    nstemi_30day,
    nstemi_30day_sa,
    nstemi_1yr, 
    nstemi_1yr_sa, 
    nstemi_1yr_surv30, 
    nstemi_1yr_surv30_sa, 
    nstemi_surv,
    nstemi_surv_sa,
    nstemi_cvd_surv, 
    nstemi_cvd_surv_sa,
    nstemi_1yr_home, 
    nstemi_1yr_home_sa
  )

write.csv(
  nstemi_long, 
  file.path(res_folder, "nstemi_collated_results_long_v2.csv")
)


# 3.2 STEMI -------------------------------------------------------------------

stemi_long <- 
  bind_rows(
    stemi_30day,
    stemi_30day_sa,
    stemi_1yr,
    stemi_1yr_sa, 
    stemi_1yr_surv30,
    stemi_1yr_surv30_sa,
    stemi_surv,
    stemi_surv_sa,
    stemi_cvd_surv,
    stemi_cvd_surv_sa,
    stemi_1yr_home,
    stemi_1yr_home_sa
  )

write.csv(
  stemi_long, 
  file.path(res_folder, "stemi_collated_results_long_v2.csv")
)
