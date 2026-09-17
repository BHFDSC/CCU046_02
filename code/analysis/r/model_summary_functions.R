#
# Model summary functions
#
# K. Fleetwood
# Last updated: 25 Oct 2024
#

#
# glmer_out -------------------------------------------------------------------
#

glmer_out <- function(glmer_obj, ci_method = "Wald", prefix = ""){
  
  mod_sum <- summary(glmer_obj)
  
  # Confidence intervals for fixed effects only
  mod_ci <- 
    confint(
      glmer_obj, 
      method = ci_method, 
      parm = "beta_"
    ) 
  
  mod_out <- 
    as.data.frame(
      cbind(
        exp(mod_sum$coefficients[, "Estimate"]), 
        exp(mod_ci)
      )
    )
  
  colnames(mod_out) <- c("or", "or_low", "or_upp")
  
  mod_out <- 
    mod_out %>%
    mutate(
      or_fmt = paste0(
        format(round(or, digits = 2), nsmall = 2), " (",
        format(round(or_low, digits = 2), nsmall = 2), ", ",
        format(round(or_upp, digits = 2), nsmall = 2), ")"
      )
    ) %>%
    rename_with(~paste0(prefix, .x, recycle0 = TRUE)) %>%
    rownames_to_column(var = "covariate")
  
  mod_out
  
}

#
# coxme_out -------------------------------------------------------------------
#

coxme_out <- function(coxme_obj, prefix = ""){

  # Confidence intervals for fixed effects only
  mod_ci <- 
    confint(
      coxme_obj
    ) 
  
  mod_out <- 
    as.data.frame(
      cbind(
        exp(coxme_obj$coefficients), 
        exp(mod_ci)
      )
    )
  
  colnames(mod_out) <- c("hr", "hr_low", "hr_upp")
  
  mod_out <- 
    mod_out %>%
    mutate(
      hr_fmt = paste0(
        format(round(hr, digits = 2), nsmall = 2), " (",
        format(round(hr_low, digits = 2), nsmall = 2), ", ",
        format(round(hr_upp, digits = 2), nsmall = 2), ")"
      )
    ) %>%
    rename_with(~paste0(prefix, .x, recycle0 = TRUE)) %>%
    rownames_to_column(var = "covariate")
  
  mod_out
  
}
