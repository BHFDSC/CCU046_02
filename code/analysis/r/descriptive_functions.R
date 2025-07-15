# Function to create summary of cohort by smi
smi_fun <- function(df) {
  
  df_n <- nrow(df)
  
  out <- 
    df %>% 
    tabyl(smi) %>% 
    adorn_pct_formatting() %>%
    mutate(
      n_rnd = rnd5(n),
      per_rnd = round(100*n_rnd/df_n, digits=1),
      n_rnd_fmt = paste0(n_rnd, " (", per_rnd, "%)")
    )
  
  out <- pull(out, n_rnd_fmt)
  
  out
  
}

# Function to turn survfit object into dataframe
as.df.sf <- function(x){ 
  
  x0 <- survfit0(x)
  
  x_out <- 
    data.frame(
      time = x0$time,
      surv = x0$surv,
      std.err = x0$std.err,
      lower = x0$lower,
      upper = x0$upper,
      strata_id = rep(1:length(x0$strata), x0$strata),
      strata = levels(summary(x0)$strata)[rep(1:length(x0$strata), x0$strata)]
    )
  
  x_out
  
}
