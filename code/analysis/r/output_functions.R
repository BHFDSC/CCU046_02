#
# Output functions
#
# Functions to round and suppress counts as per Disclosure Control Rules
#
# K.Fleetwood
# 11 Jul 2024
# 

#
# 1. Round to the nearest 5 ---------------------------------------------------
#

rnd5 <- 
  function(n){
  
    out <- 5*round(n/5)
    out
    
  }

#
# 2. Format: n (%) ------------------------------------------------------------
#

# Format n and percent, suppressing values of n less than 10

# n: unrounded
# N: unrounded
# dgts: number of digits to include after the decimal place

n_per_fmt <- 
  function(n, N, dgts = 1){
    
    n_flag_10 <- n < 10
    n_rnd <- ifelse(n_flag_10, 10, rnd5(n))
    N_rnd <- rnd5(N)
    
    per <- format(round(100*n_rnd/N_rnd, digits = dgts), nsmall = dgts)
    
    out <-  
      paste0(
        ifelse(n_flag_10, "<10", format(n_rnd, , big.mark = ",")), 
        " (", 
        ifelse(n_flag_10, paste0("<", per), per), 
        "%)"
      )
    
    out
    
  }
