#
# Addtional tableby functions
#
# K.Fleetwood
# date started: 19 Jan 24
#

#
# 1. n_per_miss
#
n_per_miss <- function(x, ...){
  n_miss <- sum(is.na(x))
  N <- length(x)
  per <- 100*n_miss/N
  paste0(n_miss, " (", format(per, digits=1, nsmall=1), "%)")
}

n_per_miss_5 <- function(x, ...){
  
  n_miss <- sum(is.na(x))
  N <- length(x)
  
  n_miss_rnd <- 5*round(n_miss/5) 
  N_rnd <- 5*round(N/5) 
  
  per_rnd <- 100*n_miss_rnd/N_rnd
  
  # Replace values less than 10
  per_cut <- round(100*10/N_rnd, digits=1)
  per_cut <- ifelse(per_cut==0, "<0.1", paste0("<", format(per_cut, digits=1, nsmall=1)))
  
  n_miss_rnd <- ifelse(n_miss_rnd < 10, "<10", as.character(n_miss_rnd))
  per_rnd <- ifelse(n_miss_rnd < 10, per_cut, format(per_rnd, digits=1, nsmall=1))
  
  paste0(n_miss_rnd, " (", per_rnd, "%)")
}

#
# 2. countpct_5
#

# Update countpct function with /5
countpct_5 <- 
  function (x, levels = NULL, na.rm = TRUE, weights = NULL, ...) 
  {
    if (is.null(levels)) 
      levels <- sort(unique(x))
    if (na.rm) {
      idx <- !is.na(x)
      if (!is.null(weights)) 
        idx <- idx & !is.na(weights)
      x <- x[idx]
      weights <- weights[idx]
    }
    if (is.selectall(x)) {
      if (is.null(weights)) 
        weights <- rep(1, nrow(x))
      wtbl <- apply(as.matrix(x) == 1, 2, function(y) sum(weights[y]))
      denom <- sum(weights)
    }
    else {
      # Round table
      wtbl <- arsenal:::wtd.table(factor(x, levels = levels), weights = weights)
      wtbl_rnd <- 5*round(wtbl/5)
      
      # Round denominator
      denom <- 5*round(sum(wtbl)/5)
      
      # Replace values less than 10
      per_rnd <- 100 * wtbl_rnd/denom

      per_cut <- round(100*10/denom, digits=1)
      per_cut <- ifelse(per_cut==0, 0.1, per_cut)
      
      wtbl_rnd[wtbl<10] <- -10
      per_rnd[wtbl<10] <- -per_cut
    }
    as.tbstat_multirow(
      lapply(
        Map(
          c, 
          wtbl_rnd, if (any(wtbl > 0)) 
      per_rnd
      else rep(list(NULL), times = length(wtbl))), as.countpct, 
      parens = c("(", ")"), pct = "%", which.pct = 2L)
    )
  }



medianrange_kf <- function(x, ...){
  y <- quantile(x, c(0.5, 0.25, 0.75), na.rm = TRUE)
  paste0(format(y[1], digits = 1, nsmall = 1), " [",
         format(y[2], digits = 1, nsmall = 1), ", ",
         format(y[3], digits = 1, nsmall = 1), "]")
}