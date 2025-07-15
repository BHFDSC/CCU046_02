# Analysis code for the STEMI cohort

03_mort_describe_data_STEMI.Rmd creates the descriptive tables and figures.

The remainder of the scripts run the mixed effects logistic regression (or Cox proportional hazards) models. There is one pair of scripts for each outcome and within each pair of scripts, (a) runs the main analysis and (b) runs the sensitivity analysis. The number at the start of each file name corresponds to the outcome as follows:

10. 30-day mortality
11. 1-year mortality
12. 1-year mortality (amongst people discharged home)
13. 1-year mortaltiy (amongst people who survived 30 days)
14. Time to mortality within the first year
15. Time to CVD mortality within the first year
