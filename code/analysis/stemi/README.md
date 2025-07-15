# Analysis code for the STEMI cohort

03_mort_describe_data_STEMI.Rmd creates the descriptive tables and figures.

The remainder of the scripts run the mixed effects logistic regression (or Cox proportional hazards) models. There is one pair of scripts for each outcome and within each pair of scripts, (a) runs the main analysis and (b) runs the sensitivity analysis. The number at the start of each file name corresponds to the outcome as follows:

20. 30-day mortality
21. 1-year mortality
22. 1-year mortality (amongst people discharged home)
23. 1-year mortaltiy (amongst people who survived 30 days)
24. Time to mortality within the first year
25. Time to CVD mortality within the first year
