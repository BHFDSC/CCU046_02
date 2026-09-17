#
# Mental illness, mortality following myocardial infarction and the impact of
# the COVID-19 pandemic in England: a cohort study
#
# Publication figures
# 
# K. Fleetwood
# 9 June 2025
# 

#
# 1. Set-up -------------------------------------------------------------------
#

library(tidyverse)
library(janitor)
library(ggh4x)
library(rcartocolor)
library(pals)

proj_folder <- "U:\\Datastore\\CMVM\\smgphs\\groups\\v1cjack1-CSO-SMI-MI\\"

res_folder <- file.path(proj_folder, "Results", "Mortality")
nstemi_folder <- file.path(res_folder, "NSTEMI")
stemi_folder <- file.path(res_folder, "STEMI")
  
pub_folder <- file.path(proj_folder, "Publications", "Outcomes")

#
# 2. Outcomes over time -------------------------------------------------------
#

# 2.1 Load data ---------------------------------------------------------------

# NSTEMI: monthly -------------------------------------------------------------
nstemi_month <-
  read.csv(file.path(res_folder, "02_nstemi_month.csv"))

# Define outcome for combining with STEMI
nstemi_month <-
  nstemi_month %>%
  mutate(
    out_comb =
      case_when(
        outcome %in% "MI admissions (N)" ~ "NSTEMI admissions (N)",
        outcome %in% "1-year mortality (amongst people surviving 30 days) (%)" ~ "NSTEMI: 1-year mortality\n(amongst people surviving 30 days) (%)",
        outcome %in% "1-year mortality (amongst people discharged home) (%)" ~ "NSTEMI: 1-year mortality\n(amongst people discharged home) (%)",
        TRUE ~ paste("NSTEMI:", outcome)
      )
  )
nstemi_month %>% tabyl(out_comb)

# NSTEMI: 4-monthly periods by SMI --------------------------------------------

nstemi_per_smi <- 
  read.csv(file.path(res_folder, "02_nstemi_period_smi_bin.csv"))

# Define outcome for combining with STEMI
nstemi_per_smi <- 
  nstemi_per_smi %>%
  mutate(
    out_comb = 
      case_when(
        outcome %in% "MI admissions (N)" ~ "NSTEMI admissions (N)",
        outcome %in% "1-year mortality (amongst people surviving 30 days) (%)" ~ "NSTEMI: 1-year mortality\n(amongst people surviving 30 days) (%)",
        outcome %in% "1-year mortality (amongst people discharged home) (%)" ~ "NSTEMI: 1-year mortality\n(amongst people discharged home) (%)",
        TRUE ~ paste("NSTEMI:", outcome)
      )
  )

# STEMI: monthly --------------------------------------------------------------
stemi_month <-
  read.csv(file.path(res_folder, "02_stemi_month.csv"))

stemi_month <-
  stemi_month %>%
  mutate(
    out_comb =
      case_when(
        outcome %in% "MI admissions (N)" ~ "STEMI admissions (N)",
        outcome %in% "1-year mortality (amongst people surviving 30 days) (%)" ~ "STEMI: 1-year mortality\n(amongst people surviving 30 days) (%)",
        outcome %in% "1-year mortality (amongst people discharged home) (%)" ~ "STEMI: 1-year mortality\n(amongst people discharged home) (%)",
        TRUE ~ paste("STEMI:", outcome)
      )
  )
stemi_month %>% tabyl(out_comb)

# STEMI: 4-monthly periods by SMI ---------------------------------------------

stemi_per_smi <- 
  read.csv(
    file.path(res_folder, "02_stemi_period_smi_bin.csv")
  )

stemi_per_smi <- 
  stemi_per_smi %>%
  mutate(
    out_comb = 
      case_when(
        outcome %in% "MI admissions (N)" ~ "STEMI admissions (N)",
        outcome %in% "1-year mortality (amongst people surviving 30 days) (%)" ~ "STEMI: 1-year mortality\n(amongst people surviving 30 days) (%)",
        outcome %in% "1-year mortality (amongst people discharged home) (%)" ~ "STEMI: 1-year mortality\n(amongst people discharged home) (%)",
        TRUE ~ paste("STEMI:", outcome)
      )
  )


# 2.2 Combined monthly plot -----------------------------------------------------

out_comb_lvls <-
  c("NSTEMI admissions (N)", "NSTEMI: 30-day mortality (%)",
    "NSTEMI: 1-year mortality (%)",
    "NSTEMI: 1-year mortality\n(amongst people surviving 30 days) (%)",
    "NSTEMI: 1-year CVD mortality (%)",
    "NSTEMI: 1-year mortality\n(amongst people discharged home) (%)",
    "STEMI admissions (N)", "STEMI: 30-day mortality (%)",
    "STEMI: 1-year mortality (%)",
    "STEMI: 1-year mortality\n(amongst people surviving 30 days) (%)",
    "STEMI: 1-year CVD mortality (%)",
    "STEMI: 1-year mortality\n(amongst people discharged home) (%)")

mi_month <- bind_rows(nstemi_month, stemi_month)
mi_month <-
  mi_month %>%
  mutate(
    mi_event_month = as.Date(mi_event_month),
    out_comb = factor(out_comb, levels = out_comb_lvls)
  )

month_plot <-
  ggplot(mi_month, aes(mi_event_month, y = n_per)) +
  geom_line() +
  facet_wrap(
    vars(out_comb),
    ncol = 2,
    scales = "free_y",
    strip.position = "top",
    dir = "v"
  ) +
  scale_x_date(
    date_labels = "%h %Y",
    expand = c(0,0)
  ) +
  theme_bw() +
  theme(
    strip.background = element_rect(fill = "white"),
    panel.spacing = unit(0.4, 'lines'),
    plot.margin = unit(c(1,1,1,1), "cm") # top, right, bottom, left
  ) +
  labs(
    x = "Month",
    y = ""
  )

# Save: A4 size
ggsave(
  month_plot,
  file = file.path(pub_folder, "outcomes_over_time.pdf"),
  width = 21, height = 29.7, units = "cm"
)

# 2.3 Combined 4-monthly by SMI plot -----------------------------------------------

mi_per_smi <- bind_rows(nstemi_per_smi, stemi_per_smi)
mi_per_smi <- 
  mi_per_smi %>%
  mutate(
    out_comb = factor(out_comb, levels = out_comb_lvls),
    period_4_date = as.Date(period_4_date),
    smi_bin = ifelse(smi_bin %in% "Mental illness", "Mental disorder", "No mental disorder"),
    smi_bin = factor(smi_bin, levels = c("Mental disorder", "No mental disorder"))
  )

per_smi_plot <- 
  ggplot(mi_per_smi, aes(x = period_4_date, y = n_per, color = smi_bin)) + 
  geom_line() + 
  facet_wrap(
    vars(out_comb),
    ncol = 2,
    scales = "free_y",
    strip.position = "top",
    dir = "v"
  ) + 
  scale_x_date(
    date_labels = "%h %Y",
    expand = c(0,0)
  ) + 
  theme_bw() +
  theme(
    strip.background = element_rect(fill = "white"),
    panel.spacing = unit(0.4, 'lines'),
    plot.margin = unit(c(1,1,1,1), "cm"), # top, right, bottom, left
    legend.position="bottom",
    legend.title=element_blank()
  ) +
  labs(
    x = "Month",
    y = ""
  )

# Save: A4 size
ggsave(
  per_smi_plot,
  file = file.path(pub_folder, "outcomes_over_time_by_smi.pdf"),
  width = 21, height = 29.7, units = "cm"
)

#
# 3. Forest plot of model estimates (primary analysis) ------------------------
#

# 3.1 Load data ---------------------------------------------------------------

nstemi_long <- 
  read.csv(file.path(res_folder, "nstemi_collated_results_long_v2.csv"))

stemi_long <- 
  read.csv(file.path(res_folder, "stemi_collated_results_long_v2.csv"))

jitter <- 0.1

# 3.2 Forest plot for NSTEMI --------------------------------------------------

nstemi_long <-
  nstemi_long %>%
  mutate(
    pos_out =
      case_when(
        outcome %in% "30-day mortality"                                                                  ~ 43,
        outcome %in% "1-year mortality"                                                                  ~ 35,
        outcome %in% "1-year mortality amongst people surviving 30 days"                                 ~ 27,
        outcome %in% "1-year mortality amongst people discharged home"               ~ 19,
        outcome %in% "Time to mortality within the first year"                      ~ 3,
        outcome %in% "Time to CVD mortality within the first year"            ~ -5, 
      ),
    pos_model =
      case_when(
        model %in% "mod1" ~ 2,
        model %in% "mod2" ~ 1,
        model %in% "mod3" ~  0,
        model %in% "mod4" ~ -1,
        model %in% "mod5" ~ -2,
        model %in% "mod5_ang" ~ -3,
        model %in% "mod5_ang_reh" ~ -4,
        model %in% "mod5_ang_reh_prv" ~ -5,
        model %in% "mod5_care" ~ -6,
      ),
    pos = pos_out + pos_model,
    smi = factor(smi, levels = c("Schizophrenia", "Bipolar disorder", "Depression")),
    model = 
      case_when(
        nchar(model) == 4              ~ substring(model, 4, 4),
        model %in% "mod5_ang"          ~ "5a", 
        model %in% "mod5_ang_reh"      ~ "5b",
        model %in% "mod5_ang_reh_prv"  ~ "5c",
        model %in% "mod5_care"         ~ "5d",
      ),
    hr_or = ifelse(!is.na(or),     "or",     "hr"),
    hr_or = factor(hr_or, levels = c("or", "hr")),
    or     = ifelse(!is.na(or),     or,     hr),
    or_low = ifelse(!is.na(or_low), or_low, hr_low),
    or_upp = ifelse(!is.na(or_upp), or_upp, hr_upp),
    or_fmt = ifelse(!is.na(or_fmt), or_fmt, hr_fmt)
  )

mod_cols <- carto_pal(9, "Safe")[c(5,2,3,4,1,6:9)]

nstemi_forest <-
  ggplot(
    filter(nstemi_long, cohort == "CC"),
    aes(or, pos, col = model, label = or_fmt)
  ) +
  facet_grid(
    hr_or ~ smi,
    scales = "free_y",
    space = "free_y",
    axes = "all",
    axis.labels = "all_x"
  ) +
  geom_vline(xintercept = 1, col="lightgrey") +
  geom_point() +
  geom_text(vjust=-0.5, show.legend = FALSE, size = 3) +
  geom_errorbarh(aes(xmin = or_low, xmax = or_upp, height = 0)) +
  scale_x_log10(
    name = "Hazard ratio (95% CI)",
    limits = c(NA, 3.3)
  ) +
  scale_color_manual(values = mod_cols) + 
  scale_y_continuous(
    minor_breaks = c(7, 15, 23, 31, 39),
    breaks = c(43, 35, 27, 19, 11, 3),
    labels = c("Angiogram\neligibility", "Angiogram\nreceipt", "Angiogram\nwithin\n72 hours",
               "Admission\nto cardiac\nward", "Referral\nfor cardiac\nrehab", "Receipt of all\nsecondary\nprevention\nmedication")
  ) +
  theme_bw() +
  theme(
    panel.grid.major.x = element_blank(),
    panel.grid.minor.x = element_blank(),
    panel.grid.major.y = element_blank(),
    axis.title.y = element_blank(),#,
    plot.background = element_rect(fill = "white"),
    strip.background =element_rect(fill="white"),
    strip.text.y = element_blank(),
    panel.spacing.y = unit(2.5, 'lines')
  ) + 
  facetted_pos_scales(
    y = list(
      scale_y_continuous(
        #limits = c(9, 45),
        minor_breaks = c(23, 31, 39),
        breaks = c(43, 35, 27, 18),
        labels = c("30-day mortality", "1-year mortality", "1-year mortality\n(patients\nsurviving\n30 days)", 
                   "1-year mortality\n(patients\ndischarged\nhome)")
      ),
      scale_y_continuous(
        limits = c(-7, 6),
        minor_breaks = c(-1, 7, 15, 23, 31, 39),
        breaks = c(3, -5),
        labels = c( "Time to\nmortality\nwithin the\nfirst year", "Time to\nCVD mortality\nwithin the\nfirst year")
      )
    )
  )

ggsave(
  file.path(pub_folder, "nstemi_forest.pdf"),
  width = 21, height = 29.7, units = "cm"
)

# 3.3 Forest plot for STEMI ---------------------------------------------------

stemi_long <-
  stemi_long %>%
  mutate(
    pos_out =
      case_when(
        outcome %in% "30-day mortality"                                                                  ~ 43,
        outcome %in% "1-year mortality"                                                                  ~ 35,
        outcome %in% "1-year mortality amongst people surviving 30 days"                                 ~ 27,
        outcome %in% "1-year mortality amongst people discharged home"               ~ 19,
        outcome %in% "Time to mortality within the first year"                      ~ 3,
        outcome %in% "Time to CVD mortality within the first year"            ~ -5, 
      ),
    pos_model =
      case_when(
        model %in% "mod1" ~ 2,
        model %in% "mod2" ~ 1,
        model %in% "mod3" ~  0,
        model %in% "mod4" ~ -1,
        model %in% "mod5" ~ -2,
        model %in% "mod5_ctb_bin" ~ -3,
        model %in% "mod5_ctb" ~ -4,
        model %in% "mod5_ctb_reh" ~ -5,
        model %in% "mod5_care" ~ -6,
      ),
    pos = pos_out + pos_model,
    smi = factor(smi, levels = c("Schizophrenia", "Bipolar disorder", "Depression")),
    model = 
      case_when(
        nchar(model) == 4              ~ substring(model, 4, 4),
        model %in% "mod5_ctb_bin"      ~ "5a", 
        model %in% "mod5_ctb"          ~ "5b",
        model %in% "mod5_ctb_reh"      ~ "5c",
        model %in% "mod5_care"         ~ "5d",
      ),
    hr_or = ifelse(!is.na(or),     "or",     "hr"),
    hr_or = factor(hr_or, levels = c("or", "hr")),
    or     = ifelse(!is.na(or),     or,     hr),
    or_low = ifelse(!is.na(or_low), or_low, hr_low),
    or_upp = ifelse(!is.na(or_upp), or_upp, hr_upp),
    or_fmt = ifelse(!is.na(or_fmt), or_fmt, hr_fmt)
  )

mod_cols <- carto_pal(9, "Safe")[c(5,2,3,4,1,6:9)]

stemi_forest <-
  ggplot(
    filter(stemi_long, cohort == "CC"),
    aes(or, pos, col = model, label = or_fmt)
  ) +
  facet_grid(
    hr_or ~ smi,
    scales = "free_y",
    space = "free_y",
    axes = "all",
    axis.labels = "all_x"
  ) +
  geom_vline(xintercept = 1, col="lightgrey") +
  geom_point() +
  geom_text(vjust=-0.5, show.legend = FALSE, size = 3) +
  geom_errorbarh(aes(xmin = or_low, xmax = or_upp, height = 0)) +
  scale_x_log10(
    name = "Hazard ratio (95% CI)",
    limits = c(NA, 4.25)
  ) +
  scale_color_manual(values = mod_cols) + 
  theme_bw() +
  theme(
    panel.grid.major.x = element_blank(),
    panel.grid.minor.x = element_blank(),
    panel.grid.major.y = element_blank(),
    axis.title.y = element_blank(),#,
    plot.background = element_rect(fill = "white"),
    strip.background =element_rect(fill="white"),
    strip.text.y = element_blank(),
    panel.spacing.y = unit(2.5, 'lines')
  ) + 
  facetted_pos_scales(
    y = list(
      scale_y_continuous(
        #limits = c(9, 45),
        minor_breaks = c(23, 31, 39),
        breaks = c(43, 35, 27, 18),
        labels = c("30-day mortality", "1-year mortality", "1-year mortality\n(patients\nsurviving\n30 days)", 
                   "1-year mortality\n(patients\ndischarged\nhome)")
      ),
      scale_y_continuous(
        limits = c(-7, 6),
        minor_breaks = c(-1, 7, 15, 23, 31, 39),
        breaks = c(3, -5),
        labels = c( "Time to\nmortality\nwithin the\nfirst year", "Time to\nCVD mortality\nwithin the\nfirst year")
      )
    )
  )

ggsave(
  file.path(pub_folder, "stemi_forest.pdf"),
  width = 21, height = 29.7, units = "cm"
)

#
# 4. Simplified plot of model estimates ---------------------------------------
#

# 4.1 Load data ---------------------------------------------------------------

nstemi_long <- 
  read.csv(file.path(res_folder, "nstemi_collated_results_long.csv"))

stemi_long <- 
  read.csv(file.path(res_folder, "stemi_collated_results_long.csv"))

jitter <- 0.1

# 4.2 Forest plot for NSTEMI (initial version PPT / graphical abstract) ----

nstemi_long <- 
  nstemi_long %>%
  filter(cohort %in% "CC", model %in% c("mod5", "mod5_care")) %>%
  mutate(
    pos = 
      case_when(
        outcome %in% "30-day mortality"                                                                  ~ 43,
        outcome %in% "1-year mortality"                                                                  ~ 35,
        outcome %in% "1-year mortality amongst people surviving 30 days"                                 ~ 27,
        outcome %in% "1-year mortality amongst people discharged home" & model %in% "mod5"               ~ 19,
        outcome %in% "1-year mortality amongst people discharged home" & model %in% "mod5_care" ~ 11,
        outcome %in% "Time to mortality within the first year"                      ~ 3,
        outcome %in% "Time to CVD mortality within the first year"            ~ -5, 

      ),
    smi = factor(smi, levels = c("Schizophrenia", "Bipolar disorder", "Depression")),
    or     = ifelse(!is.na(or),     or,     hr),
    or_low = ifelse(!is.na(or_low), or_low, hr_low),
    or_upp = ifelse(!is.na(or_upp), or_upp, hr_upp),
    or_fmt = ifelse(!is.na(or_fmt), or_fmt, hr_fmt)
  )

nstemi_forest <- 
  ggplot(
    filter(nstemi_long), 
    aes(or, pos, label = or_fmt)
  ) +
  facet_wrap(vars(smi), ncol = 3) +
  geom_vline(xintercept = 1, col="lightgrey") +
  geom_point() +
  geom_text(vjust=-0.5, show.legend = FALSE, size = 3) +
  geom_errorbarh(aes(xmin = or_low, xmax = or_upp, height = 0)) +
  scale_x_log10(
    name = "Odds ratio (or hazard ratio) (95% CI)",
    limits = c(0.6, 2.75) 
  ) +
  scale_y_continuous(
    minor_breaks = c(-1, 7, 15, 23, 31, 39),
    breaks = c(43, 35, 27, 19, 11, 3, -5),
    labels = c("30-day mortality", "1-yr mortality", "1-yr mortality\n(surviving\n30 days)", 
               "1-yr mortality\n(discharged\nhome)", "1-yr mortality\n(discharged\nhome)\n+ care", 
               "Time to\nmortality within\nthe first year\n(HR)", "Time to CVD\nmortality within\nthe first year\n(HR)")
  ) +
  theme_bw() +
  theme(
    panel.grid.major.x = element_blank(),
    panel.grid.minor.x = element_blank(),
    panel.grid.major.y = element_blank(),
    axis.title.y = element_blank(),#,
    plot.background = element_rect(fill = "white"),
    strip.background =element_rect(fill="white")
  )
nstemi_forest

ggsave(
  file.path(pub_folder, "nstemi_forest_simple_wide.pdf"), 
  width = 1.75*8.5, height = 1.75*7, units = "cm"
)

# 4.3 Forest plot for STEMI (initial version PPT / graphical abstract) --------

stemi_long <- 
  stemi_long %>%
  filter(cohort %in% "CC", model %in% c("mod5", "mod5_care")) %>%
  mutate(
    pos = 
      case_when(
        outcome %in% "30-day mortality"                                                                  ~ 43,
        outcome %in% "1-year mortality"                                                                  ~ 35,
        outcome %in% "1-year mortality amongst people surviving 30 days"                                 ~ 27,
        outcome %in% "1-year mortality amongst people discharged home" & model %in% "mod5"               ~ 19,
        outcome %in% "1-year mortality amongst people discharged home" & model %in% "mod5_care" ~ 11,
        outcome %in% "Time to mortality within the first year"                      ~ 3,
        outcome %in% "Time to CVD mortality within the first year"            ~ -5, 
        
      ),
    smi = factor(smi, levels = c("Schizophrenia", "Bipolar disorder", "Depression")),
    or     = ifelse(!is.na(or),     or,     hr),
    or_low = ifelse(!is.na(or_low), or_low, hr_low),
    or_upp = ifelse(!is.na(or_upp), or_upp, hr_upp),
    or_fmt = ifelse(!is.na(or_fmt), or_fmt, hr_fmt)
  )


stemi_forest <- 
  ggplot(
    filter(stemi_long, cohort == "CC"), 
    aes(or, pos, label = or_fmt)
  ) +
  facet_wrap(vars(smi), ncol = 3) +
  geom_vline(xintercept = 1, col="lightgrey") +
  geom_point() +
  geom_text(vjust=-0.5, show.legend = FALSE, size = 3) +
  geom_errorbarh(aes(xmin = or_low, xmax = or_upp, height = 0)) +
  scale_x_log10(
    name = "Odds ratio (or hazard ratio) (95% CI)",
    #limits = c(0.6, 2.75) 
  ) +
  scale_y_continuous(
    minor_breaks = c(-1, 7, 15, 23, 31, 39),
    breaks = c(43, 35, 27, 19, 11, 3, -5),
    labels = c("30-day mortality", "1-yr mortality", "1-yr mortality\n(surviving\n30 days)", 
               "1-yr mortality\n(discharged\nhome)", "1-yr mortality\n(discharged\nhome)\n+ care", 
               "Time to\nmortality within\nthe first year\n(HR)", "Time to CVD\nmortality within\nthe first year\n(HR)")
  ) +
  theme_bw() +
  theme(
    panel.grid.major.x = element_blank(),
    panel.grid.minor.x = element_blank(),
    panel.grid.major.y = element_blank(),
    axis.title.y = element_blank(),#,
    plot.background = element_rect(fill = "white"),
    strip.background =element_rect(fill="white")
  )
stemi_forest

ggsave(
  file.path(pub_folder, "stemi_forest_simple_wide.pdf"), 
  width = 1.75*8.5, height = 1.75*7, units = "cm"
)

#
# 5. Simplified plot of model estimates (main publication figures) ------------
#

# 5.1 Load data ---------------------------------------------------------------

nstemi_long <- 
  read.csv(file.path(res_folder, "nstemi_collated_results_long.csv"))

stemi_long <- 
  read.csv(file.path(res_folder, "stemi_collated_results_long.csv"))

jitter <- 0.1

# 5.2 Forest plot for NSTEMI --------------------------------------------------

nstemi_long <- 
  nstemi_long %>%
  filter(cohort %in% "CC", model %in% c("mod5", "mod5_care")) %>%
  mutate(
    pos = 
      case_when(
        outcome %in% "30-day mortality"                                                                  ~ 43,
        outcome %in% "1-year mortality"                                                                  ~ 35,
        outcome %in% "1-year mortality amongst people surviving 30 days"                                 ~ 27,
        outcome %in% "1-year mortality amongst people discharged home" & model %in% "mod5"               ~ 19,
        outcome %in% "1-year mortality amongst people discharged home" & model %in% "mod5_care" ~ 11,
        outcome %in% "Time to mortality within the first year"                      ~ 3,
        outcome %in% "Time to CVD mortality within the first year"            ~ -5, 
      ),
    smi = factor(smi, levels = c("Schizophrenia", "Bipolar disorder", "Depression")),
    hr_or = ifelse(!is.na(or),     "or",     "hr"),
    hr_or = factor(hr_or, levels = c("or", "hr")),
    or     = ifelse(!is.na(or),     or,     hr),
    or_low = ifelse(!is.na(or_low), or_low, hr_low),
    or_upp = ifelse(!is.na(or_upp), or_upp, hr_upp),
    or_fmt = ifelse(!is.na(or_fmt), or_fmt, hr_fmt)
  )

nstemi_forest <- 
  ggplot(
    filter(nstemi_long), 
    aes(or, pos, label = or_fmt)
  ) +
  facet_grid(
    hr_or ~ smi,
    scales = "free_y",
    space = "free_y",
    axes = "all",
    axis.labels = "all_x"
  ) +
  geom_vline(xintercept = 1, col="lightgrey") +
  geom_point() +
  geom_text(vjust=-0.5, show.legend = FALSE, size = 3) +
  geom_errorbarh(aes(xmin = or_low, xmax = or_upp, height = 0)) +
  scale_x_log10(
    name = "Hazard ratio (95% CI)",
    limits = c(0.6, 2.75) 
  ) +
  theme_bw() +
  theme(
    panel.grid.major.x = element_blank(),
    panel.grid.minor.x = element_blank(),
    panel.grid.major.y = element_blank(),
    axis.title.y = element_blank(),#,
    plot.background = element_rect(fill = "white"),
    strip.background =element_rect(fill="white"),
    strip.text.y = element_blank(),
    panel.spacing.y = unit(2.5, 'lines')
  ) + 
  facetted_pos_scales(
    y = list(
      scale_y_continuous(
        limits = c(9, 45),
        minor_breaks = c( 15, 23, 31, 39),
        breaks = c(43, 35, 27, 19, 11),
        labels = c("30-day mortality", "1-year mortality", "1-year mortality\n(patients\nsurviving\n30 days)", 
                   "1-year mortality\n(patients\ndischarged\nhome)", "1-yr mortality\n(patients\ndischarged\nhome)\n(additionally\nadjusted for\nreceipt of care)")
      ),
      scale_y_continuous(
        limits = c(-7, 5),
        minor_breaks = c(-1, 7, 15, 23, 31, 39),
        breaks = c(3, -5),
        labels = c( "Time to\nmortality\nwithin the\nfirst year", "Time to\nCVD mortality\nwithin the\nfirst year")
      )
    )
  )

ggsave(
  file.path(pub_folder, "Figure_2.pdf"), 
  width = 2.5*8.5, height = 2.5*7, units = "cm"
)

# 5.3 Forest plot for STEMI ---------------------------------------------------

stemi_long <- 
  stemi_long %>%
  filter(cohort %in% "CC", model %in% c("mod5", "mod5_care")) %>%
  mutate(
    pos = 
      case_when(
        outcome %in% "30-day mortality"                                                                  ~ 43,
        outcome %in% "1-year mortality"                                                                  ~ 35,
        outcome %in% "1-year mortality amongst people surviving 30 days"                                 ~ 27,
        outcome %in% "1-year mortality amongst people discharged home" & model %in% "mod5"               ~ 19,
        outcome %in% "1-year mortality amongst people discharged home" & model %in% "mod5_care" ~ 11,
        outcome %in% "Time to mortality within the first year"                      ~ 3,
        outcome %in% "Time to CVD mortality within the first year"            ~ -5, 
        
      ),
    smi = factor(smi, levels = c("Schizophrenia", "Bipolar disorder", "Depression")),
    hr_or = ifelse(!is.na(or),     "or",     "hr"),
    hr_or = factor(hr_or, levels = c("or", "hr")),
    or     = ifelse(!is.na(or),     or,     hr),
    or_low = ifelse(!is.na(or_low), or_low, hr_low),
    or_upp = ifelse(!is.na(or_upp), or_upp, hr_upp),
    or_fmt = ifelse(!is.na(or_fmt), or_fmt, hr_fmt)
  )

stemi_forest <- 
  ggplot(
    filter(stemi_long), 
    aes(or, pos, label = or_fmt)
  ) +
  facet_grid(
    hr_or ~ smi,
    scales = "free_y",
    space = "free_y",
    axes = "all",
    axis.labels = "all_x"
  ) +
  geom_vline(xintercept = 1, col="lightgrey") +
  geom_point() +
  geom_text(vjust=-0.5, show.legend = FALSE, size = 3) +
  geom_errorbarh(aes(xmin = or_low, xmax = or_upp, height = 0)) +
  scale_x_log10(
    name = "Hazard ratio (95% CI)"
  ) +
  theme_bw() +
  theme(
    panel.grid.major.x = element_blank(),
    panel.grid.minor.x = element_blank(),
    panel.grid.major.y = element_blank(),
    axis.title.y = element_blank(),#,
    plot.background = element_rect(fill = "white"),
    strip.background =element_rect(fill="white"),
    strip.text.y = element_blank(),
    panel.spacing.y = unit(2.5, 'lines')
  ) + 
  facetted_pos_scales(
    y = list(
      scale_y_continuous(
        limits = c(9, 45),
        minor_breaks = c( 15, 23, 31, 39),
        breaks = c(43, 35, 27, 19, 11),
        labels = c("30-day mortality", "1-year mortality", "1-year mortality\n(patients\nsurviving\n30 days)", 
                   "1-year mortality\n(patients\ndischarged\nhome)", "1-yr mortality\n(patients\ndischarged\nhome)\n(additionally\nadjusted for\nreceipt of care)")
      ),
      scale_y_continuous(
        limits = c(-7, 5),
        minor_breaks = c(-1, 7, 15, 23, 31, 39),
        breaks = c(3, -5),
        labels = c( "Time to\nmortality\nwithin the\nfirst year", "Time to\nCVD mortality\nwithin the\nfirst year")
      )
    )
  )

stemi_forest

ggsave(
  file.path(pub_folder, "Figure_3.pdf"), 
  width = 2.5*8.5, height = 2.5*7, units = "cm"
)

#
# 6. Forest plot of model estimates (sensitivity analysis) ------------------------
#

# 6.1 Load data ---------------------------------------------------------------

nstemi_long <- 
  read.csv(file.path(res_folder, "nstemi_collated_results_long_v2.csv"))

stemi_long <- 
  read.csv(file.path(res_folder, "stemi_collated_results_long_v2.csv"))

jitter <- 0.1

# 6.2 Forest plot for NSTEMI --------------------------------------------------

nstemi_long <-
  nstemi_long %>%
  filter(
    cohort %in% "CC (SA)",
    !model %in% c("mod3_ang_reh", "mod3_ang_reh_prv", "mod3_angio", "mod3_care")
  ) %>%
  mutate(
    pos_out =
      case_when(
        outcome %in% "30-day mortality"                                  ~ 40,
        outcome %in% "1-year mortality"                                  ~ 33,
        outcome %in% "1-year mortality amongst people surviving 30 days" ~ 26,
        outcome %in% "1-year mortality amongst people discharged home"   ~ 19,
        outcome %in% "Time to mortality within the first year"           ~ 3,
        outcome %in% "Time to CVD mortality within the first year"       ~ -4, 
      ),
    pos_model =
      case_when(
        model %in% "mod1" ~ 1.5,
        model %in% "mod2" ~ 0.5,
        model %in% "mod3" ~ -0.5,
        model %in% "mod4" ~ -1.5,
        model %in% "mod4_angio" ~ -2.5,
        model %in% "mod4_ang_reh" ~ -3.5,
        model %in% "mod4_ang_reh_prv" ~ -4.5,
        model %in% "mod4_care" ~ -5.5,
      ),
    pos = pos_out + pos_model,
    smi = factor(smi, levels = c("Schizophrenia", "Bipolar disorder", "Depression")),
    model = 
      case_when(
        nchar(model) == 4              ~ substring(model, 4, 4),
        model %in% "mod4_angio"          ~ "4a", 
        model %in% "mod4_ang_reh"      ~ "4b",
        model %in% "mod4_ang_reh_prv"  ~ "4c",
        model %in% "mod4_care"         ~ "4d",
      ),
    hr_or = ifelse(!is.na(or),     "or",     "hr"),
    hr_or = factor(hr_or, levels = c("or", "hr")),
    or     = ifelse(!is.na(or),     or,     hr),
    or_low = ifelse(!is.na(or_low), or_low, hr_low),
    or_upp = ifelse(!is.na(or_upp), or_upp, hr_upp),
    or_fmt = ifelse(!is.na(or_fmt), or_fmt, hr_fmt)
  )

mod_cols <- carto_pal(9, "Safe")[c(5,2,3,4,6:9)]

nstemi_forest <-
  ggplot(
    nstemi_long, aes(or, pos, col = model, label = or_fmt)
  ) +
  facet_grid(
    hr_or ~ smi,
    scales = "free_y",
    space = "free_y",
    axes = "all",
    axis.labels = "all_x"
  ) +
  geom_vline(xintercept = 1, col="lightgrey") +
  geom_point() +
  geom_text(vjust=-0.5, show.legend = FALSE, size = 3) +
  geom_errorbarh(aes(xmin = or_low, xmax = or_upp, height = 0)) +
  scale_x_log10(
    name = "Hazard ratio (95% CI)",
    limits = c(0.7, 4)
  ) +
  scale_color_manual(values = mod_cols) + 
  theme_bw() +
  theme(
    panel.grid.major.x = element_blank(),
    panel.grid.minor.x = element_blank(),
    panel.grid.major.y = element_blank(),
    axis.title.y = element_blank(),#,
    plot.background = element_rect(fill = "white"),
    strip.background =element_rect(fill="white"),
    strip.text.y = element_blank(),
    panel.spacing.y = unit(2.5, 'lines')
  ) + 
  facetted_pos_scales(
    y = list(
      scale_y_continuous(
        minor_breaks = c(22.5, 29.5, 36.5),
        breaks = c(40, 33, 26, 19),
        labels = c("30-day mortality", "1-year mortality", "1-year mortality\n(patients\nsurviving\n30 days)", 
                   "1-year mortality\n(patients\ndischarged\nhome)")
      ),
      scale_y_continuous(
        limits = c(-6, 6),
        minor_breaks = c(-0.5),
        breaks = c(3, -4),
        labels = c( "Time to\nmortality\nwithin the\nfirst year", "Time to\nCVD mortality\nwithin the\nfirst year")
      )
    )
  )

nstemi_forest

ggsave(
  file.path(pub_folder, "nstemi_forest_sa.pdf"),
  width = 21, height = 29.7, units = "cm"
)

# 6.3 Forest plot for STEMI ---------------------------------------------------

stemi_long <-
  stemi_long %>%
  filter(
    cohort %in% "CC (SA)",
    !model %in% c("mod3_ctb_bin", "mod3_ctb", "mod3_ctb_reh", "mod3_care")
  ) %>%
  mutate(
    pos_out =
      case_when(
        outcome %in% "30-day mortality"                                  ~ 40,
        outcome %in% "1-year mortality"                                  ~ 33,
        outcome %in% "1-year mortality amongst people surviving 30 days" ~ 26,
        outcome %in% "1-year mortality amongst people discharged home"   ~ 19,
        outcome %in% "Time to mortality within the first year"           ~ 3,
        outcome %in% "Time to CVD mortality within the first year"       ~ -4, 
      ),
    pos_model =
      case_when(
        model %in% "mod1" ~  1.5,
        model %in% "mod2" ~  0.5,
        model %in% "mod3" ~ -0.5,
        model %in% "mod4" ~ -1.5,
        model %in% "mod4_ctb_bin" ~ -2.5,
        model %in% "mod4_ctb" ~ -3.5,
        model %in% "mod4_ctb_reh" ~ -4.5,
        model %in% "mod4_care" ~ -5.5,
      ),
    pos = pos_out + pos_model,
    smi = factor(smi, levels = c("Schizophrenia", "Bipolar disorder", "Depression")),
    model = 
      case_when(
        nchar(model) == 4              ~ substring(model, 4, 4),
        model %in% "mod4_ctb_bin"      ~ "4a", 
        model %in% "mod4_ctb"          ~ "4b",
        model %in% "mod4_ctb_reh"      ~ "4c",
        model %in% "mod4_care"         ~ "4d",
      ),
    hr_or = ifelse(!is.na(or),     "or",     "hr"),
    hr_or = factor(hr_or, levels = c("or", "hr")),
    or     = ifelse(!is.na(or),     or,     hr),
    or_low = ifelse(!is.na(or_low), or_low, hr_low),
    or_upp = ifelse(!is.na(or_upp), or_upp, hr_upp),
    or_fmt = ifelse(!is.na(or_fmt), or_fmt, hr_fmt)
  )

mod_cols <- carto_pal(9, "Safe")[c(5,2,3,4,6:9)]

stemi_forest <-
  ggplot(
    stemi_long,
    aes(or, pos, col = model, label = or_fmt)
  ) +
  facet_grid(
    hr_or ~ smi,
    scales = "free_y",
    space = "free_y",
    axes = "all",
    axis.labels = "all_x"
  ) +
  geom_vline(xintercept = 1, col="lightgrey") +
  geom_point() +
  geom_text(vjust=-0.5, show.legend = FALSE, size = 3) +
  geom_errorbarh(aes(xmin = or_low, xmax = or_upp, height = 0)) +
  scale_x_log10(
    name = "Hazard ratio (95% CI)",
    limits = c(NA, 4.25)
  ) +
  scale_color_manual(values = mod_cols) + 
  theme_bw() +
  theme(
    panel.grid.major.x = element_blank(),
    panel.grid.minor.x = element_blank(),
    panel.grid.major.y = element_blank(),
    axis.title.y = element_blank(),
    plot.background = element_rect(fill = "white"),
    strip.background =element_rect(fill="white"),
    strip.text.y = element_blank(),
    panel.spacing.y = unit(2.5, 'lines')
  ) + 
  facetted_pos_scales(
    y = list(
      scale_y_continuous(
        minor_breaks = c(22.5, 29.5, 36.5),
        breaks = c(40, 33, 26, 19),
        labels = c("30-day mortality", "1-year mortality", "1-year mortality\n(patients\nsurviving\n30 days)", 
                   "1-year mortality\n(patients\ndischarged\nhome)")
      ),
      scale_y_continuous(
        limits = c(-6, 6),
        minor_breaks = c(-0.5),
        breaks = c(3, -4),
        labels = c( "Time to\nmortality\nwithin the\nfirst year", "Time to\nCVD mortality\nwithin the\nfirst year")
      )
    )
  )

stemi_forest

ggsave(
  file.path(pub_folder, "stemi_forest_sa.pdf"),
  width = 21, height = 29.7, units = "cm"
)
