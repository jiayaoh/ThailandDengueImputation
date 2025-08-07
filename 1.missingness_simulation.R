library(dplyr)
library(tidyr)
library(lubridate)
library(sf)

set.seed(123)

# Read district time-series
ts_district <- read.csv("df_district.csv", stringsAsFactors = FALSE) %>%
  mutate(
    date     = ymd(date),
    year     = year(date),
    Province = substr(Code, 3, 4)  #extract province code
  )

############################################################
# 1.  Missingness Simulation Functions
############################################################

# Year-block missingness
simulate_year_block <- function(df, missing_frac, seed = NULL) {
  if (!is.null(seed)) set.seed(seed)
  combos <- df %>%
    select(Code, year) %>%
    distinct()
  n_drop <- ceiling(missing_frac * nrow(combos))
  to_drop <- combos %>%
    sample_n(n_drop) %>%
    mutate(id = paste(Code, year, sep = "_")) %>%
    pull(id)
  
  df %>%
    mutate(
      id = paste(Code, year, sep = "_"),
      total_cases_missing = if_else(id %in% to_drop, NA_real_, total_cases)
    ) %>%
    select(-id) %>%
    group_by(Code) %>%
    filter(sum(!is.na(total_cases_missing)) >= 3) %>%
    ungroup()
}

# Random-month missingness
simulate_random_month <- function(df, missing_frac, seed = NULL) {
  if (!is.null(seed)) set.seed(seed)
  n_total <- nrow(df)
  n_drop  <- ceiling(missing_frac * n_total)
  drop_i  <- sample(n_total, n_drop)
  
  df2 <- df %>%
    mutate(total_cases_missing = total_cases)
  df2$total_cases_missing[drop_i] <- NA_real_
  
  df2 %>%
    group_by(Code) %>%
    filter(sum(!is.na(total_cases_missing)) >= 3) %>%
    ungroup()
}

# Spatially-clustered year-block missingness
simulate_spatial_cluster <- function(df,
                                     missing_frac,
                                     block_frac = 0.125,
                                     seed       = NULL) {
  if (!is.null(seed)) set.seed(seed)
  target_n <- ceiling(missing_frac * nrow(df))
  
  df2 <- df %>% mutate(total_cases_missing = total_cases)
  lookup <- df2 %>%
    distinct(Province, year, Code) %>%
    arrange(Province, year, Code) %>%
    group_by(Province, year) %>%
    summarise(codes = list(Code), .groups = "drop")
  
  blanked <- sum(is.na(df2$total_cases_missing))
  while (blanked < target_n) {
    row      <- lookup %>% slice_sample(n = 1)
    codes    <- row$codes[[1]]
    k        <- max(1, floor(block_frac * length(codes)))
    start    <- sample(seq_len(length(codes) - k + 1), 1)
    drop_set <- codes[start:(start + k - 1)]
    
    mask <- with(df2,
                 Province == row$Province &
                   year     == row$year     &
                   Code     %in% drop_set)
    
    df2$total_cases_missing[mask] <- NA_real_
    blanked <- sum(is.na(df2$total_cases_missing))
  }
  df2 %>%
    group_by(Code) %>%
    filter(sum(!is.na(total_cases_missing)) >= 3) %>%
    ungroup()
}

############################################################
# 2.  Generate and Save Simulated Datasets
############################################################
missing_fractions <- c(0.05, 0.10, 0.15)

# Year-block missingness
folder_names <- c(".../year_block_missing/missing_5",
                  ".../year_block_missing/missing_10",
                  ".../year_block_missing/missing_15")

for(i in seq_along(missing_fractions)) {
  missing_frac <- missing_fractions[i]
  folder_name <- folder_names[i]

  if(!dir.exists(folder_name)) {
    dir.create(folder_name)
  }
  # Run 100 simulations
  for(sim in 1:100) {
    sim_data <- simulate_year_block(ts_district, missing_frac)
    filename <- file.path(folder_name, sprintf("simulated_%d.csv", sim))
    write.csv(sim_data, filename, row.names = FALSE)
  }
  
  cat(sprintf("Completed saving 100 simulated datasets for %.0f%% missingness in folder '%s'.\n",
              missing_frac * 100, folder_name))
}


# Random-month missingness
folder_names <- c(
  ".../random_month_missing/missing_5",
  ".../random_month_missing/missing_10",
  ".../random_month_missing/missing_15"
)

for (i in seq_along(missing_fractions)) {
  missing_frac <- missing_fractions[i]
  folder_name  <- folder_names[i]
  if (!dir.exists(folder_name)) dir.create(folder_name, recursive = TRUE)
  for (sim in 1:100) {
    sim_data <- simulate_random_month(ts_district, missing_frac)
    write.csv(
      sim_data,
      file.path(folder_name, sprintf("simulated_%d.csv", sim)),
      row.names = FALSE
    )
  }
  cat(sprintf(
    "Saved 100 random‑month datasets for %.0f%% missingness in '%s'.\n",
    missing_frac * 100, folder_name
  ))
}

# Spatially-clustered year-block missingness
block_frac        <- 0.125
folder_names <- c(
  ".../spatial_cluster_missing2/missing_5",
  ".../spatial_cluster_missing2/missing_10",
  ".../spatial_cluster_missing2/missing_15"
)

for (i in seq_along(missing_fractions)) {
  mf   <- missing_fractions[i]
  fdir <- folder_names[i]
  if (!dir.exists(fdir)) dir.create(fdir)
  for (sim in 1:100) {
    seed   <- 1000 * i + sim
    sim_df <- simulate_block_missing(
      ts_district,
      missing_frac = mf,
      block_frac   = block_frac,
      seed         = seed
    )
    fn <- file.path(fdir, sprintf("sim_%03d.csv", sim))
    write.csv(sim_df, fn, row.names = FALSE)
  }
  message(sprintf(
    "Completed 100 sims @ ~%2.0f%% overall missing → folder “%s”",
    mf * 100, fdir
  ))
}


