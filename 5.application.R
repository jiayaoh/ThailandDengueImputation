library(dplyr)
library(tidyr)
library(lubridate)

############################################################
# Folder setup and settings
############################################################

# Set the working directory to this repository before running.
district_file <- "df_district.csv"
completed_file <- "df_district_imputed.csv"
cutoff_percent <- 15
refit <- FALSE  # TRUE runs a new Prophet-SC fit; FALSE uses the released estimates.
ntree <- 500
max_iter <- 10
tol <- 1e-3
seed <- 20260813
out_dir <- file.path("results", "application", if (refit) "refit" else "archived")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

############################################################
# Imputation functions
############################################################

prophet_impute <- function(df_sub, offset = 0.1) {
  df_sub <- arrange(df_sub, date)
  m <- prophet::prophet(
    data.frame(ds = df_sub$date, y = log(df_sub$total_cases + offset)),
    daily.seasonality = FALSE, weekly.seasonality = FALSE,
    yearly.seasonality = TRUE, seasonality.mode = "additive",
    changepoint.prior.scale = 0.05, uncertainty.samples = 0
  )
  fc <- predict(m, data.frame(ds = df_sub$date))
  df_sub %>% mutate(prophet_pred = pmax(0, exp(fc$yhat) - offset),
                    residual = total_cases - prophet_pred)
}

iterative_rf_impute <- function(E, ntree, max_iter, tol, seed) {
  missing <- is.na(E)
  column_means <- colMeans(E, na.rm = TRUE)
  column_means[!is.finite(column_means)] <- 0
  E_imp <- E
  for (j in seq_len(ncol(E))) E_imp[missing[, j], j] <- column_means[j]
  targets <- which(colSums(missing) > 0 & colSums(!missing) >= 2)

  # All forests in a pass use the previous residual matrix.
  for (it in seq_len(max_iter)) {
    E_old <- E_imp
    for (j in targets) {
      miss <- which(missing[, j])
      obs <- which(!missing[, j])
      X <- as.data.frame(E_old[, -j, drop = FALSE])
      set.seed(seed + 10000 * it + j)
      rf <- randomForest::randomForest(X[obs, , drop = FALSE], E_old[obs, j],
                                        ntree = ntree)
      E_imp[miss, j] <- predict(rf, X[miss, , drop = FALSE])
    }
    delta <- sum((E_imp[missing] - E_old[missing])^2)
    message("RF iteration ", it, ": squared change = ", signif(delta, 5))
    if (delta < tol) break
  }
  E_imp
}

############################################################
# Original and completed data
############################################################

df_district <- read.csv(district_file, stringsAsFactors = FALSE) %>%
  mutate(date = ymd(date)) %>% arrange(Code, date)
df_joined <- read.csv(completed_file, stringsAsFactors = FALSE) %>%
  mutate(date = ymd(date), year = year(date), month = month(date)) %>%
  arrange(Code, date)

stopifnot(!anyNA(df_district$Code), !anyNA(df_district$date),
          !anyDuplicated(df_district[c("Code", "date")]),
          !anyDuplicated(df_joined[c("Code", "date")]),
          identical(df_district[c("Code", "date", "total_cases")],
                    df_joined[c("Code", "date", "total_cases")]),
          all(df_joined$completed_cases[!is.na(df_joined$total_cases)] ==
                na.omit(df_joined$total_cases)))

############################################################
# Optional new Prophet-SC fit
############################################################

if (refit) {
  library(prophet)
  library(randomForest)

  fit_data <- df_district %>% group_by(Code) %>%
    filter(sum(!is.na(total_cases)) >= 24) %>% ungroup()
  set.seed(seed)
  df_prophet <- fit_data %>% group_by(Code) %>%
    group_modify(~ prophet_impute(.x)) %>% ungroup()
  saveRDS(df_prophet, file.path(out_dir, "prophet_real.rds"))

  wide <- df_prophet %>% select(date, Code, residual) %>%
    pivot_wider(names_from = Code, values_from = residual) %>% arrange(date)
  E <- as.matrix(select(wide, -date))
  E_imp <- iterative_rf_impute(E, ntree, max_iter, tol, seed)
  idx <- cbind(match(df_prophet$date, wide$date), match(df_prophet$Code, colnames(E)))
  df_prophet <- df_prophet %>% mutate(
    final_imputed = if_else(is.na(total_cases),
                            pmax(0, round(prophet_pred + E_imp[idx])), total_cases)
  )
  df_joined <- df_joined %>% select(-completed_cases) %>%
    left_join(select(df_prophet, Code, date, final_imputed), by = c("Code", "date")) %>%
    mutate(completed_cases = if_else(!is.na(total_cases), total_cases, final_imputed)) %>%
    select(-final_imputed)
}

############################################################
# Eligibility and outbreak classification
############################################################

df_joined <- df_joined %>% mutate(
  is_observed = !is.na(total_cases), is_imputed = is.na(total_cases),
  imputed_cases = if_else(is_imputed, completed_cases, NA_real_),
  record_status = case_when(is_observed ~ "observed",
                            is.finite(completed_cases) ~ "imputed", TRUE ~ "unresolved")
)
district_missingness <- df_joined %>% group_by(Code) %>% summarise(
  n_months = n(), n_observed_months = sum(is_observed),
  n_imputed_months = sum(is_imputed), missing_fraction = mean(is_imputed),
  .groups = "drop"
) %>% mutate(analysis_eligible = missing_fraction <= cutoff_percent / 100)

# Same calendar month in the preceding five years; at least two values.
# The baseline uses completed CASE COUNTS, including historical imputed values.
df_joined <- df_joined %>% group_by(Code, month) %>%
  arrange(year, .by_group = TRUE) %>% group_modify(function(.x, .y) {
    .x$completed_history_n <- integer(nrow(.x))
    .x$outbreak_threshold <- NA_real_
    for (i in seq_len(nrow(.x))) {
      history <- .x$completed_cases[.x$year >= .x$year[i] - 5 & .x$year < .x$year[i]]
      history <- history[is.finite(history)]
      .x$completed_history_n[i] <- length(history)
      if (length(history) >= 2) {
        .x$outbreak_threshold[i] <- median(history) + 2 * sd(history)
      }
    }
    .x
  }) %>% ungroup() %>%
  left_join(select(district_missingness, Code, missing_fraction, analysis_eligible), by = "Code") %>%
  mutate(
    observed_incidence = if_else(is.finite(Population) & Population > 0,
                                 1e5 * total_cases / Population, NA_real_),
    completed_incidence = if_else(is.finite(Population) & Population > 0,
                                  1e5 * completed_cases / Population, NA_real_),
    analysis_completed_cases = if_else(is_observed | analysis_eligible,
                                       completed_cases, NA_real_),
    potential_imputed_outbreak = if_else(
      is_imputed & analysis_eligible & !is.na(outbreak_threshold),
      completed_cases > outbreak_threshold, NA
    ),
    observed_outbreak_completed_history = if_else(
      is_observed & analysis_eligible & !is.na(outbreak_threshold),
      total_cases > outbreak_threshold, NA
    )
  ) %>% arrange(Code, date)

############################################################
# District summaries and outputs
############################################################

district_summary <- df_joined %>% group_by(Code) %>% summarise(
  n_months = n(), n_observed_months = sum(is_observed), n_imputed_months = sum(is_imputed),
  missing_fraction = first(missing_fraction), analysis_eligible = first(analysis_eligible),
  observed_cases = sum(total_cases, na.rm = TRUE),
  model_estimated_cases = sum(if_else(is_imputed & analysis_eligible, completed_cases, 0),
                              na.rm = TRUE),
  potential_outbreak_months = sum(potential_imputed_outbreak %in% TRUE),
  evaluable_imputed_months = sum(is_imputed & analysis_eligible & !is.na(outbreak_threshold)),
  unresolved_imputed_months = sum(is_imputed & !is.finite(completed_cases)),
  .groups = "drop"
) %>% mutate(
  completed_cases = observed_cases + model_estimated_cases,
  model_estimated_share_pct = if_else(analysis_eligible & completed_cases > 0,
                                      100 * model_estimated_cases / completed_cases, NA_real_),
  district_record_status = case_when(
    !analysis_eligible ~ paste0("excluded_missingness_gt", cutoff_percent, "pct"),
    n_imputed_months > 0 ~ "contains_imputed_values", TRUE ~ "observed_only"
  )
)
gap_districts <- filter(district_summary, analysis_eligible, n_imputed_months > 0)
observed <- sum(gap_districts$observed_cases)
estimated <- sum(gap_districts$model_estimated_cases)
application_summary <- data.frame(
  cutoff_percent = cutoff_percent, eligible_gap_districts = nrow(gap_districts),
  eligible_gap_months = sum(gap_districts$n_imputed_months),
  observed_cases = observed, model_estimated_cases = estimated,
  estimated_share_of_completed_pct = 100 * estimated / (observed + estimated),
  increase_relative_to_observed_pct = 100 * estimated / observed,
  potential_imputed_outbreak_months = sum(gap_districts$potential_outbreak_months),
  august_2021_potential_imputed_outbreaks = sum(
    df_joined$date == ymd("2021-08-01") & df_joined$potential_imputed_outbreak %in% TRUE
  )
)
df_district_imputed <- df_joined %>% select(
  Province, District, Code, date, year, month, Population,
  total_cases, imputed_cases, completed_cases, record_status
)
write.csv(df_district_imputed, file.path(out_dir, "df_district_imputed.csv"), row.names = FALSE)
write.csv(df_joined, file.path(out_dir, "application_records.csv"), row.names = FALSE)
write.csv(district_missingness, file.path(out_dir, "district_missingness.csv"), row.names = FALSE)
write.csv(district_summary, file.path(out_dir, "district_summary.csv"), row.names = FALSE)
write.csv(filter(df_joined, date == ymd("2021-08-01")),
          file.path(out_dir, "august_2021.csv"), row.names = FALSE)
write.csv(application_summary, file.path(out_dir, "application_summary.csv"), row.names = FALSE)
print(application_summary)
