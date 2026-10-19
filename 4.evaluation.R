library(dplyr)
library(lubridate)
library(tidyr)
library(scoringRules)
library(purrr)

evaluate_imputation <- function(df_imputed, impute_col) {
  df_comp <- df_imputed %>%
    filter(is.na(total_cases_missing) & !is.na(total_cases)) %>%
    mutate(
      error = abs(total_cases - !!sym(impute_col)),
      sq_error = (total_cases - !!sym(impute_col))^2
    )
  
  mae_val  <- mean(df_comp$error, na.rm = TRUE)
  rmse_val <- sqrt(mean(df_comp$sq_error, na.rm = TRUE))
  sd_abs   <- sd(df_comp$error, na.rm = TRUE)
  sd_sq    <- sd(df_comp$sq_error, na.rm = TRUE)
  
  list(MAE = mae_val, RMSE = rmse_val, SD_AbsError = sd_abs, SD_SqError = sd_sq)
}

compute_metrics_from_file <- function(file, impute_col = "final_imputed") {
  df <- read.csv(file, stringsAsFactors = FALSE) %>%
    mutate(date = ymd(date))
  
  res <- evaluate_imputation(df, impute_col)
  return(res)
}

summarize_metrics_for_folder <- function(folder_path, impute_col = "final_imputed") {
  files <- list.files(folder_path, pattern = "\\.csv$", full.names = TRUE)
  metrics_list <- lapply(files, compute_metrics_from_file, impute_col = impute_col)
  mae_values <- sapply(metrics_list, function(x) x$MAE)
  rmse_values <- sapply(metrics_list, function(x) x$RMSE)
  
  summary <- list(
    mean_mae = mean(mae_values, na.rm = TRUE),
    sd_mae = sd(mae_values, na.rm = TRUE),
    mean_rmse = mean(rmse_values, na.rm = TRUE),
    sd_rmse = sd(rmse_values, na.rm = TRUE)
  )
  return(summary)
}


folder_5 <- ".../arima_residual_impute/missing_5"
folder_10 <- ".../arima_residual_impute/missing_10"
folder_15 <- ".../arima_residual_impute/missing_15"

summary_5 <- summarize_metrics_for_folder(folder_5, impute_col = "final_imputed")
summary_10 <- summarize_metrics_for_folder(folder_10, impute_col = "final_imputed")
summary_15 <- summarize_metrics_for_folder(folder_15, impute_col = "final_imputed")

cat(sprintf("5%% Missingness: Mean MAE = %.3f, SD MAE = %.3f; Mean RMSE = %.3f, SD RMSE = %.3f\n", 
            summary_5$mean_mae, summary_5$sd_mae, summary_5$mean_rmse, summary_5$sd_rmse))
cat(sprintf("10%% Missingness: Mean MAE = %.3f, SD MAE = %.3f; Mean RMSE = %.3f, SD RMSE = %.3f\n", 
            summary_10$mean_mae, summary_10$sd_mae, summary_10$mean_rmse, summary_10$sd_rmse))
cat(sprintf("15%% Missingness: Mean MAE = %.3f, SD MAE = %.3f; Mean RMSE = %.3f, SD RMSE = %.3f\n", 
            summary_15$mean_mae, summary_15$sd_mae, summary_15$mean_rmse, summary_15$sd_rmse))
