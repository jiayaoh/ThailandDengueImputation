library(dplyr)
library(tidyr)
library(lubridate)
library(forecast)
library(imputeTS)
library(randomForest)
library(doParallel)
library(foreach)


# Imputation Functions
arima_impute <- function(df_sub, offset = 0.1) {
  df_sub <- df_sub %>% arrange(date)
  all_dates <- seq.Date(min(df_sub$date, na.rm=TRUE),
                        max(df_sub$date, na.rm=TRUE),
                        by = "month")
  df_sub <- df_sub %>% complete(date = all_dates)
  
  df_sub <- df_sub %>%
    mutate(y_log = ifelse(is.na(total_cases_missing),
                          NA,
                          log(total_cases_missing + offset)))
  
  start_yr  <- year(min(df_sub$date, na.rm=TRUE))
  start_mon <- month(min(df_sub$date, na.rm=TRUE))
  ts_obj    <- ts(df_sub$y_log, start = c(start_yr, start_mon), frequency = 12)
  ts_full   <- na.interp(ts_obj)
  fit       <- auto.arima(ts_full, seasonal = TRUE)
  fitted_l  <- fitted(fit)
  if (length(fitted_l) < length(ts_full)) {
    fitted_l <- c(rep(NA, length(ts_full) - length(fitted_l)), fitted_l)
  }
  
  pred_orig <- pmax(0, exp(fitted_l) - offset)
  df_sub$arima_impute <- pred_orig
  df_sub$residual      <- ifelse(is.na(df_sub$total_cases_missing),
                                 NA,
                                 df_sub$total_cases_missing - df_sub$arima_impute)
  df_sub
}

# residual imputation
iterative_rf_impute <- function(E, max_iter = 10, tol = 1e-3) {
  E_imp <- E
  for (it in seq_len(max_iter)) {
    E_old <- E_imp
    for (j in seq_len(ncol(E_imp))) {
      miss <- which(is.na(E[, j]))
      if (length(miss)==0) next
      obs  <- setdiff(seq_len(nrow(E_imp)), miss)
      if (length(obs) < 2) next
      
      X_obs <- as.data.frame(E_imp[obs, -j, drop=FALSE])
      X_obs[] <- lapply(X_obs, function(col) {
        if (all(is.na(col))) rep(0, length(col)) else { col[is.na(col)] <- mean(col, na.rm=TRUE); col }
      })
      y_obs <- E_imp[obs, j]
      rf    <- randomForest(X_obs, y_obs)
      
      X_mis <- as.data.frame(E_imp[miss, -j, drop=FALSE])
      X_mis[] <- lapply(names(X_mis), function(col) {
        v <- X_mis[[col]]
        if (all(is.na(v))) rep(0, length(v)) else { v[is.na(v)] <- mean(X_obs[[col]], na.rm=TRUE); v }
      }) %>% setNames(names(X_mis))
      
      E_imp[miss, j] <- predict(rf, X_mis)
    }
    if (sum((E_imp - E_old)^2, na.rm=TRUE) < tol) break
  }
  E_imp
}

process_residual_imputation <- function(df_arima) {
  wide <- df_arima %>%
    select(date, Code, residual) %>%
    pivot_wider(names_from = Code, values_from = residual)
  dates <- wide$date
  E     <- as.matrix(wide %>% select(-date))
  E_imp <- iterative_rf_impute(E)
  
  long_imp <- as.data.frame(E_imp) %>%
    mutate(date = dates) %>%
    pivot_longer(-date, names_to="Code", values_to="residual_imputed")
  
  df_arima %>%
    left_join(long_imp, by = c("date","Code")) %>%
    mutate(final_imputed = arima_impute + residual_imputed)
}

# folder setup
arima_in  <- c(
  ".../missing_5",
  ".../missing_10",
  ".../missing_15"
)
arima_out <- c(
  ".../arima_imputation/missing_5",
  ".../arima_imputation/missing_10",
  ".../arima_imputation/missing_15"
)

# SC outputs
resid_out <- c(
  ".../arima_residual_impute/missing_5",
  ".../arima_residual_impute/missing_10",
  ".../arima_residual_impute/missing_15"
)

for(d in c(arima_out, resid_out)) if(!dir.exists(d)) dir.create(d, recursive=TRUE)

# Parallel Cluster Setup
numCores <- 40
cl       <- makeCluster(numCores)
registerDoParallel(cl)


# ARIMA Imputation
for(i in seq_along(arima_in)) {
  files <- list.files(arima_in[i], pattern="\\.csv$", full.names=TRUE)
  foreach(f = files, .packages=c("dplyr","tidyr","lubridate","forecast","imputeTS")) %dopar% {
    df <- read.csv(f, stringsAsFactors=FALSE) %>%
      mutate(date = ymd(date))
    imp <- df %>%
      group_by(Code) %>%
      group_modify(~ arima_impute_log(.x)) %>%
      ungroup()
    write.csv(imp, file.path(arima_out[i], basename(f)), row.names=FALSE)
  }
  message("ARIMA impute done for: ", arima_in[i])
}

# Spatial Residual Imputation
for(i in seq_along(arima_out)) {
  files <- list.files(arima_out[i], pattern="\\.csv$", full.names=TRUE)
  foreach(f = files, .packages=c("dplyr","tidyr","lubridate","randomForest","forecast","imputeTS")) %dopar% {
    df_a <- read.csv(f, stringsAsFactors=FALSE) %>%
      mutate(date = ymd(date))
    final <- process_residual_imputation(df_a)
    write.csv(final, file.path(resid_out[i], basename(f)), row.names=FALSE)
  }
  message("Residual RF done for: ", arima_out[i])
}
stopCluster(cl)