library(dplyr)
library(tidyr)
library(lubridate)
library(prophet)
library(randomForest)
library(doParallel)
library(foreach)

############################################################
# Imputation Functions
############################################################

# Prophet
prophet_impute <- function(df_sub, offset = 0.1) {
  df_sub <- df_sub %>%
    mutate(date = ymd(trimws(as.character(date)))) %>%
    complete(date = seq.Date(min(date, na.rm=TRUE),
                             max(date, na.rm=TRUE),
                             by = "month"))
  
  df_prop <- df_sub %>%
    select(date, total_cases_missing) %>%
    rename(ds = date, y = total_cases_missing) %>%
    mutate(y = log(y + offset))
  
  m <- prophet(df_prop,
               daily.seasonality   = FALSE,
               weekly.seasonality  = FALSE,
               yearly.seasonality  = TRUE,
               seasonality.mode    = "additive",
               changepoint.prior.scale = 0.05)
  
  future <- data.frame(ds = seq.Date(from = as.Date("2010-01-01"),
                                     to   = as.Date("2023-09-01"),
                                     by   = "month"))
  fc <- predict(m, future) %>%
    transmute(ds,
              yhat_log = yhat,
              yhat_org = pmax(0, exp(yhat) - offset))
  
  df_sub %>%
    left_join(fc, by = c("date" = "ds")) %>%
    mutate(prophet_pred = yhat_org,
           residual     = ifelse(is.na(total_cases_missing),
                                 NA,
                                 total_cases_missing - prophet_pred))
}


# residual imputation
iterative_rf_impute <- function(E, max_iter = 10, tol = 1e-3) {
  E_imp <- E
  for (i in seq_len(max_iter)) {
    E_old <- E_imp
    for (j in seq_len(ncol(E_imp))) {
      miss_idx <- which(is.na(E[, j]))
      if (length(miss_idx)==0) next
      obs_idx  <- setdiff(seq_len(nrow(E_imp)), miss_idx)
      if (length(obs_idx)<2) next
      
      X_obs <- as.data.frame(E_imp[obs_idx, -j, drop=FALSE])
      # fill any NA predictors
      X_obs[] <- lapply(X_obs, function(col) {
        if (all(is.na(col))) rep(0, length(col))
        else { col[is.na(col)] <- mean(col, na.rm=TRUE); col }
      })
      y_obs <- E_imp[obs_idx, j]
      rf   <- randomForest(X_obs, y_obs)
      
      X_mis <- as.data.frame(E_imp[miss_idx, -j, drop=FALSE])
      X_mis[] <- lapply(names(X_mis), function(col) {
        v <- X_mis[[col]]
        if (all(is.na(v))) rep(0, length(v))
        else    { v[is.na(v)] <- mean(X_obs[[col]], na.rm=TRUE); v }
      }) %>% setNames(names(X_mis))
      
      E_imp[miss_idx, j] <- predict(rf, X_mis)
    }
    if (sum((E_imp - E_old)^2, na.rm=TRUE) < tol) break
  }
  E_imp
}

process_residual_imputation <- function(df_prophet) {
  wide <- df_prophet %>%
    select(date, Code, residual) %>%
    pivot_wider(names_from = Code, values_from = residual)
  dates <- wide$date
  E     <- as.matrix(wide %>% select(-date))
  E_imp <- iterative_rf_impute(E)
  
  long_imp <- as.data.frame(E_imp) %>%
    mutate(date = dates) %>%
    pivot_longer(-date, names_to="Code", values_to="residual_imputed")
  
  df_prophet %>%
    left_join(long_imp, by = c("date","Code")) %>%
    mutate(final_imputed = prophet_pred + residual_imputed)
}


# folder setup
prophet_in  <- c(
  ".../missing_5",
  ".../missing_10",
  ".../missing_15"
)

prophet_out <- c(
  ".../prophet_imputation/missing_5",
  ".../prophet_imputation/missing_10",
  ".../prophet_imputation/missing_15"
)

resid_out   <- c(
  ".../prophet_residual_impute/missing_5",
  ".../prophet_residual_impute/missing_10",
  ".../prophet_residual_impute/missing_15"
)

for (d in c(prophet_out, resid_out)) {
  if (!dir.exists(d)) dir.create(d, recursive=TRUE)
}


# Parallel Cluster Setup
numCores <- 40
cl       <- makeCluster(numCores)
registerDoParallel(cl)


# Prophet Imputation
for (i in seq_along(prophet_in)) {
  files <- list.files(prophet_in[i], pattern="\\.csv$", full.names=TRUE)
  foreach(f = files, .packages=c("dplyr","tidyr","lubridate","prophet")) %dopar% {
    df <- read.csv(f, stringsAsFactors=FALSE) %>%
      mutate(date = ymd(date))
    imputed <- df %>%
      group_by(Code) %>%
      group_modify(~ prophet_impute(.x)) %>%
      ungroup()
    write.csv(imputed,
              file.path(prophet_out[i], basename(f)),
              row.names=FALSE)
  }
  message("Prophet done for: ", prophet_in[i])
}

# Spatial Residual Imputation
for (i in seq_along(prophet_out)) {
  files <- list.files(prophet_out[i], pattern="\\.csv$", full.names=TRUE)
  foreach(f = files, .packages=c("dplyr","tidyr","lubridate","randomForest")) %dopar% {
    df_prop <- read.csv(f, stringsAsFactors=FALSE) %>%
      mutate(date = ymd(date))
    df_final <- process_residual_imputation(df_prop)
    write.csv(df_final,
              file.path(resid_out[i], basename(f)),
              row.names=FALSE)
  }
  message("Residual‐RF done for: ", prophet_out[i])
}
stopCluster(cl)

