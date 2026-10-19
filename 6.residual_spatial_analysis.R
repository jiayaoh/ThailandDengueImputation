library(dplyr)
library(tidyr)
library(lubridate)
library(sf)
library(randomForest)

############################################################
# Folder setup and settings
############################################################

# Set the working directory to this repository before running.
district_file <- "df_district.csv"
data_dir <- "analysis_data"
out_dir <- "results/residual_spatial_analysis"
models <- c("prophet", "arima")
refit_temporal <- FALSE  # TRUE refits models; FALSE reads the saved real residuals.
n_perm <- 999
n_boot <- 1000
block_months <- 12
bin_km <- 25
max_km <- 900
min_pairs <- 500
min_overlap <- 24
ntree <- 250
max_iter <- 5
tol <- 1e-3
seed <- 20260813
max_targets <- 0  # 0 uses all RF targets; a positive value is for quick tests only.
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

############################################################
# Temporal-model residuals
############################################################

temporal_residuals <- function(df_sub, model, offset = 0.1) {
  df_sub <- arrange(df_sub, date)
  if (model == "prophet") {
    m <- prophet::prophet(
      data.frame(ds = df_sub$date, y = log(df_sub$total_cases + offset)),
      daily.seasonality = FALSE, weekly.seasonality = FALSE,
      yearly.seasonality = TRUE, seasonality.mode = "additive",
      changepoint.prior.scale = 0.05, uncertainty.samples = 0
    )
    fc <- predict(m, data.frame(ds = df_sub$date))
    predicted <- pmax(0, exp(fc$yhat) - offset)
  } else {
    dates <- seq(min(df_sub$date), max(df_sub$date), by = "month")
    values <- df_sub$total_cases[match(dates, df_sub$date)]
    y <- ts(log(values + offset), frequency = 12,
            start = c(year(dates[1]), month(dates[1])))
    m <- forecast::auto.arima(forecast::na.interp(y), seasonal = TRUE)
    predicted <- pmax(0, exp(as.numeric(fitted(m)))[match(df_sub$date, dates)] - offset)
  }
  df_sub %>% mutate(temporal_prediction = predicted, residual = total_cases - predicted)
}

############################################################
# Residual correction and RF importance
############################################################

iterative_rf_importance <- function(E, ntree, max_iter, tol, seed) {
  missing <- is.na(E)
  column_means <- colMeans(E, na.rm = TRUE)
  column_means[!is.finite(column_means)] <- 0
  E_imp <- E
  for (j in seq_len(ncol(E))) E_imp[missing[, j], j] <- column_means[j]
  targets <- which(colSums(missing) > 0 & colSums(!missing) >= 2)
  if (max_targets > 0) {
    set.seed(seed)
    targets <- sort(sample(targets, min(max_targets, length(targets))))
  }
  stopifnot(ncol(E) >= 2, length(targets) > 0)

  # Importance is retained from the last pass; all targets read E_old.
  for (it in seq_len(max_iter)) {
    E_old <- E_imp
    importance <- vector("list", length(targets))
    for (k in seq_along(targets)) {
      j <- targets[k]
      miss <- which(missing[, j])
      obs <- which(!missing[, j])
      X <- as.data.frame(E_old[, -j, drop = FALSE])
      set.seed(seed + 10000 * it + j)
      rf <- randomForest(X[obs, , drop = FALSE], E_old[obs, j], ntree = ntree)
      E_imp[miss, j] <- predict(rf, X[miss, , drop = FALSE])
      v <- as.numeric(randomForest::importance(rf)[, "IncNodePurity"])
      importance[[k]] <- data.frame(
        target = colnames(E)[j], predictor = colnames(E)[-j], importance = v,
        share = if (sum(v) > 0) v / sum(v) else NA_real_
      )
    }
    delta <- sum((E_imp[missing] - E_old[missing])^2)
    message("RF iteration ", it, ": squared change = ", signif(delta, 5))
    if (delta < tol) break
  }
  list(residuals = E_imp, importance = bind_rows(importance))
}

############################################################
# District boundaries
############################################################

boundary_dir <- file.path(out_dir, "boundaries")
shapefile <- file.path(boundary_dir, "tha_admbnda_adm2_rtsd_20220121.shp")
if (!file.exists(shapefile)) {
  dir.create(boundary_dir, recursive = TRUE, showWarnings = FALSE)
  unzip(file.path(data_dir, "thailand_adm2_20220121.zip"), exdir = boundary_dir)
}
district_shape <- st_read(shapefile, quiet = TRUE)
stopifnot(!anyDuplicated(district_shape$ADM2_PCODE))

############################################################
# Prophet and ARIMA analyses
############################################################

for (model in models) {
  model_out <- file.path(out_dir, model)
  dir.create(model_out, recursive = TRUE, showWarnings = FALSE)
  set.seed(seed)

  if (refit_temporal) {
    if (model == "prophet") library(prophet) else library(forecast)
    df <- read.csv(district_file, stringsAsFactors = FALSE) %>%
      mutate(date = ymd(date)) %>% group_by(Code) %>%
      filter(sum(!is.na(total_cases)) >= 24) %>% ungroup()
    df_residuals <- df %>% group_by(Code) %>%
      group_modify(~ temporal_residuals(.x, model)) %>% ungroup()
    saveRDS(df_residuals, file.path(model_out, paste0(model, "_real.rds")))
  } else {
    df_residuals <- readRDS(file.path(data_dir, "residuals", paste0(model, "_real.rds")))
  }
  wide <- df_residuals %>% select(date, Code, residual) %>%
    pivot_wider(names_from = Code, values_from = residual) %>% arrange(date)
  dates <- wide$date
  E <- as.matrix(select(wide, -date))
  codes <- colnames(E)

  # Centroids in the original CRS, then UTM 47N; distances are in km.
  shape <- district_shape[match(codes, district_shape$ADM2_PCODE), ]
  stopifnot(identical(as.character(shape$ADM2_PCODE), codes))
  centroids <- suppressWarnings(st_centroid(st_geometry(shape)))
  xy <- st_coordinates(st_transform(centroids, 32647))
  D <- as.matrix(dist(xy)) / 1000

  # Binary symmetric queen weights; three-nearest fallback for isolated polygons.
  touches <- st_touches(st_geometry(shape))
  W <- matrix(0, length(codes), length(codes))
  for (j in seq_along(codes)) {
    neighbours <- touches[[j]]
    if (!length(neighbours)) {
      distances <- D[j, ]
      distances[j] <- Inf
      neighbours <- head(order(distances), 3)
    }
    W[j, neighbours] <- 1
  }
  W <- 1 * ((W + t(W)) > 0)
  diag(W) <- 0
  edges <- which(W > 0, arr.ind = TRUE)
  write.csv(data.frame(from = codes[edges[, 1]], to = codes[edges[, 2]], weight = 1),
            file.path(model_out, "spatial_weights_edges.csv"), row.names = FALSE)

  ##########################################################
  # Pairwise correlation by distance
  ##########################################################

  C <- suppressWarnings(cor(E, use = "pairwise.complete.obs"))
  C[crossprod(!is.na(E)) < min_overlap] <- NA_real_
  diag(C) <- NA_real_
  valid <- upper.tri(C) & is.finite(C)
  regional_mean <- mean(C[valid])
  distance_bin <- floor(D / bin_km) * bin_km + bin_km / 2
  correlation_by_distance <- data.frame(
    bin = distance_bin[valid], r = C[valid], distance = D[valid]
  ) %>% filter(distance <= max_km) %>% group_by(bin) %>%
    summarise(n_pairs = n(), mean_r = mean(r), .groups = "drop") %>%
    filter(n_pairs >= min_pairs)
  stopifnot(nrow(correlation_by_distance) > 0)

  # Reassign locations while keeping the correlation matrix fixed.
  contrast <- function(distance) {
    mean(C[valid & distance <= 50]) - mean(C[valid & distance >= 400])
  }
  set.seed(seed)
  observed_contrast <- contrast(D)
  geography_null <- vapply(seq_len(n_perm), function(i) {
    order <- sample.int(ncol(D))
    contrast(D[order, order])
  }, numeric(1))
  regional_summary <- data.frame(
    temporal_model = model, n_pairs = sum(valid),
    overall_mean_pairwise_correlation = regional_mean,
    near_threshold_km = 50, far_threshold_km = 400,
    observed_near_minus_far = observed_contrast, n_geography_permutations = n_perm,
    permutation_p_one_sided = (1 + sum(geography_null >= observed_contrast)) / (n_perm + 1)
  )
  write.csv(regional_summary, file.path(model_out, "regional_synchrony_summary_real.csv"),
            row.names = FALSE)
  write.csv(data.frame(permutation = seq_len(n_perm), near_minus_far_correlation = geography_null),
            file.path(model_out, "geography_permutation_real.csv"), row.names = FALSE)

  # Resample consecutive 12-month blocks, with circular wrapping.
  # The same sampled months are used for every district.
  pairs_by_bin <- lapply(correlation_by_distance$bin, function(b) {
    which(valid & distance_bin == b & D <= max_km)
  })
  bootstrap_means <- matrix(NA_real_, n_boot, nrow(correlation_by_distance))
  for (b in seq_len(n_boot)) {
    starts <- sample.int(nrow(E), ceiling(nrow(E) / block_months), replace = TRUE)
    idx <- unlist(lapply(starts, function(s) {
      ((s - 1 + seq_len(block_months) - 1) %% nrow(E)) + 1
    }))[seq_len(nrow(E))]
    C_boot <- suppressWarnings(cor(E[idx, , drop = FALSE], use = "pairwise.complete.obs"))
    bootstrap_means[b, ] <- vapply(pairs_by_bin, function(i) mean(C_boot[i], na.rm = TRUE),
                                    numeric(1))
    if (b %% 100 == 0) message(model, ": bootstrap ", b, "/", n_boot)
  }
  correlation_by_distance <- correlation_by_distance %>% mutate(
    field = "raw residuals", temporal_model = model,
    ci_low = apply(bootstrap_means, 2, quantile, probs = 0.025, na.rm = TRUE),
    ci_high = apply(bootstrap_means, 2, quantile, probs = 0.975, na.rm = TRUE),
    n_boot = n_boot, block_months = block_months
  )
  write.csv(correlation_by_distance,
            file.path(model_out, "correlogram_bootstrap_ci_real.csv"), row.names = FALSE)

  ##########################################################
  # Monthly Moran's I
  ##########################################################

  residual_sd <- apply(E, 2, sd, na.rm = TRUE)
  keep <- which(is.finite(residual_sd) & residual_sd > 0)
  E_standardized <- sweep(E[, keep, drop = FALSE], 2, residual_sd[keep], "/")
  W_standardized <- W[keep, keep, drop = FALSE]
  morans_i <- lapply(seq_len(nrow(E)), function(t) {
    ok <- which(is.finite(E_standardized[t, ]))
    if (length(ok) < 30) return(NULL)
    repeat {
      has_neighbour <- rowSums(W_standardized[ok, ok, drop = FALSE]) > 0
      if (all(has_neighbour)) break
      ok <- ok[has_neighbour]
      if (length(ok) < 30) return(NULL)
    }
    edges <- which(W_standardized[ok, ok, drop = FALSE] > 0, arr.ind = TRUE)
    x <- E_standardized[t, ok]
    moran_I <- function(values) {
      z <- values - mean(values)
      if (sum(z^2) == 0) return(NA_real_)
      length(z) / nrow(edges) * sum(z[edges[, 1]] * z[edges[, 2]]) / sum(z^2)
    }
    value <- moran_I(x)
    if (!is.finite(value)) return(NULL)
    null <- vapply(seq_len(n_perm), function(i) moran_I(sample(x)), numeric(1))
    data.frame(field = "raw residuals", date = dates[t], n_obs = length(ok),
                I = value, I_null = mean(null),
                p_perm = (1 + sum(null >= value)) / (n_perm + 1))
  }) %>% bind_rows()
  morans_summary <- morans_i %>% group_by(field) %>% summarise(
    n_months = n(), median_I = median(I), IQR_low = quantile(I, 0.25),
    IQR_high = quantile(I, 0.75), pct_sig_pos = 100 * mean(p_perm < 0.05 & I > 0),
    .groups = "drop"
  )
  write.csv(morans_i, file.path(model_out, "morans_i_real.csv"), row.names = FALSE)
  write.csv(morans_summary, file.path(model_out, "summary_morans_real.csv"), row.names = FALSE)

  ##########################################################
  # RF importance by distance
  ##########################################################

  rf_result <- iterative_rf_importance(E, ntree, max_iter, tol, seed)
  importance <- rf_result$importance
  importance$dist_km <- D[cbind(match(importance$target, codes), match(importance$predictor, codes))]
  # Match the archived terminal group: all distances >=900 km are pooled.
  importance_by_distance <- importance %>%
    mutate(bin = pmin(floor(dist_km / bin_km) * bin_km + bin_km / 2, max_km + bin_km / 2)) %>%
    group_by(bin) %>% summarise(n = n(), mean_share = mean(share),
                                se = sd(share) / sqrt(n()), .groups = "drop") %>%
    filter(n >= min_pairs)
  write.csv(importance, file.path(model_out, "importance_full.csv"), row.names = FALSE)
  write.csv(importance_by_distance, file.path(model_out, "importance_by_distance.csv"),
            row.names = FALSE)
  saveRDS(rf_result$residuals, file.path(model_out, "residual_matrix_imputed.rds"))
  message(model, " analysis completed: ", model_out)
}
