# Thailand Dengue Surveillance Data Imputation

This repository contains the data and R scripts used to evaluate temporal and
spatio-temporal methods for imputing missing monthly dengue surveillance
records across districts in Thailand from January 2010 to September 2023.

The evaluated pipelines combine Prophet or ARIMA temporal models with random-forest spatial residual correction.

## Repository contents

| File | Description |
|---|---|
| `1.missingness_simulation.R` | Generates year-block, spatially clustered year-block, and random-month missingness at 5%, 10%, and 15%. |
| `2.prophet_with_SC.R` | Performs Prophet imputation with and without random-forest spatial residual correction. |
| `3.arima_with_SC.R` | Performs ARIMA imputation with and without random-forest spatial residual correction. |
| `4.evaluation.R` | Calculates MAE and RMSE on artificially masked observations whose true values are known. |
| `5.application.R` | Real-data Prophet-SC application, eligibility filtering, incidence and outbreak classifications underlying Figs 5-6. |
| `6.residual_spatial_analysis.R` | Prophet and ARIMA residuals, monthly Moran's I, distance-correlation bootstrap and RF importance underlying Fig. 4 and Fig. S3. |
| `analysis_data/` | Saved temporal-model residuals and the exact district-boundary files. |
| `df_district.csv` | Original district-level monthly dengue surveillance records, including province names, district names, and unique district codes. |
| `df_district_imputed.csv` | Completed district-level series containing original observations and model estimates for missing records. |
| `data_dictionary.md` | Definitions, units, permitted values, and missing-value conventions for all released data columns. |


## Data structure

Each row represents one district-month. The combination of `Code` and `date`
uniquely identifies a record.

See [`data_dictionary.md`](data_dictionary.md) for variable definitions and
observed/imputed record conventions.

## Software requirements

The analysis was conducted in R. Required packages include:

```text
dplyr tidyr lubridate sf prophet forecast imputeTS randomForest doParallel foreach scoringRules purrr
```

## Intended use

Use `df_district.csv` for analyses that must retain the original surveillance
gaps. Use `df_district_imputed.csv` for analyses requiring a completed monthly
series, while retaining `record_status` and considering sensitivity to imputed
values.

The imputed case counts and potential outbreak classifications are model-based
estimates rather than externally confirmed surveillance events.


