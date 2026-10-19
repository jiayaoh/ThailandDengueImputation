# Data Dictionary

## Dataset overview

The released data contain monthly district-level dengue surveillance records
for Thailand from January 2010 to September 2023. Each row represents one
district-month. The combination of `Code` and `date` uniquely identifies a
record.

Province and district names are provided directly in both released datasets so
that users do not need a separate geographic lookup table.

## Files

### `df_district.csv`

Original district-level dengue surveillance records. Missing surveillance reports are retained as `NA` in `total_cases`.

| Column | Description |
|---|---|
| `Province` | Province name (English). |
| `District` | District name (English). |
| `Code` | Unique district identifier (`TH` + four digits). |
| `date` | First day of the surveillance month (`YYYY-MM-DD`). |
| `year` | Calendar year. |
| `month` | Calendar month (1–12). |
| `total_cases` | Originally reported monthly dengue case count; `NA` indicates a missing surveillance report. |

### `df_district_imputed.csv`

Completed district-level series containing original observations and model estimates.

| Column | Description |
|---|---|
| `Province` | Province name (English). |
| `District` | District name (English). |
| `Code` | Unique district identifier (`TH` + four digits). |
| `date` | Surveillance month (`YYYY-MM-DD`). |
| `year` | Calendar year. |
| `month` | Calendar month (1–12). |
| `Population` | District population (persons), repeated monthly within each year. |
| `total_cases` | Original dengue case count; `NA` if missing. |
| `imputed_cases` | Model-estimated cases for missing records; otherwise `NA`. |
| `completed_cases` | Original case count or imputed estimate. |
| `record_status` | `observed` or `imputed`. |


## Analysis scope

The completed file retains the district-month estimates available in the
application dataset. Manuscript application summaries should be reproduced
using the district-level missingness eligibility threshold specified in the
Methods. The imputed case counts are model-based estimates and should not be
interpreted as externally confirmed surveillance reports.

