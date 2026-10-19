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

Original district-level dengue surveillance records. Missing surveillance
reports are retained as `NA` in `total_cases`.

| Column | Unit or values | Description |
|---|---|---|---|
| `Province` | English name | Province containing the district. |
| `District` | English name | District name. |
| `Code` | `TH` followed by four digits | Unique district identifier. |
| `date` | `YYYY-MM-DD` | First day of the surveillance month. |
| `year` | Calendar year | Year of the surveillance record. |
| `month` | 1-12 | Calendar month. |
| `total_cases` | Dengue cases | Originally reported monthly dengue case count. `NA` indicates a missing surveillance report. |

### `df_district_imputed.csv`

Completed district-level series derived from the original surveillance data and
the model estimates used in the application analysis. Original observations
remain unchanged, and `record_status` identifies the provenance of every
completed value.

| Column | Unit or values | Description |
|---|---|---|---|
| `Province` | English name | Province containing the district. |
| `District` | English name | District name. |
| `Code` | `TH` followed by four digits | Unique district identifier. |
| `date` | `YYYY-MM-DD` | First day of the surveillance month. |
| `year` | Calendar year | Year of the surveillance record. |
| `month` | 1-12 | Calendar month. |
| `Population` | Persons | District population for the corresponding year, repeated for each month in that year and used as the denominator for incidence calculations. |
| `total_cases` | Dengue cases | Originally reported monthly dengue case count. This remains `NA` when the original surveillance report was missing. |
| `imputed_cases` | Estimated dengue cases | Model estimate for an originally missing district-month. This is `NA` for observed records. |
| `completed_cases` | Dengue cases | Analysis-ready count: the original `total_cases` when observed, otherwise `imputed_cases`. |
| `record_status` | `observed` or `imputed` | Indicates whether `completed_cases` comes from the original surveillance record or model-based imputation. |

## Missing-value and provenance conventions

- `NA` in `total_cases` indicates that no original surveillance value was
  available for that district-month.
- For an observed record, `imputed_cases` is `NA`, `completed_cases` equals
  `total_cases`, and `record_status` is `observed`.
- For an imputed record, `total_cases` remains `NA`, `completed_cases` equals
  `imputed_cases`, and `record_status` is `imputed`.
- Original observed values were not replaced by model predictions.

| `total_cases` | `imputed_cases` | `completed_cases` | `record_status` |
|---:|---:|---:|---|
| 12 | `NA` | 12 | `observed` |
| `NA` | 8 | 8 | `imputed` |

## Analysis scope

The completed file retains the district-month estimates available in the
application dataset. Manuscript application summaries should be reproduced
using the district-level missingness eligibility threshold specified in the
Methods. The imputed case counts are model-based estimates and should not be
interpreted as externally confirmed surveillance reports.

