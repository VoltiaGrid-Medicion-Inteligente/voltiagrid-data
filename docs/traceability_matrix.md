# Traceability Matrix - US-05

This matrix maps the non-functional rules (RN-01 to RN-11) and acceptance criteria (CA-01 to CA-10) to the corresponding project modules and acceptance tests (`tests/test_acceptance.py`).

| Rule / Requirement | Acceptance Criteria | Target Module / Component                  | PyTest Test Function                     | Current Status     |
| :----------------- | :------------------ | :----------------------------------------- | :--------------------------------------- | :----------------- |
| **RN-01**          | CA-01               | Ingestion Pipeline (`data/raw/`)           | `test_ca_01_ingestion_format`            | ⚠️ XFAIL (Pending) |
| **RN-02**          | CA-02               | Cleaning & Imputation Module               | `test_ca_02_missing_values`              | ⚠️ XFAIL (Pending) |
| **RN-03**          | CA-03               | Household Processing (`schemas/f1.py`)     | `test_ca_03_household_uniqueness`        | ⚠️ XFAIL (Pending) |
| **RN-04**          | CA-04               | Date Range Consistency Pipeline            | `test_ca_04_date_range_consistency`      | ⚠️ XFAIL (Pending) |
| **RN-05**          | CA-05               | Tariff Calendar Reference (`F7`)           | `test_ca_05_tariff_band_mapping`         | ⚠️ XFAIL (Pending) |
| **RN-06**          | CA-06               | Transformer Losses Calculation             | `test_ca_06_transform_loss_calculations` | ⚠️ XFAIL (Pending) |
| **RN-07**          | CA-07               | Airflow DAG Orchestration                  | `test_ca_07_airflow_dag_execution`       | ⚠️ XFAIL (Pending) |
| **RN-08**          | CA-08               | PySpark Explicit Schemas (`schemas/`)      | `test_ca_08_schema_enforcement`          | ⚠️ XFAIL (Pending) |
| **RN-09**          | CA-09               | Output Storage Structure (`clean/curated`) | `test_ca_09_output_directory_structure`  | ⚠️ XFAIL (Pending) |
| **RN-10, RN-11**   | CA-10               | Standards, Logging, & Code Language        | `test_ca_10_english_logging_standards`   | ⚠️ XFAIL (Pending) |
