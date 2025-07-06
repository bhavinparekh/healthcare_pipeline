# Healthcare Data Pipeline Design

## Overview
This document outlines the design and implementation of a batch processing pipeline for healthcare data, built to process monthly patient monitoring data from multiple hospitals. The pipeline is implemented using Apache Airflow and Python, handling vital signs (`vitals.csv`) and lab results (`lab_results.csv`) to produce validated, transformed, and aggregated data suitable for research and analysis. A centralized configuration file (`pipeline_config.json`) defines file paths, separators, and validation rules, validated using a Pydantic model in `config_model.py`. Configuration values are extracted into local variables for clarity, and no error handling is applied for config loading, assuming the file is valid and present. The design prioritizes data quality, error handling for data processing, and efficient storage, while maintaining simplicity and scalability.

## Solution Architecture

### Pipeline Overview
The pipeline consists of four tasks orchestrated by an Apache Airflow DAG (`healthcare_pipeline.py`):
1. **Data Validation**: Validates input data using `pipeline_config.json`.
2. **Data Transformation**: Transforms validated data for analysis.
3. **Statistics Calculation**: Computes monthly aggregates for vitals and lab results.
4. **Quality Reporting**: Generates data quality metrics.

The pipeline uses a file-based approach, with all paths, separators, and validation parameters defined in `pipeline_config.json`. Outputs are stored in structured directories under `/opt/airflow/data/output/`.

### Data Flow
- **Input**: Reads `vitals.csv` (semicolon-separated) and `lab_results.csv` (comma-separated) from `/opt/airflow/data/input/`.
- **Validation** (`data_validator.py`):
  - Validates column presence, numeric fields (`value`, `result_value`), date formats, and ranges using `pipeline_config.json`.
  - Logs issues to `validated/validation_issues.txt`.
  - Saves validated data to `/opt/airflow/data/output/validated/vitals.csv` and `labs.csv`.
- **Transformation** (`data_transformer.py`):
  - Reads validated files, calculates `age`, standardizes units, flags abnormal lab results, and converts dates to `datetime`.
  - Saves to `/opt/airflow/data/output/transformed/clean_vitals.parquet` and `clean_labs.parquet`.
- **Statistics Calculation** (`stats_calculator.py`):
  - Reads transformed Parquet files, computes monthly aggregates (mean, median, min, max, count) by `hospital_id` and type.
  - Saves to `/opt/airflow/data/output/stats/vitals_stats.parquet` and `lab_stats.parquet`.
- **Quality Reporting** (`quality_reporter.py`):
  - Reads transformed Parquet files, generates metrics (e.g., total records, missing values, abnormal results, unique patients).
  - Saves to `/opt/airflow/data/output/reports/quality_report.csv`.

### Architecture Diagram
Create using a tool like draw.io or Lucidchart and save as `docs/architecture.png`:
```
[Input: vitals.csv, lab_results.csv] → [Config: pipeline_config.json]
  ↓
[Validate: data_validator.py]
  ↓ Outputs: validated/vitals.csv, labs.csv, validation_issues.txt
[Transform: data_transformer.py]
  ↓ Outputs: transformed/clean_vitals.parquet, clean_labs.parquet
  ↓
[Stats: stats_calculator.py] → [Outputs: stats/vitals_stats.parquet, lab_stats.parquet]
[Quality: quality_reporter.py] → [Outputs: reports/quality_report.csv]
```

### Considerations
- **Data Quality Validation**:
  - Checks column presence, numeric fields, dates, and ranges (e.g., blood pressure systolic: 80–200) from `pipeline_config.json`.
  - Uses Pydantic model in `config_model.py` for schema validation, assuming the config file is valid and present.
  - Non-numeric values are converted to `NaN`, invalid dates are logged.
- **Transformation Logic**:
  - Calculates patient age, standardizes units, flags abnormal lab results, and ensures `datetime` columns.
- **Error Handling**:
  - Try-except blocks log errors and raise exceptions for Airflow retries (3 attempts, 5-minute delay) during data processing.
  - No error handling for config loading; assumes `pipeline_config.json` exists and is valid.
  - Config validation errors (via Pydantic) will fail tasks, relying on Airflow retries.
- **Storage Strategy**:
  - CSV for validated data (readability), Parquet for transformed/statistics data (efficiency).
  - Outputs organized in `validated/`, `transformed/`, `stats/`, `reports/` subdirectories.

## Data Quality Approach
- **Validation Checks** (`data_validator.py`):
  - **Columns**: Ensures expected columns from `pipeline_config.json`.
  - **Numeric**: Converts `value` and `result_value` to numeric, logs non-numeric values.
  - **Dates**: Standardizes to `YYYY-MM-DD`, logs invalid formats.
  - **Ranges**: Validates vital signs (e.g., systolic: 80–200) and lab results (e.g., hemoglobin: 10–20 or `reference_range`).
  - **Missing Values**: Logs counts for all columns.
- **Quality Metrics** (`quality_reporter.py`):
  - Total records, missing values, abnormal lab results, unique patients.
- **Logging**: Issues saved to `validation_issues.txt` for traceability.

## Setup and Running Instructions
1. **Prepare Files**:
   - Place `data_validator.py`, `data_transformer.py`, `stats_calculator.py`, `quality_reporter.py`, `config_model.py` in `scripts/`.
   - Place `healthcare_pipeline.py` in `dags/`.
   - Place `pipeline_config.json` in `config/`.
   - Ensure `data/input/vitals.csv` and `lab_results.csv` have correct formats:
     - `vitals.csv`: `hospital_id;measurement_date;patient_id;vital_type;value;unit;date_of_birth`
     - `lab_results.csv`: `hospital_id,test_date,patient_id,test_type,result_value,reference_range,unit,date_of_birth`
2. **Set Permissions** (Windows):
   ```powershell
   icacls "C:\Users\bhavi\VsProjects\healthcare_pipeline\data" /grant Everyone:F /T
   icacls "C:\Users\bhavi\VsProjects\healthcare_pipeline\logs" /grant Everyone:F /T
   icacls "C:\Users\bhavi\VsProjects\healthcare_pipeline\config" /grant Everyone:F /T
   icacls "C:\Users\bhavi\VsProjects\healthcare_pipeline\scripts" /grant Everyone:F /T
   ```
3. **Update Docker Compose**:
   - Ensure `docker-compose.yml` mounts directories:
     ```yaml
     volumes:
       - ./data:/opt/airflow/data
       - ./logs:/opt/airflow/logs
       - ./config:/opt/airflow/config
       - ./scripts:/opt/airflow/scripts
     ```
4. **Install Dependencies**:
   - Update `requirements.txt`:
     ```text
     pandas==2.2.2
     apache-airflow==2.9.3
     psycopg2-binary==2.9.9
     pydantic==2.8.2
     ```
5. **Start Docker**:
   ```powershell
   docker-compose up -d --build
   ```
6. **Verify Containers**:
   ```powershell
   docker-compose ps
   ```
   Ensure `airflow-init-1` is `exited (0)` and others are `running`.
7. **Run Pipeline**:
   - Access Airflow UI at `http://localhost:8080` (admin/admin).
   - Enable and trigger `healthcare_data_pipeline`.
8. **Check Outputs**:
   - Verify `data/output/` contains:
     - `validated/vitals.csv`, `labs.csv`, `validation_issues.txt`
     - `transformed/clean_vitals.parquet`, `clean_labs.parquet`
     - `stats/vitals_stats.parquet`, `lab_stats.parquet`
     - `reports/quality_report.csv`
   - Check logs:
     ```powershell
     docker-compose logs airflow-scheduler
     ```

## Assumptions
- Input files are in `data/input/` with specified columns.
- Non-numeric values and invalid dates are errors, converted to `NaN`.
- `pipeline_config.json` exists, is correctly formatted, and matches the Pydantic schema in `config_model.py`.
- Output, config, and scripts directories are writable.
- Monthly batch processing, no real-time requirements.

## Limitations
- Limited to predefined ranges in `pipeline_config.json`.
- `format="mixed"` for dates may fail for highly irregular formats.
- No real-time processing.
- No external data quality tools beyond Pydantic.
- No automated data correction.
- No error handling for missing or malformed `pipeline_config.json`, relying on Airflow retries.

## Future Improvements
- Integrate Great Expectations for advanced schema validation.
- Support dynamic date parsing with `dateutil`.
- Parallelize tasks with Airflow task groups or Dask.
- Add Airflow notifications for failures.
- Implement automated data cleaning.
- Add unit tests for all scripts.
- Reintroduce config error handling for robustness.

## Technologies Used
- **Python**: `pandas`, `pathlib`, `logging`, `json`, `pydantic`.
- **Apache Airflow**: Workflow orchestration.
- **Pandas**: Data processing.
- **JSON**: Centralized configuration.
- **Pydantic**: Configuration validation in `config_model.py`.
- **Parquet/CSV**: Storage formats.

## Notes
- Centralized configuration in `pipeline_config.json` with Pydantic validation in `config_model.py` improves maintainability.
- Configuration values are extracted into local variables for clarity.
- Lack of config error handling reduces robustness; Airflow retries mitigate failures.
- File-based data flow ensures simplicity and scalability.
- Project structure (`scripts/`, `dags/`, `data/`, `config/`) follows Airflow conventions.
- Scripts include comments for key steps.