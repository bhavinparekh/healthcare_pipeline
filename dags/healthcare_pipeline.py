from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import data_validator
import data_transformer
import stats_calculator
import quality_reporter

# Define default arguments for the DAG
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    "healthcare_data_pipeline",
    default_args=default_args,
    description="Pipeline for processing healthcare data",
    schedule_interval="@monthly",
    start_date=datetime(2025, 7, 1),
    catchup=False,
) as dag:
    # Task to validate data
    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=data_validator.validate_data,
    )

    # Task to transform data
    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=data_transformer.transform_data,
    )

    # Task to calculate statistics
    calculate_statistics = PythonOperator(
        task_id="calculate_statistics",
        python_callable=stats_calculator.calculate_statistics,
    )

    # Task to generate quality report
    generate_quality_report = PythonOperator(
        task_id="generate_quality_report",
        python_callable=quality_reporter.generate_quality_report,
    )

    # Set task dependencies
    validate_data >> transform_data >> [calculate_statistics, generate_quality_report]