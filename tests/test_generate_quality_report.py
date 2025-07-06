import sys
import os
import shutil
import pandas as pd
import logging

# Add scripts directory to Python path (optional, since workflow sets it)
# sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from quality_reporter import generate_quality_report

# Configure logging for testing
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_generate_quality_report():
    # Set test mode
    os.environ["TEST_MODE"] = "true"

    # Mock input data (transformed data with is_abnormal column)
    vitals_data = pd.DataFrame({
        "hospital_id": [1],
        "measurement_date": ["2025-01-01"],
        "patient_id": [100],
        "vital_type": ["blood_pressure_systolic"],
        "value": [90],
        "unit": ["mmHg"],
        "date_of_birth": ["1990-01-01"]
    })
    labs_data = pd.DataFrame({
        "hospital_id": [1],
        "test_date": ["2025-01-01"],
        "patient_id": [100],
        "test_type": ["hemoglobin"],
        "result_value": [12],
        "reference_range": ["10-20"],
        "unit": ["g/dL"],
        "date_of_birth": ["1990-01-01"],
        "is_abnormal": [0]  # Assuming 0 means normal
    })

    # Use existing test_pipeline_config.json (no creation if it exists)
    config_dir = "./config"
    config_path = f"{config_dir}/test_pipeline_config.json"
    os.makedirs(config_dir, exist_ok=True)
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Expected test_pipeline_config.json not found at {config_path}. Please create it with the required test configuration.")

    # Prepare transformed input directory as per config
    transformed_input_dir = "./data/output/transformed"
    os.makedirs(transformed_input_dir, exist_ok=True)
    vitals_data.to_parquet(f"{transformed_input_dir}/clean_vitals.parquet", index=False)
    labs_data.to_parquet(f"{transformed_input_dir}/clean_labs.parquet", index=False)

    # Run quality report generation with the test config file
    generate_quality_report(config_file="test_pipeline_config.json")

    # Check output in reports directory as per config
    reports_output_dir = "./data/output/reports"
    assert os.path.exists(reports_output_dir), "Reports directory not created"
    assert os.path.exists(f"{reports_output_dir}/quality_report.csv"), "Quality report file not created"

    # Verify quality report
    quality_report = pd.read_csv(f"{reports_output_dir}/quality_report.csv")
    assert len(quality_report) == 1, "Quality report should have one row"
    assert quality_report["total_vitals_records"].iloc[0] == 1, "Unexpected total vitals records"
    assert quality_report["total_labs_records"].iloc[0] == 1, "Unexpected total labs records"
    assert quality_report["vitals_missing_values"].iloc[0] == 0, "Unexpected vitals missing values"
    assert quality_report["labs_missing_values"].iloc[0] == 0, "Unexpected labs missing values"
    assert quality_report["abnormal_lab_results"].iloc[0] == 0, "Unexpected abnormal lab results"
    assert quality_report["unique_patients"].iloc[0] == 1, "Unexpected unique patients"

    # Clean up
    for file in [f"{transformed_input_dir}/clean_vitals.parquet", f"{transformed_input_dir}/clean_labs.parquet",
                 f"{reports_output_dir}/quality_report.csv"]:
        if os.path.exists(file):
            os.remove(file)
    if os.path.exists(reports_output_dir):
        shutil.rmtree(reports_output_dir)  # Use rmtree to remove non-empty directories
    if os.path.exists(transformed_input_dir):
        shutil.rmtree(transformed_input_dir)  # Use rmtree to remove non-empty directories
    if os.path.exists("./data/output") and not os.listdir("./data/output"):
        os.rmdir("./data/output")
    if os.path.exists("./data") and not os.listdir("./data"):
        os.rmdir("./data")
    del os.environ["TEST_MODE"]