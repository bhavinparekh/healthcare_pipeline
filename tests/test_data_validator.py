import sys
import os
import shutil
# Add scripts directory to Python path (optional, since workflow sets it)
# sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from data_validator import validate_data  # Should work with workflow PYTHONPATH
import pandas as pd
import logging

# Configure logging for testing
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_validate_data():
    # Mock input data
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
        "reference_range": "10-20",
        "unit": ["g/dL"],
        "date_of_birth": ["1990-01-01"]
    })

    # Create mock config directory and file at the expected path
    config_dir = "/opt/airflow/config"
    config_path = f"{config_dir}/pipeline_config.json"
    os.makedirs(config_dir, exist_ok=True)
    if not os.path.exists(config_path):
        with open(config_path, "w") as f:
            f.write('{"input_dir": ".", "output_dir": ".", "vitals_file": "vitals.csv", "labs_file": "lab_results.csv", "vitals_sep": ";", "labs_sep": ",", "validated_subdir": "validated", "issues_file": "validation_issues.txt", "date_format": "%Y-%m-%d", "vitals_columns": ["hospital_id", "measurement_date", "patient_id", "vital_type", "value", "unit", "date_of_birth"], "labs_columns": ["hospital_id", "test_date", "patient_id", "test_type", "result_value", "reference_range", "unit", "date_of_birth"], "vital_ranges": {"blood_pressure_systolic": {"min": 80, "max": 200}}, "lab_ranges": {"hemoglobin": {"min": 10, "max": 20}}}')

    # Save mock data in the root directory (as expected by input_dir=".")
    vitals_data.to_csv("vitals.csv", sep=";", index=False)
    labs_data.to_csv("lab_results.csv", sep=",", index=False)

    # Run validation
    validate_data()

    # Check output
    output_dir = "./validated"
    assert os.path.exists(output_dir), "Validated directory not created"
    assert os.path.exists(f"{output_dir}/vitals.csv"), "Validated vitals.csv not created"
    assert os.path.exists(f"{output_dir}/lab_results.csv"), "Validated lab_results.csv not created"
    with open(f"{output_dir}/validation_issues.txt", "r") as f:
        issues = f.read()
        assert "Out of range" not in issues, "Validation issues detected"

    # Clean up
    for file in ["vitals.csv", "lab_results.csv", f"{output_dir}/vitals.csv", f"{output_dir}/lab_results.csv", f"{output_dir}/validation_issues.txt", config_path]:
        if os.path.exists(file):
            os.remove(file)
    if os.path.exists(output_dir):
        os.rmdir(output_dir)
    if os.path.exists(config_dir):
        os.rmdir(config_dir)
