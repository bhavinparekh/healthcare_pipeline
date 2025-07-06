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
    # Set test mode
    os.environ["TEST_MODE"] = "true"

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

    # Use existing test_pipeline_config.json (no creation if it exists)
    config_dir = "./config"
    config_path = f"{config_dir}/test_pipeline_config.json"
    os.makedirs(config_dir, exist_ok=True)
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Expected test_pipeline_config.json not found at {config_path}. Please create it with the required test configuration.")

    # Save mock data in the test input directory as per config
    test_input_dir = "./data/input"
    os.makedirs(test_input_dir, exist_ok=True)
    vitals_data.to_csv(f"{test_input_dir}/vitals.csv", sep=";", index=False)
    labs_data.to_csv(f"{test_input_dir}/lab_results.csv", sep=",", index=False)

    # Run validation with the test config file
    validate_data(config_file="test_pipeline_config.json")

    # Check output in test output directory as per config
    test_output_dir = "./data/output"
    output_dir = f"{test_output_dir}/validated"
    assert os.path.exists(output_dir), "Validated directory not created"
    assert os.path.exists(f"{output_dir}/vitals.csv"), "Validated vitals.csv not created"
    assert os.path.exists(f"{output_dir}/lab_results.csv"), "Validated lab_results.csv not created"
    with open(f"{output_dir}/validation_issues.txt", "r") as f:
        issues = f.read()
        assert "Out of range" not in issues, "Validation issues detected"

    # Clean up

    if os.path.exists(output_dir):
        os.rmdir(output_dir)
    if os.path.exists(test_input_dir) and not os.listdir(test_input_dir):  # Only remove if empty
        os.rmdir(test_input_dir)
    if os.path.exists(test_output_dir) and not os.listdir(test_output_dir):  # Only remove if empty
        os.rmdir(test_output_dir)
    del os.environ["TEST_MODE"]