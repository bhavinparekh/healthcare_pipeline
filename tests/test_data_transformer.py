import sys
import os
import shutil
import pandas as pd
import logging

# Add scripts directory to Python path (optional, since workflow sets it)
# sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from data_transformer import transform_data

# Configure logging for testing
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_transform_data():
    # Set test mode
    os.environ["TEST_MODE"] = "true"

    # Mock input data (validated data)
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

    # Prepare validated input directory as per config
    validated_input_dir = "./data/output/validated"
    os.makedirs(validated_input_dir, exist_ok=True)
    vitals_data.to_csv(f"{validated_input_dir}/vitals.csv", sep=";", index=False)
    labs_data.to_csv(f"{validated_input_dir}/lab_results.csv", sep=",", index=False)

    # Run transformation with the test config file
    transform_data(config_file="test_pipeline_config.json")

    # Check output in transformed directory as per config
    transformed_output_dir = "./data/output/transformed"
    assert os.path.exists(transformed_output_dir), "Transformed directory not created"
    assert os.path.exists(f"{transformed_output_dir}/vitals.csv"), "Transformed vitals.csv not created"
    assert os.path.exists(f"{transformed_output_dir}/lab_results.csv"), "Transformed lab_results.csv not created"

    # Verify transformation
    transformed_vitals = pd.read_csv(f"{transformed_output_dir}/vitals.csv", sep=";")
    transformed_labs = pd.read_csv(f"{transformed_output_dir}/lab_results.csv", sep=",")
    assert "transformed_value" in transformed_vitals.columns, "Transformed value column missing"
    assert "transformed_result" in transformed_labs.columns, "Transformed result column missing"
    assert transformed_vitals["transformed_value"].iloc[0] == 180, "Value transformation incorrect (expected 90 * 2 = 180)"
    assert transformed_labs["transformed_result"].iloc[0] == 18.0, "Result transformation incorrect (expected 12 * 1.5 = 18.0)"

    # Clean up
    for file in [f"{transformed_output_dir}/vitals.csv", f"{transformed_output_dir}/lab_results.csv"]:
        if os.path.exists(file):
            os.remove(file)
    if os.path.exists(transformed_output_dir):
        shutil.rmtree(transformed_output_dir)  # Use rmtree to remove non-empty directories
    if os.path.exists(validated_input_dir) and not os.listdir(validated_input_dir):
        os.rmdir(validated_input_dir)
    if os.path.exists("./data/output") and not os.listdir("./data/output"):
        os.rmdir("./data/output")
    if os.path.exists("./data") and not os.listdir("./data"):
        os.rmdir("./data")
    del os.environ["TEST_MODE"]