import sys
import os
import shutil
import pandas as pd
import logging

# Add scripts directory to Python path (optional, since workflow sets it)
# sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from stats_calculator import calculate_statistics

# Configure logging for testing
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_calculate_statistics():
    # Set test mode
    os.environ["TEST_MODE"] = "true"

    # Mock input data (transformed data)
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
        "date_of_birth": ["1990-01-01"]
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

    # Run statistics calculation with the test config file
    calculate_statistics(config_file="test_pipeline_config.json")

    # Check output in stats directory as per config
    stats_output_dir = "./data/output/stats"
    assert os.path.exists(stats_output_dir), "Stats directory not created"
    assert os.path.exists(f"{stats_output_dir}/vitals_stats.parquet"), "Vitals stats file not created"
    assert os.path.exists(f"{stats_output_dir}/lab_stats.parquet"), "Labs stats file not created"

    # Verify statistics
    vitals_stats = pd.read_parquet(f"{stats_output_dir}/vitals_stats.parquet")
    lab_stats = pd.read_parquet(f"{stats_output_dir}/lab_stats.parquet")
    assert "mean" in vitals_stats.columns, "Mean column missing in vitals stats"
    assert "median" in vitals_stats.columns, "Median column missing in vitals stats"
    assert "min" in vitals_stats.columns, "Min column missing in vitals stats"
    assert "max" in vitals_stats.columns, "Max column missing in vitals stats"
    assert "count" in vitals_stats.columns, "Count column missing in vitals stats"
    assert "mean" in lab_stats.columns, "Mean column missing in lab stats"
    assert "median" in lab_stats.columns, "Median column missing in lab stats"
    assert "min" in lab_stats.columns, "Min column missing in lab stats"
    assert "max" in lab_stats.columns, "Max column missing in lab stats"
    assert "count" in lab_stats.columns, "Count column missing in lab stats"
    # Verify specific values (single row due to mock data)
    assert vitals_stats["mean"].iloc[0] == 90.0, "Unexpected mean for vitals"
    assert lab_stats["mean"].iloc[0] == 12.0, "Unexpected mean for labs"
    assert vitals_stats["count"].iloc[0] == 1, "Unexpected count for vitals"
    assert lab_stats["count"].iloc[0] == 1, "Unexpected count for labs"

    # Clean up
    for file in [f"{transformed_input_dir}/clean_vitals.parquet", f"{transformed_input_dir}/clean_labs.parquet",
                 f"{stats_output_dir}/vitals_stats.parquet", f"{stats_output_dir}/lab_stats.parquet"]:
        if os.path.exists(file):
            os.remove(file)
    if os.path.exists(stats_output_dir):
        shutil.rmtree(stats_output_dir)  # Use rmtree to remove non-empty directories
    if os.path.exists(transformed_input_dir):
        shutil.rmtree(transformed_input_dir)  # Use rmtree to remove non-empty directories
    if os.path.exists("./data/output") and not os.listdir("./data/output"):
        os.rmdir("./data/output")
    if os.path.exists("./data") and not os.listdir("./data"):
        os.rmdir("./data")
    del os.environ["TEST_MODE"]