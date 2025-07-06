import pandas as pd
import logging
from datetime import datetime
from pathlib import Path
import json
from config_model import PipelineConfig

def transform_data():
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Load and validate configuration
        with open("/opt/airflow/config/pipeline_config.json", "r") as f:
            config_data = json.load(f)
        config = PipelineConfig(**config_data).dict()

        # Extract config values
        output_dir = config["output_dir"]
        validated_subdir = config["validated_subdir"]
        transformed_subdir = config["transformed_subdir"]
        vitals_file = config["vitals_file"]
        labs_file = config["labs_file"]
        vitals_sep = config["vitals_sep"]
        labs_sep = config["labs_sep"]
        issues_file = config["issues_file"]

        # Define input and output paths
        input_path = Path(output_dir) / validated_subdir
        output_path = Path(output_dir) / transformed_subdir

        # Read validated files
        vitals = pd.read_csv(input_path / vitals_file, sep=vitals_sep)
        labs = pd.read_csv(input_path / labs_file, sep=labs_sep)

        # Read issues if file exists
        issues_path = input_path / issues_file
        issues = []
        if issues_path.exists():
            with open(issues_path, "r") as f:
                issues = f.read().splitlines()
            logger.info(f"Loaded {len(issues)} validation issues")

        # Calculate age
        current_year = datetime.now().year
        vitals["age"] = current_year - pd.to_datetime(vitals["date_of_birth"]).dt.year
        labs["age"] = current_year - pd.to_datetime(labs["date_of_birth"]).dt.year

        # Convert date columns to datetime for Parquet storage
        vitals["measurement_date"] = pd.to_datetime(vitals["measurement_date"])
        labs["test_date"] = pd.to_datetime(labs["test_date"])

        # Flag abnormal lab values
        labs["is_abnormal"] = labs.apply(
            lambda row: row["result_value"] < float(row["reference_range"].split("-")[0])
            or row["result_value"] > float(row["reference_range"].split("-")[1]),
            axis=1,
        )

        # Standardize units
        vitals["unit"] = vitals["unit"].str.lower()
        labs["unit"] = labs["unit"].str.lower()

        # Save transformed data
        output_path.mkdir(parents=True, exist_ok=True)
        vitals.to_parquet(output_path / "clean_vitals.parquet")
        labs.to_parquet(output_path / "clean_labs.parquet")

        logger.info("Transformation completed")

    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise