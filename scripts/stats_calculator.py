import pandas as pd
import logging
from pathlib import Path
import json
import os 
from config_model import PipelineConfig

def calculate_statistics(config_file="pipeline_config.json"):
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Load and validate configuration
        config_dir = "/opt/airflow/config" if os.environ.get("DOCKER_ENV", "false").lower() == "true" else "./config"
        config_path = os.path.join(config_dir, config_file if config_file != "test_pipeline_config.json" else config_file)
        if config_file == "test_pipeline_config.json" and os.environ.get("TEST_MODE", "false").lower() != "true":
            config_path = os.path.join(config_dir, "pipeline_config.json")

        with open(config_path, "r") as f:
            config_data = json.load(f)
        config = PipelineConfig(**config_data).model_dump()

        # Extract config values
        output_dir = config["output_dir"]
        transformed_subdir = config["transformed_subdir"]
        stats_subdir = config["stats_subdir"]

        # Define input and output paths
        input_path = Path(output_dir) / transformed_subdir
        output_path = Path(output_dir) / stats_subdir

        # Read transformed files
        vitals = pd.read_parquet(input_path / "clean_vitals.parquet")
        labs = pd.read_parquet(input_path / "clean_labs.parquet")

        # Log column types for debugging
        logger.info(f"vitals['measurement_date'] type: {vitals['measurement_date'].dtype}")
        logger.info(f"labs['test_date'] type: {labs['test_date'].dtype}")

        # Calculate vitals statistics
        vitals_stats = (
            vitals.groupby(["hospital_id", "vital_type", pd.Grouper(key="measurement_date", freq="ME")])
            ["value"]
            .agg(["mean", "median", "min", "max", "count"])
            .reset_index()
        )

        # Calculate lab statistics
        lab_stats = (
            labs.groupby(["hospital_id", "test_type", pd.Grouper(key="test_date", freq="ME")])
            ["result_value"]
            .agg(["mean", "median", "min", "max", "count"])
            .reset_index()
        )

        # Save statistics
        output_path.mkdir(parents=True, exist_ok=True)
        vitals_stats.to_parquet(output_path / "vitals_stats.parquet")
        lab_stats.to_parquet(output_path / "lab_stats.parquet")

        logger.info("Statistics calculated")

    except Exception as e:
        logger.error(f"Statistics failed: {e}")
        raise