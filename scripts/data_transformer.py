import pandas as pd
import logging
from pathlib import Path
import json
import os
from config_model import PipelineConfig

def transform_data(config_file="pipeline_config.json"):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    config_dir = "/opt/airflow/config" if os.environ.get("DOCKER_ENV", "false").lower() == "true" else "./config"
    config_path = os.path.join(config_dir, config_file if config_file != "test_pipeline_config.json" else config_file)
    if config_file == "test_pipeline_config.json" and os.environ.get("TEST_MODE", "false").lower() != "true":
        config_path = os.path.join(config_dir, "pipeline_config.json")

    try:
        with open(config_path, "r") as f:
            config_data = json.load(f)
        config = PipelineConfig(**config_data).model_dump()

        input_dir = config["output_dir"]  # Using validated output as input
        output_dir = config["output_dir"]
        vitals_file = config["vitals_file"]
        labs_file = config["labs_file"]
        vitals_sep = config["vitals_sep"]
        labs_sep = config["labs_sep"]
        transformed_subdir = config["transformed_subdir"]

        input_path = Path(input_dir) / "validated"
        output_path = Path(output_dir) / transformed_subdir

        vitals = pd.read_csv(input_path / vitals_file, sep=vitals_sep)
        labs = pd.read_csv(input_path / labs_file, sep=labs_sep)

        # Example transformation: Add a transformed column
        vitals["transformed_value"] = vitals["value"] * 2
        labs["transformed_result"] = labs["result_value"] * 1.5

        output_path.mkdir(parents=True, exist_ok=True)
        vitals.to_csv(output_path / vitals_file, sep=vitals_sep, index=False)
        labs.to_csv(output_path / labs_file, sep=labs_sep, index=False)

        logger.info("Data transformation completed")

    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise