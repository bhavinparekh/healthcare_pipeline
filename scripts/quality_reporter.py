import pandas as pd
import logging
from pathlib import Path
import json
from config_model import PipelineConfig

def generate_quality_report():
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
        transformed_subdir = config["transformed_subdir"]
        reports_subdir = config["reports_subdir"]

        # Define input and output paths
        input_path = Path(output_dir) / transformed_subdir
        output_path = Path(output_dir) / reports_subdir

        # Read transformed files
        vitals = pd.read_parquet(input_path / "clean_vitals.parquet")
        labs = pd.read_parquet(input_path / "clean_labs.parquet")

        # Calculate quality metrics
        metrics = {
            "total_vitals_records": len(vitals),
            "total_labs_records": len(labs),
            "vitals_missing_values": vitals.isnull().sum().sum(),
            "labs_missing_values": labs.isnull().sum().sum(),
            "abnormal_lab_results": labs["is_abnormal"].sum(),
            "unique_patients": len(set(vitals["patient_id"]).union(set(labs["patient_id"]))),
        }

        # Save quality report
        output_path.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([metrics]).to_csv(output_path / "quality_report.csv", index=False)

        logger.info("Quality report generated")

    except Exception as e:
        logger.error(f"Quality report failed: {e}")
        raise