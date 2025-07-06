import pandas as pd
import logging
from pathlib import Path
import json
from config_model import PipelineConfig

def validate_data():
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    try:
        # Load and validate configuration
        with open("/opt/airflow/config/pipeline_config.json", "r") as f:
            config_data = json.load(f)
        config = PipelineConfig(**config_data).dict()

        # Extract config values
        input_dir = config["input_dir"]
        output_dir = config["output_dir"]
        vitals_file = config["vitals_file"]
        labs_file = config["labs_file"]
        vitals_sep = config["vitals_sep"]
        labs_sep = config["labs_sep"]
        validated_subdir = config["validated_subdir"]
        issues_file = config["issues_file"]
        date_format = config["date_format"]
        vitals_columns = config["vitals_columns"]
        labs_columns = config["labs_columns"]
        vital_ranges = config["vital_ranges"]
        lab_ranges = config["lab_ranges"]

        # Define output path
        output_path = Path(output_dir) / validated_subdir

        # Read input files
        vitals = pd.read_csv(Path(input_dir) / vitals_file, sep=vitals_sep)
        labs = pd.read_csv(Path(input_dir) / labs_file, sep=labs_sep)

        issues = []

        # Validate column presence
        missing_vitals_cols = [col for col in vitals_columns if col not in vitals.columns]
        if missing_vitals_cols:
            issues.append(f"Missing columns in {vitals_file}: {missing_vitals_cols}")
            logger.warning(f"Missing columns in {vitals_file}: {missing_vitals_cols}")

        missing_labs_cols = [col for col in labs_columns if col not in labs.columns]
        if missing_labs_cols:
            issues.append(f"Missing columns in {labs_file}: {missing_labs_cols}")
            logger.warning(f"Missing columns in {labs_file}: {missing_labs_cols}")

        # Convert vitals['value'] to numeric, handle non-numeric values
        non_numeric_vitals = vitals[~vitals["value"].apply(lambda x: isinstance(x, (int, float)) or (isinstance(x, str) and x.replace(".", "").replace("-", "").isdigit()))]
        if not non_numeric_vitals.empty:
            logger.warning(f"Non-numeric values in vitals['value']: {non_numeric_vitals['value'].tolist()}")
            issues.append(f"Non-numeric values in vitals['value']: {len(non_numeric_vitals)} records")

        vitals["value"] = pd.to_numeric(vitals["value"], errors="coerce")
        if vitals["value"].isna().any():
            logger.warning(f"Found {vitals['value'].isna().sum()} invalid numeric values in vitals['value']")
            issues.append(f"Invalid numeric values in vitals['value']: {vitals['value'].isna().sum()} records")

        # Convert labs['result_value'] to numeric, handle non-numeric values
        non_numeric_labs = labs[~labs["result_value"].apply(lambda x: isinstance(x, (int, float)) or (isinstance(x, str) and x.replace(".", "").replace("-", "").isdigit()))]
        if not non_numeric_labs.empty:
            logger.warning(f"Non-numeric values in labs['result_value']: {non_numeric_labs['result_value'].tolist()}")
            issues.append(f"Non-numeric values in labs['result_value']: {len(non_numeric_labs)} records")

        labs["result_value"] = pd.to_numeric(labs["result_value"], errors="coerce")
        if labs["result_value"].isna().any():
            logger.warning(f"Found {labs['result_value'].isna().sum()} invalid numeric values in labs['result_value']")
            issues.append(f"Invalid numeric values in labs['result_value']: {labs['result_value'].isna().sum()} records")

        # Validate and standardize date formats
        for col in ["measurement_date", "date_of_birth"]:
            try:
                vitals[col] = pd.to_datetime(vitals[col], format="mixed", errors="coerce")
                invalid_dates = vitals[vitals[col].isna()][col].index
                if not invalid_dates.empty:
                    logger.warning(f"Invalid {col} in vitals: {vitals.loc[invalid_dates, col].tolist()}")
                    issues.append(f"Invalid {col} in vitals: {len(invalid_dates)} records")
                vitals[col] = vitals[col].dt.strftime(date_format)
            except Exception as e:
                logger.error(f"Failed to parse {col} in vitals: {e}")
                issues.append(f"Failed to parse {col} in vitals: {e}")

        for col in ["test_date", "date_of_birth"]:
            try:
                labs[col] = pd.to_datetime(labs[col], format="mixed", errors="coerce")
                invalid_dates = labs[labs[col].isna()][col].index
                if not invalid_dates.empty:
                    logger.warning(f"Invalid {col} in labs: {labs.loc[invalid_dates, col].tolist()}")
                    issues.append(f"Invalid {col} in labs: {len(invalid_dates)} records")
                labs[col] = labs[col].dt.strftime(date_format)
            except Exception as e:
                logger.error(f"Failed to parse {col} in labs: {e}")
                issues.append(f"Failed to parse {col} in labs: {e}")

        # Validate vitals ranges
        for vital_type, ranges in vital_ranges.items():
            mask = (vitals["vital_type"] == vital_type) & (
                (vitals["value"] < ranges["min"]) | (vitals["value"] > ranges["max"])
            )
            if mask.any():
                issues.append(f"Out of range {vital_type}: {len(vitals[mask])} records")
                logger.warning(f"Out of range {vital_type}: {len(vitals[mask])} records")

        # Validate labs ranges
        for _, row in labs.iterrows():
            if pd.notna(row["result_value"]):
                test_type = row["test_type"]
                if test_type in lab_ranges:
                    ref_min, ref_max = lab_ranges[test_type]["min"], lab_ranges[test_type]["max"]
                else:
                    ref_min, ref_max = map(float, row["reference_range"].split("-"))
                if not (ref_min <= row["result_value"] <= ref_max):
                    issues.append(f"Out of range {row['test_type']} for patient {row['patient_id']}")
                    logger.warning(f"Out of range {row['test_type']} for patient {row['patient_id']}")

        # Validate missing values
        missing_vitals = vitals.isnull().sum()
        for col, count in missing_vitals.items():
            if count > 0:
                issues.append(f"Missing {col} in vitals: {count} records")
                logger.warning(f"Missing {col} in vitals: {count} records")

        missing_labs = labs.isnull().sum()
        for col, count in missing_labs.items():
            if count > 0:
                issues.append(f"Missing {col} in labs: {count} records")
                logger.warning(f"Missing {col} in labs: {count} records")

        # Log issues
        if issues:
            logger.warning(f"Validation issues: {issues}")
            output_path.mkdir(parents=True, exist_ok=True)
            with open(output_path / issues_file, "w") as f:
                f.write("\n".join(issues))

        # Save validated data
        output_path.mkdir(parents=True, exist_ok=True)
        vitals.to_csv(output_path / vitals_file, sep=vitals_sep, index=False)
        labs.to_csv(output_path / labs_file, sep=labs_sep, index=False)

        logger.info("Validation completed")

    except Exception as e:
        logger.error(f"Validation failed: {e}")
        raise