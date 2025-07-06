from pydantic import BaseModel
from typing import List, Dict

class PipelineConfig(BaseModel):
    input_dir: str
    output_dir: str
    vitals_file: str
    labs_file: str
    vitals_sep: str
    labs_sep: str
    validated_subdir: str
    transformed_subdir: str
    stats_subdir: str
    reports_subdir: str
    issues_file: str
    date_format: str
    vitals_columns: List[str]
    labs_columns: List[str]
    vital_ranges: Dict[str, Dict[str, float]]
    lab_ranges: Dict[str, Dict[str, float]]