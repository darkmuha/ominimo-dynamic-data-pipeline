import json
from pathlib import Path
from typing import Any, Dict


def load_metadata(path: str) -> Dict[str, Any]:
    """
    Loads pipeline metadata from a JSON file.

    :param path: File path to the metadata JSON file
    :return: Dictionary containing metadata with dataflows, sources, and validations
    """
    metadata_path = Path(path)
    if not metadata_path.is_file():
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")

    with metadata_path.open("r", encoding="utf-8") as f:
        return json.load(f)

