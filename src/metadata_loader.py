import json
from pathlib import Path
from typing import Any, Dict

from src.logger import get_logger

logger = get_logger()


def load_metadata(path: str) -> Dict[str, Any]:
    """
    Loads pipeline metadata from a JSON file.

    :param path: File path to the metadata JSON file
    :return: Dictionary containing metadata with dataflows, sources, and validations
    """
    metadata_path = Path(path)
    if not metadata_path.is_file():
        logger.error(f"Metadata file not found: {metadata_path}")
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")

    with metadata_path.open("r", encoding="utf-8") as f:
        metadata = json.load(f)
    return metadata
