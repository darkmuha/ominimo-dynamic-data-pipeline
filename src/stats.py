import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, max as spark_max, min as spark_min

from src.logger import get_logger

logger = get_logger()


def compute_field_stats(df: DataFrame, fields: List[str] = None) -> Dict:
    """
    Computes global statistics for specified fields across the entire DataFrame.

    :param df: Input DataFrame
    :param fields: List of field names to compute stats for. If None, computes for all columns
    :return: Dictionary containing global statistics
    """
    total_count = df.count()

    if total_count == 0:
        logger.warning("DataFrame is empty, returning empty statistics")
        return {"total_records": 0, "fields": {}}

    if fields is None:
        fields = df.columns

    stats = {"total_records": total_count, "fields": {}}

    for field in fields:
        if field not in df.columns:
            continue

        field_stats = {
            "null_count": df.filter(col(field).isNull()).count(),
            "non_null_count": df.filter(col(field).isNotNull()).count(),
            "distinct_count": df.select(field).distinct().count(),
        }

        field_type = dict(df.dtypes)[field]

        if field_type in ["int", "bigint", "double", "float", "decimal"]:
            numeric_stats = df.select(
                spark_min(col(field)).alias("min"),
                spark_max(col(field)).alias("max"),
            ).collect()[0]

            field_stats["min"] = numeric_stats["min"]
            field_stats["max"] = numeric_stats["max"]

        elif field_type in ["date", "timestamp"]:
            date_stats = df.select(
                spark_min(col(field)).alias("min"),
                spark_max(col(field)).alias("max"),
            ).collect()[0]

            field_stats["min_date"] = (
                str(date_stats["min"]) if date_stats["min"] else None
            )
            field_stats["max_date"] = (
                str(date_stats["max"]) if date_stats["max"] else None
            )

        field_stats["null_percentage"] = (
            (field_stats["null_count"] / total_count * 100) if total_count > 0 else 0
        )

        stats["fields"][field] = field_stats

    return stats


def compute_validation_stats(ok_df: DataFrame, ko_df: DataFrame) -> Dict:
    """
    Computes validation statistics comparing OK and KO records.

    :param ok_df: DataFrame containing valid records
    :param ko_df: DataFrame containing rejected records
    :return: Dictionary containing validation statistics
    """
    ok_count = ok_df.count()
    ko_count = ko_df.count()
    total_count = ok_count + ko_count

    validation_stats = {
        "total_records": total_count,
        "valid_records": ok_count,
        "rejected_records": ko_count,
        "validation_pass_rate": (
            (ok_count / total_count * 100) if total_count > 0 else 0
        ),
        "validation_fail_rate": (
            (ko_count / total_count * 100) if total_count > 0 else 0
        ),
    }

    if ko_count > 0 and "validation_errors" in ko_df.columns:
        error_counts = (
            ko_df.select(explode(col("validation_errors")).alias("error"))
            .groupBy("error")
            .count()
            .orderBy(col("count").desc())
            .collect()
        )

        validation_stats["top_validation_errors"] = [
            {"error": row["error"], "count": row["count"]} for row in error_counts
        ]

    return validation_stats


def format_stats_json(stats: Dict) -> str:
    """
    Formats statistics dictionary into JSON string.

    :param stats: Statistics dictionary
    :return: JSON string representation
    """
    return json.dumps(stats, indent=2, default=str)


def generate_stats_filename(name: str, base_path: str = None) -> str:
    """
    Generates a filename for statistics output based on name.

    :param name: Base name for the stats file
    :param base_path: Optional base directory path
    :return: Full file path
    """
    filename = f"{name}.json"

    if base_path:
        base = Path(base_path)
        base.mkdir(parents=True, exist_ok=True)
        return str(base / filename)

    return filename
