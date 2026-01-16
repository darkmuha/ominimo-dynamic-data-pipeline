from datetime import datetime
from pathlib import Path
from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, current_timestamp, expr, when

from src.logger import get_logger
from src.stats import (
    compute_field_stats,
    compute_validation_stats,
    format_stats_json,
    generate_stats_filename,
)
from src.validator import apply_validations

logger = get_logger()


def _col_from_path(path: str):
    """
    Builds a Spark column from a dotted path.

    :param path: Dotted path string (e.g., 'driver.age' for nested fields)
    :return: Spark Column object
    """
    return col(path)


def normalize_fields(df: DataFrame, fields: List[Dict]) -> DataFrame:
    """
    Normalizes heterogeneous JSON into a flatter, canonical schema by coalescing fields from multiple sources.

    :param df: Input DataFrame with potentially nested or inconsistent field names
    :param fields: List of field configuration dictionaries, each containing 'name' (target column name) and 'sources' (list of field paths to coalesce in priority order)
    :return: DataFrame with normalized canonical columns
    """
    result = df

    for field_cfg in fields:
        target = field_cfg["name"]
        sources = field_cfg.get("sources", [])
        if not sources:
            continue

        cols = [_col_from_path(p) for p in sources]
        value_expr = coalesce(*cols) if len(cols) > 1 else cols[0]

        result = result.withColumn(target, value_expr)

    return result


def drop_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Drops a list of columns from the DataFrame.

    :param df: Input DataFrame
    :param columns: List of column names to drop
    :return: DataFrame with specified columns removed
    """
    if not columns:
        return df
    return df.drop(*columns)


def _calculate_risk_category(age_col):
    """
    Calculates insurance risk category based on driver age.

    :param age_col: Spark column expression for driver age
    :return: Spark column expression with risk category values
    """
    return (
        when(age_col.isNull(), "Unknown")
        .when((age_col >= 18) & (age_col <= 25), "High Risk")
        .when((age_col >= 26) & (age_col <= 65), "Standard Risk")
        .when(age_col >= 66, "Medium Risk")
        .otherwise("Unknown")
    )


def add_fields(df: DataFrame, fields: List[Dict]) -> DataFrame:
    """
    Adds or overrides simple metadata fields on the DataFrame.

    :param df: Input DataFrame
    :param fields: List of field configuration dictionaries, each containing 'name' (target column name) and 'function' (supported function name like "current_timestamp", "risk_category")
    :return: DataFrame with added metadata fields
    """
    result = df

    for field_cfg in fields:
        name = field_cfg["name"]
        func = field_cfg.get("function")

        if func == "current_timestamp":
            result = result.withColumn(name, current_timestamp())
        elif func == "risk_category":
            age_col = expr("try_cast(driver_age as int)")
            result = result.withColumn(name, _calculate_risk_category(age_col))
        else:
            raise ValueError(
                f"Unsupported add_fields function: {func!r} for field {name!r}"
            )

    return result


def apply_transformations(
    dataflow_meta: dict, frames: Dict[str, DataFrame]
) -> Dict[str, DataFrame]:
    """
    Applies transformations declared in metadata to the given DataFrames.

    :param dataflow_meta: Dataflow metadata dictionary containing transformation definitions
    :param frames: Dictionary mapping transformation names to DataFrames
    :return: Updated dictionary of DataFrames after applying all transformations
    """
    transformations = dataflow_meta.get("transformations", [])
    if not transformations:
        return frames

    updated = dict(frames)

    for t in transformations:
        t_type = t.get("type")
        params = t.get("params", {})
        input_name = params["input"]
        output_name = params.get("output", t["name"])

        input_df = updated[input_name]

        if t_type == "normalize_fields":
            fields = params.get("fields", [])
            output_df = normalize_fields(input_df, fields)
            updated[output_name] = output_df
        elif t_type == "drop_columns":
            columns = params.get("columns", [])
            output_df = drop_columns(input_df, columns)
            updated[output_name] = output_df
        elif t_type == "add_fields":
            fields = params.get("fields", [])
            output_df = add_fields(input_df, fields)
            updated[output_name] = output_df
        elif t_type == "validate_fields":
            validations = params.get("validations", [])
            ok_output = params.get("ok_output", f"{t['name']}_ok")
            ko_output = params.get("ko_output", f"{t['name']}_ko")
            ok_df, ko_df = apply_validations(input_df, validations)
            updated[ok_output] = ok_df
            updated[ko_output] = ko_df
        elif t_type == "compute_stats":
            fields = params.get("fields", None)
            include_validation_stats = params.get("include_validation_stats", False)
            output_path = params.get("output_path", None)
            stats_name = params.get("name", t["name"])

            stats = compute_field_stats(input_df, fields)

            if include_validation_stats:
                ok_df = updated.get(params.get("ok_input"), None)
                ko_df = updated.get(params.get("ko_input"), None)
                if ok_df is not None and ko_df is not None:
                    stats["validation_stats"] = compute_validation_stats(ok_df, ko_df)
                else:
                    logger.warning(
                        "Validation stats requested but OK/KO DataFrames not found"
                    )

            stats["generated_at"] = datetime.now().isoformat()
            stats["stats_name"] = stats_name

            stats_json = format_stats_json(stats)

            if output_path:
                output_dir = Path(output_path)
                output_dir.mkdir(parents=True, exist_ok=True)
                output_file = generate_stats_filename(stats_name, str(output_dir))
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(stats_json)
                logger.info(f"Statistics written to: {output_file}")

            updated[t["name"]] = input_df
        else:
            raise ValueError(f"Unsupported transformation type: {t_type!r}")

    return updated
