from datetime import datetime
from pathlib import Path
from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, current_timestamp
from pyspark.sql.types import StructType

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


def _column_exists(df: DataFrame, column_path: str) -> bool:
    """
    Checks if a column path exists in the DataFrame schema.

    :param df: Input DataFrame
    :param column_path: Column path (e.g., 'driver.age' or 'driver_age')
    :return: True if the column exists, False otherwise
    """
    schema = df.schema
    field_names = {field.name for field in schema.fields}

    if "." not in column_path:
        return column_path in field_names

    parts = column_path.split(".")
    current_schema = schema

    for i, part in enumerate(parts):
        field = None
        for f in current_schema.fields:
            if f.name == part:
                field = f
                break

        if field is None:
            return False

        if i == len(parts) - 1:
            return True

        if not isinstance(field.dataType, StructType):
            return False

        current_schema = field.dataType

    return True


def _apply_naming_convention(field_path: str, convention: str) -> str:
    """
    Applies a naming convention to flatten a nested field path.

    :param field_path: Dotted path string (e.g., 'driver.age' or 'parent.child.grandchild')
    :param convention: Naming convention - 'snake_case', 'keep_dot', or 'camelCase'
    :return: Flattened field name according to the convention
    """
    if convention == "keep_dot":
        return field_path
    elif convention == "snake_case":
        return field_path.replace(".", "_")
    elif convention == "camelCase":
        parts = field_path.split(".")
        result = parts[0]
        for part in parts[1:]:
            result += part[0].upper() + part[1:] if part else ""
        return result
    else:
        raise ValueError(
            f"Unsupported naming convention: {convention!r}. "
            f"Supported values: 'snake_case', 'keep_dot', 'camelCase'"
        )


def _discover_nested_fields(df: DataFrame, prefix: str = "") -> List[Dict[str, str]]:
    """
    Recursively discovers nested struct fields in a DataFrame schema.

    :param df: Input DataFrame
    :param prefix: Current path prefix for nested fields (e.g., 'driver' for nested discovery)
    :return: List of dictionaries with 'field_path' (e.g., 'driver.age') and 'flattened_name' (e.g., 'driver_age')
    """
    nested_fields = []
    schema = df.schema

    def _traverse_struct(struct_type: StructType, current_prefix: str):
        """Helper function to recursively traverse struct types."""
        for field in struct_type.fields:
            field_name = field.name
            full_path = (
                f"{current_prefix}.{field_name}" if current_prefix else field_name
            )

            # Check if this field is a struct type (nested object)
            if isinstance(field.dataType, StructType):
                # Recursively traverse nested struct
                _traverse_struct(field.dataType, full_path)
            else:
                if current_prefix:
                    nested_fields.append(
                        {
                            "field_path": full_path,
                            "flattened_name": None,
                        }
                    )

    _traverse_struct(schema, prefix)

    return nested_fields


def normalize_fields(
    df: DataFrame, fields: List[Dict], naming_convention: str = "snake_case"
) -> DataFrame:
    """
    Normalizes heterogeneous JSON into a flatter, canonical schema by coalescing fields from multiple sources.
    Automatically discovers and maps nested fields that are not explicitly defined.

    :param df: Input DataFrame with potentially nested or inconsistent field names
    :param fields: List of field configuration dictionaries, each containing 'name' (target column name) and 'sources' (list of field paths to coalesce in priority order)
    :param naming_convention: Naming convention for auto-generated flattened fields - 'snake_case' (default), 'keep_dot', or 'camelCase'
    :return: DataFrame with normalized canonical columns
    """
    # Get set of explicitly defined field names
    explicitly_defined = {field_cfg["name"] for field_cfg in fields}

    # Discover nested fields in the DataFrame
    discovered_nested = _discover_nested_fields(df)

    # Create a mapping of flattened names to their nested paths
    nested_paths_by_flattened_name = {}
    for nested_field in discovered_nested:
        field_path = nested_field["field_path"]
        flattened_name = _apply_naming_convention(field_path, naming_convention)
        if flattened_name not in nested_paths_by_flattened_name:
            nested_paths_by_flattened_name[flattened_name] = []
        nested_paths_by_flattened_name[flattened_name].append(field_path)

    # Enhance existing field definitions with nested sources
    enhanced_fields = []
    for field_cfg in fields:
        field_name = field_cfg["name"]
        existing_sources = field_cfg.get("sources", []).copy()

        # Check if there are nested paths that match this field name
        if field_name in nested_paths_by_flattened_name:
            for nested_path in nested_paths_by_flattened_name[field_name]:
                # Add nested path if not already in sources
                if nested_path not in existing_sources:
                    existing_sources.append(nested_path)
                    logger.debug(
                        f"Auto-added nested source '{nested_path}' to existing field '{field_name}'"
                    )

        enhanced_fields.append({"name": field_name, "sources": existing_sources})

    # Generate auto-mappings for nested fields that aren't explicitly defined
    auto_generated_fields = []
    for nested_field in discovered_nested:
        field_path = nested_field["field_path"]
        flattened_name = _apply_naming_convention(field_path, naming_convention)

        # Only auto-generate if this field name is not already explicitly defined
        if flattened_name not in explicitly_defined:
            # Generate sources: try both nested path and flat variant
            sources = [field_path]
            # Also try the flat variant (e.g., if path is "driver.age", try "driver_age")
            flat_variant = field_path.replace(".", "_")
            if flat_variant != field_path and flat_variant not in sources:
                sources.append(flat_variant)

            auto_generated_fields.append({"name": flattened_name, "sources": sources})
            logger.debug(
                f"Auto-generated mapping: {flattened_name} <- {sources} "
                f"(from nested field {field_path})"
            )

    # Merge enhanced explicit fields with auto-generated fields
    all_fields = enhanced_fields + auto_generated_fields

    # Apply all field mappings
    result = df
    for field_cfg in all_fields:
        target = field_cfg["name"]
        sources = field_cfg.get("sources", [])
        if not sources:
            continue

        # Filter out sources that don't exist in the DataFrame
        existing_sources = [s for s in sources if _column_exists(df, s)]
        if not existing_sources:
            logger.warning(
                f"No valid sources found for field '{target}'. "
                f"Tried: {sources}. Skipping this field."
            )
            continue

        cols = [_col_from_path(p) for p in existing_sources]
        value_expr = coalesce(*cols) if len(cols) > 1 else cols[0]

        result = result.withColumn(target, value_expr)

    return result


def drop_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Drops a list of columns from the DataFrame. Only drops columns that actually exist.

    :param df: Input DataFrame
    :param columns: List of column names to drop
    :return: DataFrame with specified columns removed
    """
    if not columns:
        return df
    
    # Only drop columns that actually exist in the DataFrame
    existing_columns = set(df.columns)
    columns_to_drop = [col for col in columns if col in existing_columns]
    
    if not columns_to_drop:
        return df
    
    logger.debug(f"Dropping columns: {columns_to_drop}")
    return df.drop(*columns_to_drop)


def select_columns(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Selects only the specified columns from the DataFrame.

    :param df: Input DataFrame
    :param columns: List of column names to keep
    :return: DataFrame with only the specified columns
    """
    if not columns:
        return df
    
    # Only select columns that actually exist in the DataFrame
    existing_columns = set(df.columns)
    columns_to_select = [c for c in columns if c in existing_columns]
    
    if not columns_to_select:
        logger.warning(f"None of the specified columns exist: {columns}")
        return df
    
    logger.debug(f"Selecting columns: {columns_to_select}")
    return df.select(*columns_to_select)


def add_fields(df: DataFrame, fields: List[Dict]) -> DataFrame:
    """
    Adds or overrides simple metadata fields on the DataFrame.

    :param df: Input DataFrame
    :param fields: List of field configuration dictionaries, each containing 'name' (target column name) and 'function' (supported function name like "current_timestamp")
    :return: DataFrame with added metadata fields
    """
    result = df

    for field_cfg in fields:
        name = field_cfg["name"]
        func = field_cfg.get("function")

        if func == "current_timestamp":
            result = result.withColumn(name, current_timestamp())
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
            naming_convention = params.get("auto_flatten_naming", "snake_case")
            output_df = normalize_fields(input_df, fields, naming_convention)
            updated[output_name] = output_df
        elif t_type == "drop_columns":
            columns = params.get("columns", [])
            output_df = drop_columns(input_df, columns)
            updated[output_name] = output_df
        elif t_type == "select_columns":
            columns = params.get("columns", [])
            output_df = select_columns(input_df, columns)
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
