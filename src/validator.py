from functools import reduce
from typing import Dict, List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    array,
    col,
    expr,
    lit,
    try_to_date,
    when,
)

from src.logger import get_logger

logger = get_logger()


def _build_check_condition(field: str, check: str, df: DataFrame = None):
    """
    Maps a validation name to a boolean Spark expression and an error label.

    :param field: Name of the field/column to validate
    :param check: Validation check name (e.g., "notEmpty", "min:18", "isDate")
    :param df: Optional DataFrame (currently unused, reserved for future use)
    :return: Tuple of (condition, label) where condition is a Spark boolean expression and label is an error message string
    """
    if check == "notEmpty":
        condition = col(field).isNotNull() & (col(field) != "")
        label = f"{field}:must_be_non_empty"
    elif check == "notNull":
        condition = col(field).isNotNull()
        label = f"{field}:must_not_be_null"
    elif check == "isNumeric":
        numeric_col = expr(f"try_cast({field} as double)")
        condition = col(field).isNotNull() & numeric_col.isNotNull()
        label = f"{field}:must_be_numeric"
    elif check == "isInteger":
        numeric_col = expr(f"try_cast({field} as double)")
        int_col = expr(f"try_cast(try_cast({field} as double) as int)")
        condition = (
            col(field).isNotNull() & numeric_col.isNotNull() & (numeric_col == int_col)
        )
        label = f"{field}:must_be_integer"
    elif check.startswith("min:"):
        min_val = float(check.split(":")[1])
        numeric_col = expr(f"try_cast({field} as double)")
        condition = col(field).isNull() | (
            numeric_col.isNotNull() & (numeric_col >= min_val)
        )
        label = f"{field}:must_be_at_least_{min_val}"
    elif check.startswith("max:"):
        max_val = float(check.split(":")[1])
        numeric_col = expr(f"try_cast({field} as double)")
        condition = col(field).isNull() | (
            numeric_col.isNotNull() & (numeric_col <= max_val)
        )
        label = f"{field}:must_be_at_most_{max_val}"
    elif check.startswith("range:"):
        parts = check.split(":")[1].split("-")
        min_val = float(parts[0])
        max_val = float(parts[1])
        numeric_col = expr(f"try_cast({field} as double)")
        condition = col(field).isNull() | (
            numeric_col.isNotNull()
            & (numeric_col >= min_val)
            & (numeric_col <= max_val)
        )
        label = f"{field}:must_be_between_{min_val}_and_{max_val}"
    elif check == "isDate":
        date_col = try_to_date(col(field), "yyyy-MM-dd")
        condition = col(field).isNull() | date_col.isNotNull()
        label = f"{field}:must_be_valid_date"
    elif check.startswith("dateBefore:"):
        other_field = check.split(":")[1]
        date_col = try_to_date(col(field), "yyyy-MM-dd")
        other_date_col = try_to_date(col(other_field), "yyyy-MM-dd")
        condition = (
            col(field).isNull()
            | col(other_field).isNull()
            | (
                date_col.isNotNull()
                & other_date_col.isNotNull()
                & (date_col <= other_date_col)
            )
        )
        label = f"{field}:must_be_before_{other_field}"
    elif check.startswith("dateAfter:"):
        other_field = check.split(":")[1]
        date_col = try_to_date(col(field), "yyyy-MM-dd")
        other_date_col = try_to_date(col(other_field), "yyyy-MM-dd")
        condition = (
            col(field).isNull()
            | col(other_field).isNull()
            | (
                date_col.isNotNull()
                & other_date_col.isNotNull()
                & (date_col >= other_date_col)
            )
        )
        label = f"{field}:must_be_after_{other_field}"
    elif check.startswith("pattern:"):
        pattern = check.split(":", 1)[1]
        condition = col(field).isNull() | col(field).rlike(pattern)
        label = f"{field}:must_match_pattern"
    else:
        condition = lit(True)
        label = f"{field}:unknown_validation_{check}"

    return condition, label


def apply_validations(df: DataFrame, rules: List[Dict]) -> Tuple[DataFrame, DataFrame]:
    """
    Applies field-level validations dynamically from metadata.

    :param df: Input DataFrame containing raw policy records
    :param rules: List of rule dictionaries, each containing 'field' (column name) and 'validations' (list of validation names like ["notEmpty", "notNull"])
    :return: Tuple of (ok_df, ko_df) where ok_df contains records that passed all validations and ko_df contains records that failed at least one validation with a validation_errors array column
    """
    if not rules:
        logger.warning("No validation rules provided")
        return df, df.limit(0)

    conditions = []
    error_specs = []

    for rule in rules:
        field = rule["field"]
        checks = rule.get("validations", []) or []

        for check in checks:
            condition, label = _build_check_condition(field, check, df)
            conditions.append(condition)
            error_specs.append((condition, label))

    if not conditions:
        return df, df.limit(0)

    is_valid_expr = reduce(lambda a, b: a & b, conditions)
    df_with_flag = df.withColumn("is_valid", is_valid_expr)

    error_cols = []
    for idx, (condition, label) in enumerate(error_specs):
        err_col_name = f"_validation_err_{idx}"
        df_with_flag = df_with_flag.withColumn(
            err_col_name,
            when(~condition, lit(label)).otherwise(lit(None)),
        )
        error_cols.append(err_col_name)

    if error_cols:
        errors_array = array(*[col(c) for c in error_cols])
        df_with_errors = (
            df_with_flag.withColumn(
                "validation_errors_raw",
                errors_array,
            )
            .withColumn(
                "validation_errors",
                expr("filter(validation_errors_raw, x -> x is not null)"),
            )
            .drop(*error_cols, "validation_errors_raw")
        )
    else:
        df_with_errors = df_with_flag.withColumn(
            "validation_errors", array().cast("array<string>")
        )

    ok_df = df_with_errors.filter(col("is_valid") == True).drop("is_valid", "validation_errors")
    ko_df = df_with_errors.filter(col("is_valid") == False).drop("is_valid")

    return ok_df, ko_df
