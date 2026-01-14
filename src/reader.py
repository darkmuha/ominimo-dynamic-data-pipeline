from typing import Dict

from pyspark.sql import DataFrame, SparkSession


def read_sources(spark: SparkSession, dataflow_meta: dict) -> Dict[str, DataFrame]:
    """
    Reads all sources defined in a single dataflow metadata block.

    :param spark: SparkSession instance
    :param dataflow_meta: Dataflow metadata dictionary containing source definitions with 'name', 'path' (supports glob patterns), and 'format' (json, csv)
    :return: Dictionary mapping source names to DataFrames
    """
    sources: Dict[str, DataFrame] = {}

    for src in dataflow_meta.get("sources", []):
        name = src["name"]
        path = src["path"]
        fmt = src.get("format", "json").lower()

        reader = spark.read

        if fmt == "json":
            df = reader.json(path)
        elif fmt == "csv":
            df = reader.option("header", True).csv(path)
        else:
            raise ValueError(f"Unsupported source format: {fmt!r} for source {name!r}")

        sources[name] = df

    return sources

