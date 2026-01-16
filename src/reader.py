from typing import Dict

from pyspark.sql import DataFrame, SparkSession

from src.logger import get_logger

logger = get_logger()


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
            df = (
                reader.format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("sep", ",")
                .option("quote", '"')
                .option("escape", '"')
                .option("multiLine", "false")
                .option("encoding", "UTF-8")
                .load(path)
            )
        else:
            logger.error(f"Unsupported source format: {fmt!r} for source {name!r}")
            raise ValueError(f"Unsupported source format: {fmt!r} for source {name!r}")

        sources[name] = df

    return sources
