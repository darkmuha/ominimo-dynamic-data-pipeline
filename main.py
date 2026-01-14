import argparse
import glob
from pathlib import Path

from pyspark.sql import SparkSession

from src.metadata_loader import load_metadata
from src.reader import read_sources
from src.transformations import apply_transformations


def build_spark(app_name: str = "motor-ingestion-local") -> SparkSession:
    """
    Builds and returns a SparkSession instance.

    :param app_name: Application name for the Spark session
    :return: SparkSession instance configured for local execution
    """
    return SparkSession.builder.appName(app_name).master("local[*]").getOrCreate()


def _expand_path_pattern(path: str) -> str:
    """
    Expands glob patterns to actual file paths.

    :param path: File path that may contain glob patterns (e.g., "Data/*.json")
    :return: Comma-separated string of actual file paths, or original path if no glob pattern found
    """
    if "*" in path or "?" in path:
        expanded = glob.glob(path)
        if not expanded:
            raise FileNotFoundError(f"No files found matching pattern: {path}")
        return ",".join(expanded)
    return path


def _write_sinks(
    spark: SparkSession, dataflow_meta: dict, frames: dict, project_root: Path
) -> None:
    """
    Writes DataFrames to sinks defined in metadata.

    :param spark: SparkSession instance
    :param dataflow_meta: Dataflow metadata dictionary containing sink definitions
    :param frames: Dictionary mapping transformation names to DataFrames
    :param project_root: Project root path for resolving relative paths
    :return: None
    """
    for sink in dataflow_meta.get("sinks", []):
        input_name = sink["input"]
        df = frames[input_name]

        paths = sink.get("paths", [])
        fmt = sink.get("format", "json").lower()
        mode = sink.get("saveMode", "OVERWRITE").lower()

        for p in paths:
            path_obj = Path(p)
            if not path_obj.is_absolute():
                path_obj = project_root / path_obj

            path_obj.parent.mkdir(parents=True, exist_ok=True)
            df.write.mode(mode).format(fmt).save(str(path_obj))
            print(f"Successfully wrote {input_name} to {path_obj}")


def run_pipeline(input_path: str = None, dataflow_name: str = None) -> None:
    """
    Runs the motor insurance policy ingestion pipeline.

    :param input_path: Optional path to input JSON file(s). If None, defaults to Data/*.json
    :param dataflow_name: Optional name of the dataflow to run from metadata. If None, uses the first dataflow
    :return: None
    """
    project_root = Path(__file__).parent
    metadata_path = project_root / "metadata_motor.json"

    metadata = load_metadata(str(metadata_path))
    dataflows = metadata.get("dataflows", [])
    if not dataflows:
        raise ValueError("No dataflows defined in metadata.")

    if dataflow_name:
        dataflow = next(
            (df for df in dataflows if df.get("name") == dataflow_name), None
        )
        if not dataflow:
            raise ValueError(f"Dataflow '{dataflow_name}' not found in metadata.")
    else:
        dataflow = dataflows[0]

    if input_path is None:
        input_path = str(project_root / "Data" / "*.json")

    input_path = _expand_path_pattern(input_path)

    if dataflow.get("sources"):
        dataflow["sources"][0]["path"] = input_path

    spark = build_spark()

    try:
        frames = read_sources(spark, dataflow)
        frames = apply_transformations(dataflow, frames)

        ok_df = frames["validation_ok"]
        ko_df = frames["validation_ko"]

        print("=== STANDARD_OK (valid records) ===")
        ok_df.show(truncate=False)

        print("=== STANDARD_KO (rejected records with validation_errors) ===")
        ko_df.show(truncate=False)

        _write_sinks(spark, dataflow, frames, project_root)
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Motor insurance policy ingestion pipeline"
    )
    parser.add_argument(
        "--input-path",
        type=str,
        default=None,
        help="Path to input JSON file(s). If not provided, defaults to Data/*.json (all JSON files in Data folder)",
    )
    parser.add_argument(
        "--dataflow-name",
        type=str,
        default=None,
        help="Name of the dataflow to run from metadata. If not provided, uses the first dataflow.",
    )

    args = parser.parse_args()
    run_pipeline(input_path=args.input_path, dataflow_name=args.dataflow_name)
