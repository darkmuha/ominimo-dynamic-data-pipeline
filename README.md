# Ominimo - DE Technical Test

A PySpark-based framework for dynamically processing, validating, and storing motor insurance policy data using metadata-driven pipelines.

## Requirements [This was for me to keep track]

-   Ensure solution is reproducible and version-controlled using Git and Docker - Done
-   Read motor insurance policy data sources defined via metadata - Done
-   Apply field-level validations (e.g., non-empty vehicle registration number/plate_number, valid driver age, regex patterns) - Done
-   Add ingestion metadata (timestamp) to all records - Done
-   Write validated policy records to target storage (STANDARD_OK) - Done
-   Log or store rejected records with validation errors (STANDARD_KO) - Done
-   **Generate code or workflows dynamically from metadata** - must not be ad-hoc - Done
-   **Must be fully metadata-driven** - no hardcoded logic, designed to work dynamically from metadata - Done
-   Global statistics and validation metrics - Done
-   Optional: Airflow orchestration - Done

## Setup

**Prerequisites:**

-   Docker Desktop installed and running on your machine
-   Docker Compose (usually included with Docker Desktop)

**IMPORTANT: Before running the pipeline, create your .env file:**

```bash
cp .env.example .env
```

The `.env.example` file contains all required environment variables with default values ready to use.

**Project Directory (AIRFLOW_PROJ_DIR):** The project directory is automatically detected from your current working directory. Make sure to run `docker-compose` commands from the project root directory.

-   **Linux/Mac**: The `$PWD` environment variable is automatically available - no action needed
-   **Windows**: Docker Desktop automatically handles path conversion - no action needed

Simply ensure you're in the project root directory when running `docker-compose` commands, and the volumes will be mounted correctly.

1. **Build the Docker image:**

    ```bash
    docker-compose build
    ```

2. **Run the pipeline:**

    ```bash
    docker-compose up
    ```

3. **Run with custom arguments:**

    ```bash
    docker-compose run --rm motor-ingestion python main.py --input-path Data/motor_policies.json
    ```

    Or with a specific dataflow:

    ```bash
    docker-compose run --rm motor-ingestion python main.py --dataflow-name motor-ingestion
    docker-compose run --rm motor-ingestion python main.py --dataflow-name motor-ingestion-csv --input-path Data/motor_policies.csv
    ```

**Command-line Arguments:**

-   `--input-path`: Path to input file(s). Defaults to `Data/*.json` if not provided
-   `--dataflow-name`: Name of the dataflow to run from metadata. Uses the first dataflow if not provided

## Input Data

**JSON Format:** Input data should be in JSON Lines format (one JSON object per line) placed in the `Data/` directory. Example:

```json
{"policy_number":"P-20001","driver_age":36,"plate_number":"ABC-111","policy_start_date":"2024-01-01","policy_end_date":"2025-01-01"}
{"policy_number":"P-20002","driver":{"age":45},"vehicle":{"plate":"XYZ-222"},"policy":{"start":"2024-03-01","end":"2025-03-01"}}
```

**CSV Format:** Input data should be CSV with header row. Example:

```csv
policy_number,driver_age,plate_number,policy_start_date,policy_end_date
P-20001,36,ABC-111,2024-01-01,2025-01-01
P-20002,45,XYZ-222,2024-03-01,2025-03-01
```

## Output

-   **STANDARD_OK**: Valid records written to `Data/output/events/motor_policy/` (JSON format) or `Data/output/events/motor_policy_csv/` (CSV format)
-   **STANDARD_KO**: Rejected records with `validation_errors` array (JSON) or comma-separated string (CSV) written to `Data/output/discards/motor_policy/` (JSON) or `Data/output/discards/motor_policy_csv/` (CSV)
-   **Global Statistics**: Field-level statistics and validation metrics written to `Data/output/stats/` as JSON files (e.g., `global_stats.json`, `global_stats_csv.json`)
-   **Execution Logs**: Pipeline execution logs written to `Data/output/logs/` as timestamped log files (e.g., `pipeline_20260115_183000.log`)

## Metadata-Driven Design

The pipeline is fully configured via `metadata_motor.json`. Transformations, validations, and outputs are defined in metadata without hardcoding business logic. Supported validations include: `notNull`, `notEmpty`, `isNumeric`, `isInteger`, `min`, `max`, `range`, `isDate`, `dateBefore`, `dateAfter`, `pattern` (regex).

**Available Dataflows:**

-   `motor-ingestion`: Processes JSON input with field normalization for nested structures (PRIMARY - fully tested)
-   `motor-ingestion-csv`: Processes CSV input with simpler transformation pipeline (WORKING - all validations functional)

## Airflow Orchestration

This project includes Apache Airflow for orchestrating the pipeline. Airflow provides scheduling, monitoring, and retry capabilities.

### Setup Airflow

1. **Initialize Airflow (first time only):**

    ```bash
    docker-compose -f docker-compose-airflow.yaml up airflow-init
    ```

2. **Start Airflow services:**

    ```bash
    docker-compose -f docker-compose-airflow.yaml up -d
    ```

3. **Access Airflow UI:**

    - Open your browser and go to: `http://localhost:8080`
    - Default credentials: `airflow` / `airflow`

4. **Build your pipeline image (required for Airflow):**
    ```bash
    docker-compose build
    ```

### Using Airflow

-   The DAG `motor_ingestion_pipeline` is automatically loaded from the `dags/` directory
-   The DAG runs daily by default (configurable in `dags/motor_pipeline_dag.py`)
-   You can trigger runs manually from the Airflow UI
-   View logs, retry failed tasks, and monitor pipeline execution from the UI

### Stop Airflow

```bash
docker-compose -f docker-compose-airflow.yaml down
```

To also remove volumes (clean reset):

```bash
docker-compose -f docker-compose-airflow.yaml down -v
```
