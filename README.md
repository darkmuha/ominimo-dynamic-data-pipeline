# Ominimo - DE Technical Test

A PySpark-based framework for dynamically processing, validating, and storing motor insurance policy data using metadata-driven pipelines.

## Requirements

-   Ensure solution is reproducible and version-controlled using Git and Docker - Done
-   Read motor insurance policy data sources defined via metadata - Done
-   Apply field-level validations (e.g., non-empty vehicle registration number/plate_number, valid driver age) - Done (will improve)
-   Add ingestion metadata (timestamp) to all records - Done
-   Write validated policy records to target storage (STANDARD_OK) - Done
-   Log or store rejected records with validation errors (STANDARD_KO) - Done
-   **Generate code or workflows dynamically from metadata** - must not be ad-hoc - Done
-   **Must be fully metadata-driven** - no hardcoded logic, designed to work dynamically from metadata - Done
-   Add other stats that I think would be interesting to track, including global ones as well - Pending
-   Optional: Airflow orchestration - Pending

## Setup

**Prerequisites:**

-   Docker Desktop installed and running on your machine
-   Docker Compose (usually included with Docker Desktop)

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
    ```

## Output

-   **STANDARD_OK**: Valid records written to `Data/output/events/motor_policy/`
-   **STANDARD_KO**: Rejected records with validation errors written to `Data/output/discards/motor_policy/`

## Metadata-Driven Design

The pipeline is fully configured via `metadata_motor.json`. Transformations, validations, and outputs are defined in metadata without hardcoding business logic. Supported validations include: `notNull`, `notEmpty`, `isNumeric`, `isInteger`, `min`, `max`, `range`, `isDate`, `dateBefore`, `dateAfter`.
