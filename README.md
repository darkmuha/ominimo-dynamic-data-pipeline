# Ominimo - DE Technical Test

A PySpark-based framework for dynamically processing, validating, and storing motor insurance policy data using metadata-driven pipelines.

## Requirements

-   Ensure solution is reproducible and version-controlled using Git and Docker
-   Read motor insurance policy data sources defined via metadata (will probably use Mockaroo or something similar - note to self)
-   Apply field-level validations (e.g., non-empty vehicle registration number/plate_number, valid driver age)
-   Add ingestion metadata (timestamp) to all records (I'm a fan of having `updated_at` and `created_at`, so will add those)
-   Write validated policy records to target storage (STANDARD_OK)
-   Log or store rejected records with validation errors (STANDARD_KO)
-   **Generate code or workflows dynamically from metadata** - must not be ad-hoc
-   **Must be fully metadata-driven** - no hardcoded logic, designed to work dynamically from metadata
-   Add other stats that I think would be interesting to track, including global ones as well
-   Optional (but will probably do): Airflow orchestration (to be added after core functionality since the other stuff is more important)
