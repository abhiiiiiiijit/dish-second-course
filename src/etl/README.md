#  Data Pipeline Setup & Execution

1.  Extracts **Daily Visits** and **GA Sessions** data using API calls
2.  Saves the extracted data locally
3.  Loads processed data into **Google BigQuery**

------------------------------------------------------------------------

## Prerequisites

Before running the pipeline, ensure you have:

1.  **Python ≥ 3.10**

2.  Install dependencies:

    ``` sh
    pip install -r requirements.txt
    ```
    or

    ``` sh
    poetry install
    ```

3.  Set up Google Cloud authentication:

    ``` sh
    gcloud auth application-default login
    ```

    or export a service account key:

    ``` sh
    export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
    ```

4.  Required BigQuery permissions:

    -   BigQuery Data Editor
    -   BigQuery Job User

------------------------------------------------------------------------

## Running the Pipeline
The script accepts optional CLI arguments:

| Argument | Description | Default |
| :--- | :--- | :--- |
| `--start-date` | Start date for extraction (inclusive) | `2016-08-01` |
| `--end-date` | End date (exclusive) | `2016-08-02` |
| `--limit` | Max records per API request | `50` |
| `--base-path` | Directory to store raw data files | `data/ga_sessions/` |
| `--project-id` | Google Cloud Project ID | `dish-second-course` |
| `--table-id` | BigQuery Table ID (project.dataset.table) | `dish-second-course.analytics.ga_sessions` |

### Example execution

``` sh
 python3 src/etl/data_pipeline.py --start-date 2016-08-01     --end-date 2016-08-03     --limit 50 
```

Run with defaults:

``` sh
python3 data_pipeline.py
```

------------------------------------------------------------------------

## Testing

``` sh
pytest -v
```

------------------------------------------------------------------------

## Output

-   Extracted raw data saved in: `data/ga_sessions/`
-   Processed data loaded into: BigQuery table from `--table-id`

------------------------------------------------------------------------
## Approach to Handling Duplicate Data (Upsert Logic)

Below approach ensures idempotency and data integrity by using a staging-first, key-based MERGE strategy.

1. Natural Key Identification:

    First, we define a "natural key" for each data source to establish a unique record identifier.

    Example (Daily Visits): visit_date

    Example (GA Sessions): A composite key, such as fullVisitorId + visitId + date

2. Staging and Source Deduplication:

    Data extracted from APIs is initially loaded into a temporary staging table in BigQuery. Before merging, we deduplicate this staged data to handle any duplicates from the source or API batches.

    Method: We use the QUALIFY clause with ROW_NUMBER() partitioned by the natural key.

    Logic: This selects only one instance of each unique record (e.g., ROW_NUMBER() OVER (PARTITION BY visit_date ORDER BY load_timestamp DESC) = 1), ensuring the source data is unique before the merge operation.

3. Upsert via MERGE Statement:

    We use BigQuery's MERGE statement to atomically update the final target table.

    WHEN MATCHED: If a record (based on the natural key) already exists, we update its fields. This can be a full overwrite or an additive update (e.g., target.total_visits = target.total_visits + source.total_visits).

    WHEN NOT MATCHED: If the record is new, we insert it.

    WHEN NOT MATCHED BY SOURCE: This clause can optionally be used to handle soft deletes or archive records present in the target but not in the new source batch.

----------------------------------------------------------------------------------------

## Data Quality Checks to Implement

We implement a multi-layered data quality strategy, integrating checks at various pipeline stages using tools like dbt tests or Great Expectations (implemented in Trivago core data piplines).

1. Ingestion & Schema Checks:

    These checks occur as data first enters the system.

    Schema Validation: Verifies that incoming JSON or flat files adhere to expected structures (e.g., channelGrouping exists, total_visits is a number).

    Type Matching: Ensures data types are correct (e.g., visit_date is a valid date string).

2. Transformation & Business Logic Checks:

    These checks validate the data's integrity and business sense during or after transformation.

    Completeness: Checks for NULL values in critical columns, especially natural keys (visit_date should never be null).

    Uniqueness: Confirms that primary keys are unique within the transformed data (e.g., dbt test 'unique' on visit_date).

    Range & Value Validation: Ensures values are within expected bounds (e.g., total_visits > 0, dates are within the expected API range).

    Referential Integrity: Confirms that foreign keys in one table match primary keys in another (e.g., dbt test 'relationship').

3. Post-Load & Production Checks

    These checks run against the final, production-ready tables.

    Freshness: Monitors data lag to ensure data is updated within the expected timeframe (e.g., lag < 24 hours).

    Aggregation Consistency: Cross-checks aggregations (e.g., the sum of hits per session should match daily totals).

    Duplicate Detection: Final row count and key-based duplicate checks post-upsert to confirm the MERGE logic was successful.

4. Alerting & Monitoring

    Alerts: Failures at any stage trigger automated alerts via Slack (implemented in Trivago core data piplines) or email.

    Quarantine: Critical failures may route bad records to a quarantine table for manual review, preventing pipeline stoppage.

    Monitoring: DQ metrics and pipeline health are monitored in dashboards (e.g.,Looker Studio (which I have implemented in Trivago core data pipelines)) for compliance and operational awareness.