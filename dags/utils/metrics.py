from prometheus_client import Counter, Histogram

# Number of files processed
FILES_PROCESSED = Counter(
    "sales_pipeline_files_processed_total",
    "Total number of processed files"
)

# Number of rows processed
ROWS_PROCESSED = Counter(
    "sales_pipeline_rows_processed_total",
    "Total number of processed rows"
)

# Validation failures
VALIDATION_FAILURES = Counter(
    "sales_pipeline_validation_failures_total",
    "Total validation failures"
)

# Pipeline stage duration
TASK_DURATION = Histogram(
    "sales_pipeline_task_duration_seconds",
    "Execution time of pipeline tasks",
    ["task_name"]
)