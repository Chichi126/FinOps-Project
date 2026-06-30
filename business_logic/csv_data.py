import csv
from datetime import datetime, timezone
from decimal import Decimal
from airflow.errors import AirflowException
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

# --- Config Defaults ---
BATCH_SIZE = 5_000

CH_DDL = """
CREATE TABLE IF NOT EXISTS fintech_analytics.ledger_entries (
    entry_id      String,
    payment_id    String,
    user_id       String,
    entry_type    LowCardinality(String),
    currency      LowCardinality(String),
    amount        Decimal(18, 2),
    posted_at     DateTime64(3, 'UTC'),
    failure_mode  Nullable(String),
    loaded_at     DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
ORDER BY (posted_at, entry_id)
PARTITION BY toYYYYMM(posted_at)
SETTINGS index_granularity = 8192;
"""

def create_ch_table(ch_hook: ClickHouseHook):
    ch_hook.run(CH_DDL)
    print("  ✔ ClickHouse table ready: fintech_analytics.ledger_entries")

def get_ch_count(ch_hook: ClickHouseHook) -> int:
    result = ch_hook.run("SELECT COUNT(*) FROM fintech_analytics.ledger_entries")
    return result[0][0]

def parse_row(row: dict) -> dict:
    """Convert CSV string values to correct Python types for ClickHouse."""
    posted_at_raw = row["posted_at"]
    try:
        posted_at = datetime.fromisoformat(posted_at_raw)
    except ValueError:
        posted_at = datetime.strptime(posted_at_raw, "%Y-%m-%d %H:%M:%S")

    if posted_at.tzinfo is None:
        posted_at = posted_at.replace(tzinfo=timezone.utc)

    amount = float(Decimal(row["amount"]))
    failure_mode = row["failure_mode"] if row["failure_mode"] else None

    return {
        "entry_id":     row["entry_id"],
        "payment_id":   row["payment_id"],
        "user_id":      row["user_id"],
        "entry_type":   row["entry_type"],
        "currency":     row["currency"],
        "amount":       amount,
        "posted_at":    posted_at,
        "failure_mode":  failure_mode,
        "loaded_at":    datetime.now(timezone.utc),
    }

def read_csv_in_batches(filepath: str, batch_size: int):
    """Generator — yields one batch at a time."""
    with open(filepath, newline="") as f:
        reader = csv.DictReader(f)
        batch = []
        for row in reader:
            batch.append(parse_row(row))
            if len(batch) == batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

def insert_batch(ch_hook: ClickHouseHook, rows: list[dict]):
    ch_hook.run(
        """
        INSERT INTO fintech_analytics.ledger_entries (
            entry_id, payment_id, user_id, entry_type,
            currency, amount, posted_at, failure_mode, loaded_at
        ) VALUES
        """,
        rows,
    )

def count_csv_rows(filepath: str) -> int:
    with open(filepath, newline="") as f:
        return sum(1 for _ in csv.DictReader(f))

# --- Main Entrypoint for Airflow PythonOperator ---
def run_csv_to_clickhouse_etl(
    clickhouse_conn_id: str = "clickhouse_default",
    csv_file_path: str = None,
    **context
):
    """
    Main function invoked by Airflow. If csv_file_path is not explicitly given,
    it tries to pull it dynamically from the upstream data generator task via XCom.
    """
    # Fallback to XCom if path wasn't provided directly in op_kwargs
    if not csv_file_path:
        ti = context['task_instance']
        # This pulls the return value of 'generate_mock_data'
        csv_file_path = ti.xcom_pull(task_ids='generate_mock_data')
    
    if not csv_file_path:
        raise AirflowException("No CSV file path provided and couldn't find one via XCom.")

    print(f"Source CSV : {csv_file_path}")
    ch_hook = ClickHouseHook(clickhouse_conn_id=clickhouse_conn_id)
    
    create_ch_table(ch_hook)

    total_rows = count_csv_rows(csv_file_path)
    ch_existing = get_ch_count(ch_hook)

    print(f"  CSV rows       : {total_rows:,}")
    print(f"  ClickHouse now : {ch_existing:,}\n")

    total_loaded = 0
    batch_num = 0

    for batch in read_csv_in_batches(csv_file_path, BATCH_SIZE):
        batch_num += 1
        insert_batch(ch_hook, batch)
        total_loaded += len(batch)

        pct = total_loaded / total_rows * 100
        print(f"  batch {batch_num:>3} | {total_loaded:>8,} / {total_rows:,} rows ({pct:.1f}%)")

    ch_final = get_ch_count(ch_hook)
    print(f"✅ CSV ETL Completed. Loaded: {total_loaded:,}. ClickHouse Grand Total: {ch_final:,}")