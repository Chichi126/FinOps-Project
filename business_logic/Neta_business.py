from datetime import datetime, timezone
from decimal import Decimal
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

# --- Config Defaults ---
BATCH_SIZE = 5_000

CH_DDL = """
CREATE TABLE IF NOT EXISTS fintech_analytics.payments (
    payment_id        String,
    user_id           String,
    merchant_id       String,
    transaction_type  LowCardinality(String),
    channel           LowCardinality(String),
    currency          LowCardinality(String),
    amount            Decimal(18, 2),
    status            LowCardinality(String),
    initiated_at      DateTime64(3, 'UTC'),
    completed_at      Nullable(DateTime64(3, 'UTC')),
    failure_mode      Nullable(String),
    loaded_at         DateTime64(3, 'UTC')
)
ENGINE = MergeTree()
ORDER BY (initiated_at, payment_id)
PARTITION BY toYYYYMM(initiated_at)
SETTINGS index_granularity = 8192;
"""

def create_ch_table(ch_hook: ClickHouseHook):
    ch_hook.run(CH_DDL)
    print("  ✔ ClickHouse table ready")

def get_pg_count(pg_hook: PostgresHook) -> int:
    return pg_hook.get_first("SELECT COUNT(*) FROM payments")[0]

def get_ch_count(ch_hook: ClickHouseHook) -> int:
    result = ch_hook.run("SELECT COUNT(*) FROM fintech_analytics.payments")
    return result[0][0]

def fetch_batch_from_pg(pg_hook: PostgresHook, batch_size: int, offset: int) -> list:
    """Fetches a batch of rows from Postgres as dict-like objects."""
    sql = """
        SELECT
            payment_id::text,
            user_id::text,
            merchant_id::text,
            transaction_type,
            channel,
            currency,
            amount,
            status,
            initiated_at,
            completed_at,
            failure_mode
        FROM payments
        ORDER BY initiated_at
        LIMIT %s OFFSET %s
    """
    # Using PostgresHook's underlying connection to easily get a dictionary cursor
    conn = pg_hook.get_conn()
    with conn.cursor(name='ch_loader_cursor') as cur:  # Server-side cursor for efficiency
        cur.execute(sql, (batch_size, offset))
        # Get column names to build dictionaries manually or use fetchall
        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
    return rows

def insert_batch_to_ch(ch_hook: ClickHouseHook, rows: list):
    loaded_at = datetime.now(timezone.utc)
    ch_rows = [
        {
            "payment_id":       str(r["payment_id"]),
            "user_id":          str(r["user_id"]),
            "merchant_id":      str(r["merchant_id"]),
            "transaction_type": r["transaction_type"],
            "channel":          r["channel"],
            "currency":         r["currency"],
            "amount":           float(r["amount"]) if isinstance(r["amount"], Decimal) else r["amount"],
            "status":           r["status"],
            "initiated_at":     r["initiated_at"],
            "completed_at":     r["completed_at"],
            "failure_mode":     r["failure_mode"],
            "loaded_at":        loaded_at,
        }
        for r in rows
    ]
    
    # ClickHouseHook execution syntax handles dictionary lists beautifully
    ch_hook.run(
        """
        INSERT INTO fintech_analytics.payments (
            payment_id, user_id, merchant_id, transaction_type, channel,
            currency, amount, status, initiated_at, completed_at,
            failure_mode, loaded_at
        ) VALUES
        """,
        ch_rows
    )

# --- Main Entrypoint for Airflow PythonOperator ---
def run_pg_to_clickhouse_etl(
    postgres_conn_id: str = "postgres_default", 
    clickhouse_conn_id: str = "clickhouse_default",
    limit: int = None,
    offset: int = 0
):
    print("Initialize hooks...")
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    ch_hook = ClickHouseHook(clickhouse_conn_id=clickhouse_conn_id)

    # Initialize destination
    create_ch_table(ch_hook)

    pg_total = get_pg_count(pg_hook)
    ch_existing = get_ch_count(ch_hook)
    to_load = min(limit, pg_total - offset) if limit else pg_total - offset

    print(f"  Postgres rows  : {pg_total:,}")
    print(f"  ClickHouse now : {ch_existing:,}")
    print(f"  To load        : {to_load:,}\n")

    if to_load <= 0:
        print("  Nothing to load.")
        return

    total_loaded = 0
    current_offset = offset
    batch_num = 0

    while True:
        batch_num += 1
        rows = fetch_batch_from_pg(pg_hook, BATCH_SIZE, current_offset)

        if not rows:
            break

        insert_batch_to_ch(ch_hook, rows)
        total_loaded += len(rows)
        current_offset += len(rows)

        pct = total_loaded / to_load * 100
        print(f"  batch {batch_num:>3} | {total_loaded:>8,} / {to_load:,} rows ({pct:.1f}%)")

        if limit and total_loaded >= limit:
            break
        if len(rows) < BATCH_SIZE:
            break

    ch_final = get_ch_count(ch_hook)
    print(f"✅ ETL Run Completed. Loaded: {total_loaded:,}. ClickHouse Grand Total: {ch_final:,}")