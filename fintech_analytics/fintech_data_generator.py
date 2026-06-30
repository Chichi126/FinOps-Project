import csv
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from faker import Faker
from airflow.providers.postgres.hooks.postgres import PostgresHook

# --- Config & Constants ---
FAILURE_RATE = 0.001
BATCH_SIZE   = 500

CURRENCIES        = ["NGN", "USD", "GBP", "EUR", "KES", "GHS"]
CHANNELS          = ["mobile_app", "web", "ussd", "api", "pos"]
TRANSACTION_TYPES = ["transfer", "payment", "withdrawal", "deposit", "refund"]
STATUSES          = ["success", "failed", "reversed"]

fake = Faker()
Faker.seed(42)
random.seed(42)

DDL = """
CREATE TABLE IF NOT EXISTS payments (
    payment_id        UUID PRIMARY KEY,
    user_id           UUID           NOT NULL,
    merchant_id       UUID           NOT NULL,
    transaction_type  VARCHAR(20)    NOT NULL,
    channel           VARCHAR(20)    NOT NULL,
    currency          VARCHAR(3)     NOT NULL,
    amount            NUMERIC(18, 2) NOT NULL,
    status            VARCHAR(20)    NOT NULL,
    initiated_at      TIMESTAMPTZ    NOT NULL,
    completed_at      TIMESTAMPTZ,
    failure_mode      VARCHAR(50)
);
CREATE INDEX IF NOT EXISTS idx_payments_user      ON payments(user_id);
CREATE INDEX IF NOT EXISTS idx_payments_status    ON payments(status);
CREATE INDEX IF NOT EXISTS idx_payments_initiated ON payments(initiated_at);
"""

LEDGER_FIELDS = [
    "entry_id", "payment_id", "user_id", "entry_type",
    "currency", "amount", "posted_at", "failure_mode",
]

# --- Helper Functions ---
def random_amount(min_val=10, max_val=500_000):
    amt = random.uniform(min_val, max_val)
    return Decimal(str(amt)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def random_timestamp(days_back=90):
    now   = datetime.now(timezone.utc)
    delta = timedelta(seconds=random.randint(0, days_back * 24 * 3600))
    return now - delta

def inject_failure(payment: dict, ledger: dict | None) -> tuple[dict, dict | None]:
    if random.random() >= FAILURE_RATE:
        return payment, ledger
    mode = random.choice(["missing_ledger", "amount_mismatch", "timeout"])
    if mode == "missing_ledger":
        payment["failure_mode"] = "missing_ledger"
        return payment, None
    if mode == "amount_mismatch":
        delta     = random.uniform(-500, 500)
        corrupted = float(ledger["amount"]) + delta
        ledger["amount"]        = Decimal(str(corrupted)).quantize(Decimal("0.01"))
        ledger["failure_mode"]  = "amount_mismatch"
        payment["failure_mode"] = "amount_mismatch"
        return payment, ledger
    if mode == "timeout":
        payment["status"]       = "processing"
        payment["completed_at"] = None
        payment["initiated_at"] = datetime.now(timezone.utc) - timedelta(hours=random.randint(6, 72))
        payment["failure_mode"] = "timeout"
        return payment, ledger
    return payment, ledger

def build_row(user_pool, merchant_pool):
    payment_id   = str(uuid.uuid4())
    user_id      = str(random.choice(user_pool))
    merchant_id  = str(random.choice(merchant_pool))
    txn_type     = random.choice(TRANSACTION_TYPES)
    channel      = random.choice(CHANNELS)
    currency     = random.choice(CURRENCIES)
    amount       = random_amount()
    status       = random.choice(STATUSES)
    initiated_at = random_timestamp()
    completed_at = (initiated_at + timedelta(seconds=random.randint(1, 30))) if status != "failed" else None

    payment = {
        "payment_id": payment_id, "user_id": user_id, "merchant_id": merchant_id,
        "transaction_type": txn_type, "channel": channel, "currency": currency,
        "amount": amount, "status": status, "initiated_at": initiated_at,
        "completed_at": completed_at, "failure_mode": None,
    }
    ledger = {
        "entry_id": str(uuid.uuid4()), "payment_id": payment_id, "user_id": user_id,
        "entry_type": "debit" if txn_type in ("transfer", "payment", "withdrawal") else "credit",
        "currency": currency, "amount": amount, "posted_at": completed_at or initiated_at,
        "failure_mode": None,
    }
    return inject_failure(payment, ledger)

def write_payments_to_postgres(payments: list[dict], pg_hook: PostgresHook):
    """Uses Airflow's PostgresHook to insert rows safely."""
    conn = pg_hook.get_conn()
    cur  = conn.cursor()
    cur.execute(DDL)
    conn.commit()

    rows = [
        (
            r["payment_id"], r["user_id"], r["merchant_id"],
            r["transaction_type"], r["channel"], r["currency"],
            r["amount"], r["status"],
            r["initiated_at"], r["completed_at"], r["failure_mode"],
        )
        for r in payments
    ]
    cur.executemany(
        """
        INSERT INTO payments
            (payment_id, user_id, merchant_id, transaction_type, channel,
             currency, amount, status, initiated_at, completed_at, failure_mode)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (payment_id) DO NOTHING
        """,
        rows,
    )
    conn.commit()
    cur.close()
    conn.close()

def write_ledger_to_csv(ledger_entries: list[dict], output_dir: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(output_dir, f"ledger_entries_{ts}.csv")
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=LEDGER_FIELDS)
        writer.writeheader()
        writer.writerows(ledger_entries)
    return path

# --- The Callable Entrypoint for Airflow ---
def generate_fintech_data(n_rows: int, output_dir: str, postgres_conn_id: str = "postgres_default"):
    """
    Main function invoked by Airflow's PythonOperator.
    """
    print(f"🏦 Generating {n_rows:,} transactions.")
    
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    user_pool     = [uuid.uuid4() for _ in range(max(100, n_rows // 20))]
    merchant_pool = [uuid.uuid4() for _ in range(max(20,  n_rows // 100))]

    all_payments = []
    all_ledger   = []

    for i in range(n_rows):
        payment, ledger = build_row(user_pool, merchant_pool)
        all_payments.append(payment)
        if ledger is not None:
            all_ledger.append(ledger)

    # Write Data
    print(f"Writing {len(all_payments):,} payments to Postgres via Hook...")
    for start in range(0, len(all_payments), BATCH_SIZE):
        write_payments_to_postgres(all_payments[start : start + BATCH_SIZE], pg_hook)

    print(f"Writing {len(all_ledger):,} ledger entries to CSV...")
    csv_path = write_ledger_to_csv(all_ledger, output_dir)
    print(f"Generation successful. CSV placed at: {csv_path}")
    
    return csv_path