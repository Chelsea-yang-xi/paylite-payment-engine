
import sqlite3
import time
import threading
import random

from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


# ==================================================
# 1. FastAPI Application
# ==================================================

app = FastAPI(
    title="Paylite Payment Engine",
    description="A payment processing engine for backend engineering practice.",
    version="0.6.0",
)

DB = "paylite.db"


# ==================================================
# 2. Database Connection & Initialization
# ==================================================

def get_connection():
    """
    Create a SQLite database connection.

    timeout allows a request to wait briefly when another
    transaction holds the database write lock.
    """
    return sqlite3.connect(DB, timeout=10.0)


def init_db():
    conn = get_connection()

    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS payments (
                id INTEGER PRIMARY KEY,
                idempotency_key TEXT UNIQUE,
                amount INTEGER,
                currency TEXT,
                merchant TEXT,
                status TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY,
                ts TEXT,
                actor TEXT,
                payment_id INTEGER,
                action TEXT,
                old_status TEXT,
                new_status TEXT
            )
        """)

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


init_db()


# ==================================================
# 3. Payment Request Model
# ==================================================

class PaymentRequest(BaseModel):
    idempotency_key: str
    amount: int
    currency: str
    merchant: str


# ==================================================
# 4. Audit Logging
# ==================================================

def insert_audit(
        conn,
        actor,
        payment_id,
        action,
        old_status,
        new_status,
):
    """
    Insert an audit record using the caller's connection.

    IMPORTANT:
    This function does not call commit().
    Payment changes and audit records can therefore
    belong to the same database transaction.
    """

    conn.execute(
        """
        INSERT INTO audit_log
        (
            ts,
            actor,
            payment_id,
            action,
            old_status,
            new_status
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            datetime.now().isoformat(),
            actor,
            payment_id,
            action,
            old_status,
            new_status,
        ),
    )


def audit(
        actor,
        payment_id,
        action,
        old_status,
        new_status,
):
    """
    Standalone audit helper.

    Prefer insert_audit(conn, ...) when audit logging
    must share a transaction with payment updates.
    """

    conn = get_connection()

    try:
        insert_audit(
            conn,
            actor,
            payment_id,
            action,
            old_status,
            new_status,
        )

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


# ==================================================
# 5. Rate Limiting
# ==================================================

WINDOW = 60
LIMIT = 5

buckets = {}
rate_lock = threading.Lock()


def allow_request(merchant):
    """
    Fixed-window rate limiter.

    Maximum:
        5 requests per merchant per 60 seconds.

    Uses a threading lock for safety within
    a single Python process.
    """

    now = time.time()

    with rate_lock:

        if merchant not in buckets:
            buckets[merchant] = [now, 1]
            return True

        start, count = buckets[merchant]

        if now - start >= WINDOW:
            buckets[merchant] = [now, 1]
            return True

        if count < LIMIT:
            buckets[merchant][1] += 1
            return True

        return False


# ==================================================
# 6. TTL Cache
# ==================================================

cache = {}
TTL = 5
cache_lock = threading.Lock()


def get_cached_payment(payment_id):
    now = time.monotonic()

    with cache_lock:
        entry = cache.get(payment_id)

        if entry is None:
            return None

        data, expiry = entry

        if now < expiry:
            return data

        cache.pop(payment_id, None)

    return None


def invalidate_payment_cache(payment_id):
    with cache_lock:
        cache.pop(payment_id, None)


# ==================================================
# 7. Simulated Bank Processing
# ==================================================

def process_with_bank(payment_id):
    """
    Simulates an external banking operation.

    Processing time:
        2-4 seconds.

    Result:
        90% COMPLETED
        10% FAILED

    No real payment or money transfer occurs.
    """

    time.sleep(random.uniform(2, 4))

    if random.random() < 0.9:
        return "COMPLETED"

    return "FAILED"


# ==================================================
# 8. Idempotency Helper
# ==================================================

def _is_same_payment_request(
        existing_row,
        payment_request,
):
    """
    Compare the stored payment with an incoming request.

    The idempotency key must not be reused for
    a different payment payload.
    """

    if existing_row is None:
        return False

    _, amount, currency, merchant, _ = existing_row

    return (
        amount,
        currency,
        merchant,
    ) == (
        payment_request.amount,
        payment_request.currency,
        payment_request.merchant,
    )


# ==================================================
# 9. Synchronous Payment API
# ==================================================

@app.post("/payments")
def create_payment(p: PaymentRequest):
    """
    Create a synchronous payment.

    Guarantees within this SQLite prototype:

    - Database uniqueness for idempotency keys
    - Serialized SQLite write transactions
    - Atomic payment creation and audit logging
    """

    if not allow_request(p.merchant):
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
        )

    conn = get_connection()

    try:
        conn.execute("BEGIN IMMEDIATE")

        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                id,
                amount,
                currency,
                merchant,
                status
            FROM payments
            WHERE idempotency_key = ?
            """,
            (p.idempotency_key,),
        )

        existing = cur.fetchone()

        if existing is not None:

            conn.rollback()

            if _is_same_payment_request(existing, p):
                return {
                    "payment_id": existing[0],
                    "status": existing[4],
                    "replayed": True,
                }

            raise HTTPException(
                status_code=409,
                detail=(
                    "Idempotency key already used "
                    "for a different request"
                ),
            )

        cur.execute(
            """
            INSERT INTO payments
            (
                idempotency_key,
                amount,
                currency,
                merchant,
                status
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                p.idempotency_key,
                p.amount,
                p.currency,
                p.merchant,
                "COMPLETED",
            ),
        )

        payment_id = cur.lastrowid

        insert_audit(
            conn,
            "api",
            payment_id,
            "CREATE",
            None,
            "COMPLETED",
        )

        conn.commit()

        return {
            "payment_id": payment_id,
            "status": "COMPLETED",
            "replayed": False,
        }

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


# ==================================================
# 10. Asynchronous Payment API
# ==================================================

@app.post("/payments/async")
def create_payment_async(p: PaymentRequest):
    """
    Create an asynchronous payment.

    The payment is stored as PENDING.

    The worker will later discover it directly
    from the SQLite database.

    No in-memory queue is required.
    """

    if not allow_request(p.merchant):
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
        )

    conn = get_connection()

    try:
        conn.execute("BEGIN IMMEDIATE")

        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                id,
                amount,
                currency,
                merchant,
                status
            FROM payments
            WHERE idempotency_key = ?
            """,
            (p.idempotency_key,),
        )

        existing = cur.fetchone()

        if existing is not None:

            conn.rollback()

            if _is_same_payment_request(existing, p):
                return {
                    "payment_id": existing[0],
                    "status": existing[4],
                    "replayed": True,
                }

            raise HTTPException(
                status_code=409,
                detail=(
                    "Idempotency key already used "
                    "for a different request"
                ),
            )

        cur.execute(
            """
            INSERT INTO payments
            (
                idempotency_key,
                amount,
                currency,
                merchant,
                status
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                p.idempotency_key,
                p.amount,
                p.currency,
                p.merchant,
                "PENDING",
            ),
        )

        payment_id = cur.lastrowid

        insert_audit(
            conn,
            "api",
            payment_id,
            "CREATE",
            None,
            "PENDING",
        )

        conn.commit()

        return {
            "payment_id": payment_id,
            "status": "PENDING",
            "replayed": False,
        }

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


# ==================================================
# 11. Durable Worker Processing
# ==================================================

def process_next_pending():
    """
    Process one pending payment.

    This implementation is intended for a
    single worker process only.

    Returns:
        True  - payment processed
        False - no eligible payment processed
    """

    conn = get_connection()

    try:
        row = conn.execute(
            """
            SELECT id
            FROM payments
            WHERE status = 'PENDING'
            ORDER BY id
            LIMIT 1
            """
        ).fetchone()

    finally:
        conn.close()

    if row is None:
        return False

    payment_id = row[0]

    # Simulated external bank processing.
    # An exception leaves the payment PENDING.
    final_status = process_with_bank(payment_id)

    if final_status not in (
            "COMPLETED",
            "FAILED",
    ):
        raise ValueError(
            "Unexpected bank result"
        )

    conn = get_connection()

    try:
        conn.execute("BEGIN IMMEDIATE")

        cur = conn.execute(
            """
            UPDATE payments
            SET status = ?
            WHERE id = ?
              AND status = 'PENDING'
            """,
            (
                final_status,
                payment_id,
            ),
        )

        if cur.rowcount != 1:
            conn.rollback()
            return False

        insert_audit(
            conn,
            "worker",
            payment_id,
            "PROCESS",
            "PENDING",
            final_status,
        )

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()

    invalidate_payment_cache(payment_id)

    print(
        f"[WORKER] Payment {payment_id} "
        f"-> {final_status}"
    )

    return True


# ==================================================
# 12. Payment Query API
# ==================================================

@app.get("/payments/{payment_id}")
def get_payment(payment_id: int):
    """
    Get payment details.

    Uses a simple TTL cache to reduce
    repeated database queries.
    """

    cached = get_cached_payment(payment_id)

    if cached is not None:
        print(
            f"[CACHE HIT] Payment {payment_id}"
        )
        return cached

    print(
        f"[CACHE MISS] Payment {payment_id}"
    )

    conn = get_connection()

    try:
        row = conn.execute(
            """
            SELECT
                id,
                amount,
                currency,
                merchant,
                status
            FROM payments
            WHERE id = ?
            """,
            (payment_id,),
        ).fetchone()

    finally:
        conn.close()

    if row is None:
        raise HTTPException(
            status_code=404,
            detail="Payment not found",
        )

    result = {
        "id": row[0],
        "amount": row[1],
        "currency": row[2],
        "merchant": row[3],
        "status": row[4],
    }

    with cache_lock:
        cache[payment_id] = (
            result,
            time.monotonic() + TTL,
        )

    return result


# ==================================================
# 13. Audit Query API
# ==================================================

@app.get("/payments/{payment_id}/audit")
def get_audit(payment_id: int):
    """
    Return the audit history for a payment.
    """

    conn = get_connection()

    try:
        rows = conn.execute(
            """
            SELECT
                ts,
                actor,
                action,
                old_status,
                new_status
            FROM audit_log
            WHERE payment_id = ?
            ORDER BY id
            """,
            (payment_id,),
        ).fetchall()

    finally:
        conn.close()

    return [
        {
            "ts": row[0],
            "actor": row[1],
            "action": row[2],
            "old": row[3],
            "new": row[4],
        }
        for row in rows
    ]


# ==================================================
# 14. Pending Payment Count
# ==================================================

def get_pending_count():
    """
    Count pending payments directly from SQLite.
    """

    conn = get_connection()

    try:
        count = conn.execute(
            """
            SELECT COUNT(*)
            FROM payments
            WHERE status = 'PENDING'
            """
        ).fetchone()[0]

        return count

    finally:
        conn.close()


# ==================================================
# 15. Health Check
# ==================================================

@app.get("/health")
def health():
    """
    Report API database health and pending work.
    """

    try:
        conn = get_connection()

        try:
            conn.execute("SELECT 1")

        finally:
            conn.close()

        db_ok = True

    except Exception:
        db_ok = False

    return {
        "status": (
            "ok"
            if db_ok
            else "degraded"
        ),
        "database": db_ok,
        "pending_payments": (
            get_pending_count()
            if db_ok
            else None
        ),
    }
