
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from fastapi.testclient import TestClient

import main


def test_concurrent_duplicate_requests(
        tmp_path,
        monkeypatch,
):
    db_path = tmp_path / "concurrency.db"

    monkeypatch.setattr(main, "DB", str(db_path))
    main.init_db()

    main.buckets.clear()

    workers = 5
    barrier = Barrier(workers)

    payload = {
        "idempotency_key": "concurrent-order-001",
        "amount": 89900,
        "currency": "EUR",
        "merchant": "ConcurrentTestMerchant",
    }

    def send_payment(_):
        # Make all workers ready before sending.
        barrier.wait(timeout=10)

        with TestClient(main.app) as client:
            response = client.post(
                "/payments",
                json=payload,
            )

        return response

    with ThreadPoolExecutor(
            max_workers=workers
    ) as executor:
        responses = list(
            executor.map(
                send_payment,
                range(workers),
            )
        )

    # Every request should receive a valid response.
    assert all(
        response.status_code == 200
        for response in responses
    )

    data = [
        response.json()
        for response in responses
    ]

    # Every request refers to the same payment.
    payment_ids = {
        item["payment_id"]
        for item in data
    }

    assert len(payment_ids) == 1

    # Exactly one request creates the payment.
    assert sum(
        item["replayed"] is False
        for item in data
    ) == 1

    assert sum(
        item["replayed"] is True
        for item in data
    ) == workers - 1

    # Verify the committed database state.
    conn = sqlite3.connect(str(db_path))

    try:
        payment_count = conn.execute(
            "SELECT COUNT(*) FROM payments"
        ).fetchone()[0]

        audit_count = conn.execute(
            "SELECT COUNT(*) FROM audit_log"
        ).fetchone()[0]

    finally:
        conn.close()

    assert payment_count == 1
    assert audit_count == 1
