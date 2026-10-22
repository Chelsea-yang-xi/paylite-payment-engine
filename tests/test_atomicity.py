
import sqlite3
import pytest

from fastapi.testclient import TestClient
from main import app, init_db, buckets


def test_payment_rolls_back_when_audit_fails(
        tmp_path, monkeypatch
):
    import main

    db_path = tmp_path / "atomicity.db"
    monkeypatch.setattr(main, "DB", str(db_path))
    init_db()

    # Avoid interference from the rate limiter
    buckets.clear()

    def broken_audit(*args, **kwargs):
        raise RuntimeError("Simulated audit failure")

    monkeypatch.setattr(
        main, "insert_audit", broken_audit
    )

    client = TestClient(app)

    payload = {
        "idempotency_key": "rollback-test-001",
        "amount": 5000,
        "currency": "EUR",
        "merchant": "RollbackTest"
    }

    with pytest.raises(
            RuntimeError,
            match="Simulated audit failure"
    ):
        client.post("/payments", json=payload)

    # Inspect database after the simulated failure
    conn = sqlite3.connect(str(db_path))

    payment_count = conn.execute(
        "SELECT COUNT(*) FROM payments"
    ).fetchone()[0]

    audit_count = conn.execute(
        "SELECT COUNT(*) FROM audit_log"
    ).fetchone()[0]

    conn.close()

    assert payment_count == 0
    assert audit_count == 0
