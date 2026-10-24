
import sqlite3

from fastapi.testclient import TestClient

import main


def test_pending_payment_survives_worker_restart(
        tmp_path, monkeypatch
):
    db_path = tmp_path / "recovery.db"

    monkeypatch.setattr(main, "DB", str(db_path))
    main.init_db()
    main.buckets.clear()

    client = TestClient(main.app)

    payload = {
        "idempotency_key": "recovery-001",
        "amount": 5000,
        "currency": "EUR",
        "merchant": "RecoveryTest",
    }

    # Step 1: create a pending payment
    response = client.post(
        "/payments/async",
        json=payload,
    )

    assert response.status_code == 200
    assert response.json()["status"] == "PENDING"

    payment_id = response.json()["payment_id"]

    # Step 2: simulate a worker failure
    def broken_bank(pid):
        raise RuntimeError("Bank temporarily unavailable")

    monkeypatch.setattr(
        main, "process_with_bank", broken_bank
    )

    try:
        main.process_next_pending()
        assert False, "Expected worker failure"
    except RuntimeError:
        pass

    # Step 3: payment remains pending
    conn = sqlite3.connect(str(db_path))

    status = conn.execute(
        "SELECT status FROM payments WHERE id = ?",
        (payment_id,),
    ).fetchone()[0]

    conn.close()

    assert status == "PENDING"

    # Step 4: simulate the bank recovering
    monkeypatch.setattr(
        main,
        "process_with_bank",
        lambda pid: "COMPLETED",
    )

    assert main.process_next_pending() is True

    # Step 5: verify committed records
    conn = sqlite3.connect(str(db_path))

    status = conn.execute(
        "SELECT status FROM payments WHERE id = ?",
        (payment_id,),
    ).fetchone()[0]

    actions = conn.execute(
        """
        SELECT action
        FROM audit_log
        WHERE payment_id = ?
        ORDER BY id
        """,
        (payment_id,),
    ).fetchall()

    conn.close()

    assert status == "COMPLETED"
    assert actions == [
        ("CREATE",),
        ("PROCESS",),
    ]

    # No more pending payment
    assert main.process_next_pending() is False


import pytest


def test_async_payment_rolls_back_on_audit_failure(
        tmp_path, monkeypatch
):
    db_path = tmp_path / "async_atomicity.db"

    monkeypatch.setattr(main, "DB", str(db_path))
    main.init_db()
    main.buckets.clear()

    def broken_audit(*args, **kwargs):
        raise RuntimeError("Audit database failure")

    monkeypatch.setattr(
        main, "insert_audit", broken_audit
    )

    client = TestClient(main.app)

    with pytest.raises(
            RuntimeError,
            match="Audit database failure",
    ):
        client.post(
            "/payments/async",
            json={
                "idempotency_key": "atomic-async-001",
                "amount": 1000,
                "currency": "EUR",
                "merchant": "AtomicityTest",
            },
        )

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
