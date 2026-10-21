import sqlite3

from fastapi.testclient import TestClient

from main import app, init_db


def test_same_key_with_different_amount(tmp_path, monkeypatch):
    db_path = tmp_path / "paylite.db"
    monkeypatch.setattr("main.DB", str(db_path))
    init_db()

    client = TestClient(app)

    first = {
        "idempotency_key": "order-001",
        "amount": 1000,
        "currency": "EUR",
        "merchant": "Coolblue",
    }
    second = {
        "idempotency_key": "order-001",
        "amount": 999999,
        "currency": "EUR",
        "merchant": "Coolblue",
    }

    first_res = client.post("/payments", json=first)
    assert first_res.status_code == 200
    assert first_res.json()["replayed"] is False

    second_res = client.post("/payments", json=second)
    assert second_res.status_code == 409
    assert second_res.json()["detail"] == "Idempotency key already used for a different request"

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(
        "SELECT id, idempotency_key, amount, currency, merchant FROM payments WHERE idempotency_key = ?",
        (first["idempotency_key"],),
    )
    rows = cur.fetchall()
    conn.close()

    assert len(rows) == 1
    assert rows[0][2] == first["amount"]
    assert rows[0][3] == first["currency"]
    assert rows[0][4] == first["merchant"]


def test_same_key_with_same_request_replays_original_payment(tmp_path, monkeypatch):

    db_path = tmp_path / "paylite.db"
    monkeypatch.setattr("main.DB", str(db_path))
    init_db()

    client = TestClient(app)

    first = {
        "idempotency_key": "order-003",
        "amount": 1000,
        "currency": "EUR",
        "merchant": "Coolblue",
    }
    second = {
        "idempotency_key": "order-003",
        "amount": 1000,
        "currency": "EUR",
        "merchant": "Coolblue",
    }

    first_res = client.post("/payments", json=first)
    assert first_res.status_code == 200
    assert first_res.json()["replayed"] is False

    second_res = client.post("/payments", json=second)
    assert second_res.status_code == 200
    assert second_res.json()["replayed"] is True

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(
        "SELECT id, idempotency_key, amount, currency, merchant FROM payments WHERE idempotency_key = ?",
        (first["idempotency_key"],),
    )
    rows = cur.fetchall()
    conn.close()

    assert first_res.status_code == 200
    assert first_res.json()["replayed"] is False

    assert second_res.status_code == 200
    assert second_res.json()["replayed"] is True
    assert second_res.json()["payment_id"] == first_res.json()["payment_id"]

    assert len(rows) == 1
    assert rows[0][0] == first_res.json()["payment_id"]
    assert rows[0][1] == first["idempotency_key"]
    assert rows[0][2] == first["amount"]
    assert rows[0][3] == first["currency"]
    assert rows[0][4] == first["merchant"]