import sqlite3
import time
import threading
import random
from collections import deque
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


app = FastAPI()
DB = "paylite.db"


# ============ 建表 (Day 18 模板) ============
def init_db():
    conn = sqlite3.connect(DB)
    conn.execute("""CREATE TABLE IF NOT EXISTS payments (
        id INTEGER PRIMARY KEY,
        idempotency_key TEXT UNIQUE,
        amount INTEGER,
        currency TEXT,
        merchant TEXT,
        status TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY, ts TEXT, actor TEXT,
        payment_id INTEGER, action TEXT, old_status TEXT, new_status TEXT
    )""")
    conn.commit()
    conn.close()


init_db()


class PaymentRequest(BaseModel):
    idempotency_key: str
    amount: int          # 单位:分! 899.99欧 = 89999
    currency: str
    merchant: str


# ============ D4: 审计留痕 (只INSERT,永不改删) ============
def audit(actor, payment_id, action, old_status, new_status):
    conn = sqlite3.connect(DB)
    conn.execute("INSERT INTO audit_log (ts, actor, payment_id, action, old_status, new_status) VALUES (?, ?, ?, ?, ?, ?)",
                 (datetime.now().isoformat(), actor, payment_id, action, old_status, new_status))
    conn.commit()
    conn.close()


# ============ D2: 限流器 (固定窗口) ============
WINDOW = 60
LIMIT = 5
buckets = {}


def allow_request(merchant):
    now = time.time()
    if merchant not in buckets:
        buckets[merchant] = [now, 1]
        return True
    start, count = buckets[merchant]
    if now - start > WINDOW:
        buckets[merchant] = [now, 1]
        return True
    if count < LIMIT:
        buckets[merchant][1] += 1
        return True
    return False


# ============ D2: TTL 缓存 ============
cache = {}
TTL = 5


# ============ D3: 队列 + 后台工人 ============
task_queue = deque()


def process_with_bank(payment_id):
    """模拟银行扣款: 耗时2-4秒, 90%成功"""
    time.sleep(random.uniform(2, 4))
    return "COMPLETED" if random.random() < 0.9 else "FAILED"


def worker_loop():
    print("[WORKER] 后台工人上岗")
    while True:
        if len(task_queue) == 0:
            time.sleep(0.2)
            continue
        pid = task_queue.popleft()
        print(f"[WORKER] 开始处理 payment {pid} ...")
        final = process_with_bank(pid)
        conn = sqlite3.connect(DB)
        conn.execute("UPDATE payments SET status = ? WHERE id = ?", (final, pid))
        conn.commit()
        conn.close()
        audit("worker", pid, "PROCESS", "PENDING", final)   # ← 你推导出的那行!
        print(f"[WORKER] payment {pid} -> {final}")


threading.Thread(target=worker_loop, daemon=True).start()


def _is_same_payment_request(existing_row, payment_request):
    if existing_row is None:
        return False
    _, amount, currency, merchant, _ = existing_row
    return (amount, currency, merchant) == (payment_request.amount, payment_request.currency, payment_request.merchant)


# ============ D1: 同步支付 (幂等) ============
@app.post("/payments")
def create_payment(p: PaymentRequest):
    if not allow_request(p.merchant):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("SELECT id, amount, currency, merchant, status FROM payments WHERE idempotency_key = ?",
                (p.idempotency_key,))
    existing = cur.fetchone()
    if existing is not None:
        conn.close()
        if _is_same_payment_request(existing, p):
            return {"payment_id": existing[0], "status": existing[4], "replayed": True}
        raise HTTPException(status_code=409, detail="Idempotency key already used for a different request")
    cur.execute("INSERT INTO payments (idempotency_key, amount, currency, merchant, status) VALUES (?, ?, ?, ?, ?)",
                (p.idempotency_key, p.amount, p.currency, p.merchant, "COMPLETED"))
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    audit("api", new_id, "CREATE", None, "COMPLETED")
    return {"payment_id": new_id, "status": "COMPLETED", "replayed": False}


# ============ D3: 异步支付 (幂等 + 队列) ============
@app.post("/payments/async")
def create_payment_async(p: PaymentRequest):
    if not allow_request(p.merchant):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("SELECT id, amount, currency, merchant, status FROM payments WHERE idempotency_key = ?",
                (p.idempotency_key,))
    existing = cur.fetchone()
    if existing is not None:
        conn.close()
        if _is_same_payment_request(existing, p):
            return {"payment_id": existing[0], "status": existing[4], "replayed": True}
        raise HTTPException(status_code=409, detail="Idempotency key already used for a different request")
    cur.execute("INSERT INTO payments (idempotency_key, amount, currency, merchant, status) VALUES (?, ?, ?, ?, ?)",
                (p.idempotency_key, p.amount, p.currency, p.merchant, "PENDING"))
    conn.commit()
    new_id = cur.lastrowid
    conn.close()
    audit("api", new_id, "CREATE", None, "PENDING")
    task_queue.append(new_id)
    return {"payment_id": new_id, "status": "PENDING", "replayed": False}


# ============ D2: 查单 (带TTL缓存) ============
@app.get("/payments/{payment_id}")
def get_payment(payment_id: int):
    now = time.time()
    if payment_id in cache:
        data, expiry = cache[payment_id]
        if now < expiry:
            print(f"[CACHE HIT] payment {payment_id}")
            return data
    print(f"[CACHE MISS] payment {payment_id} -> querying DB")
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("SELECT id, amount, currency, merchant, status FROM payments WHERE id = ?",
                (payment_id,))
    row = cur.fetchone()
    conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail="Payment not found")
    result = {"id": row[0], "amount": row[1], "currency": row[2],
              "merchant": row[3], "status": row[4]}
    cache[payment_id] = (result, now + TTL)
    return result


# ============ D4: 审计查询 + 健康检查 ============
@app.get("/payments/{payment_id}/audit")
def get_audit(payment_id: int):
    conn = sqlite3.connect(DB)
    cur = conn.cursor()
    cur.execute("SELECT ts, actor, action, old_status, new_status FROM audit_log WHERE payment_id = ? ORDER BY id",
                (payment_id,))
    rows = cur.fetchall()
    conn.close()
    result = []
    for r in rows:
        result.append({"ts": r[0], "actor": r[1], "action": r[2], "old": r[3], "new": r[4]})
    return result


@app.get("/health")
def health():
    try:
        conn = sqlite3.connect(DB)
        conn.execute("SELECT 1")
        conn.close()
        db_ok = True
    except Exception:
        db_ok = False
    return {"status": "ok" if db_ok else "degraded",
            "database": db_ok,
            "queue_depth": len(task_queue)}
