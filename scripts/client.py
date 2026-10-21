import requests


payload = {"idempotency_key": "order-abc-001",
           "amount": 89999, "currency": "EUR", "merchant": "Coolblue"}


# ① 第一次支付
r1 = requests.post("http://localhost:8000/payments", json=payload)
print("①", r1.json())


# ② 手抖重复: 同一个payload原样再发一次
r2 = requests.post("http://localhost:8000/payments", json=payload)
print("②", r2.json())


# ③ 新订单: 换key换金额
payload2 = {"idempotency_key": "order-xyz-002",
            "amount": 12500, "currency": "EUR", "merchant": "Mediamarkt"}
r3 = requests.post("http://localhost:8000/payments", json=payload2)
print("③", r3.json())
