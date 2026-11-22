"""Payment service: charges through a (simulated) external card provider."""
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from common import db as dbmod
from common.logs import log
from common.mq import Broker

DECLINE_ABOVE = 2000.0  # the fake provider declines large amounts

conn = dbmod.connect()
conn.execute(
    "CREATE TABLE IF NOT EXISTS payments ("
    "order_id TEXT PRIMARY KEY, amount REAL NOT NULL, status TEXT NOT NULL)"
)
broker = Broker(conn)


def charge_provider(order_id, amount):
    """Stand-in for the external provider. A real call would send order_id as the
    provider's idempotency key so a retry can never charge the card twice."""
    return amount <= DECLINE_ABOVE


async def handle_event(routing_key, body):
    if routing_key != "payment.charge":
        return
    order_id = body["order_id"]
    row = conn.execute("SELECT status FROM payments WHERE order_id = ?",
                       (order_id,)).fetchone()
    if row is None:  # first time we see this order: charge once
        ok = charge_provider(order_id, body["amount"])
        status = "SUCCEEDED" if ok else "FAILED"
        conn.execute("INSERT INTO payments VALUES (?, ?, ?)",
                     (order_id, body["amount"], status))
    else:  # replay: repeat the stored outcome, never charge again
        status = row["status"]
    log("payment_result", order_id=order_id, status=status)
    await broker.publish(
        "payment.succeeded" if status == "SUCCEEDED" else "payment.failed",
        {"order_id": order_id},
        message_id=f"{order_id}:payment-result",
    )


@asynccontextmanager
async def lifespan(app):
    await broker.start()
    await broker.subscribe("payment.commands", ["payment.charge"], handle_event)
    yield
    await broker.stop()


app = FastAPI(title="Payment service", lifespan=lifespan)


@app.get("/health")
def health():
    if broker.connection is None or broker.connection.is_closed:
        raise HTTPException(503, "broker not connected")
    return {"status": "ok"}
