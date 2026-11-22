"""Order service: REST endpoint + saga orchestrator for the place-order flow.

Saga (orchestration):
  POST /orders -> PENDING -> stock.reserve -> payment.charge -> CONFIRMED
  Compensation: payment fails -> stock.release -> CANCELLED
"""
import json
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field

from common import db as dbmod
from common.logs import correlation_id_var, log
from common.mq import Broker

conn = dbmod.connect()
conn.executescript(
    """
    CREATE TABLE IF NOT EXISTS orders (
        id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL,
        email TEXT NOT NULL,
        items TEXT NOT NULL,
        amount REAL NOT NULL,
        status TEXT NOT NULL,
        reason TEXT
    );
    CREATE TABLE IF NOT EXISTS idempotency_keys (
        key TEXT PRIMARY KEY,
        order_id TEXT NOT NULL
    );
    """
)
broker = Broker(conn)


class Item(BaseModel):
    sku: str
    qty: int = Field(gt=0)


class NewOrder(BaseModel):
    customer_id: str
    email: str
    items: list[Item] = Field(min_length=1)
    amount: float = Field(gt=0)


def set_status(order_id, new_status, reason=None, only_from="PENDING"):
    """Move the order forward once. Replays of the same event change nothing."""
    cur = conn.execute(
        "UPDATE orders SET status = ?, reason = ? WHERE id = ? AND status = ?",
        (new_status, reason, order_id, only_from),
    )
    return cur.rowcount == 1


async def handle_event(routing_key, body):
    order_id = body["order_id"]
    order = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
    if order is None:
        return
    if routing_key == "stock.reserved":
        # Step 2: ask Payment to charge. Message ID is derived from the order,
        # so a repeated publish is recognised as a duplicate downstream.
        await broker.publish(
            "payment.charge",
            {"order_id": order_id, "amount": order["amount"]},
            message_id=f"{order_id}:charge",
        )
    elif routing_key == "stock.rejected":
        if set_status(order_id, "CANCELLED", "out_of_stock"):
            await broker.publish("order.cancelled", {
                "order_id": order_id, "email": order["email"], "reason": "out_of_stock"},
                message_id=f"{order_id}:cancelled")
    elif routing_key == "payment.succeeded":
        if set_status(order_id, "CONFIRMED"):
            await broker.publish("order.confirmed", {
                "order_id": order_id, "email": order["email"]},
                message_id=f"{order_id}:confirmed")
    elif routing_key == "payment.failed":
        # COMPENSATING ACTION: give the reserved stock back, then cancel.
        await broker.publish("stock.release", {"order_id": order_id},
                             message_id=f"{order_id}:release")
        if set_status(order_id, "CANCELLED", "payment_declined"):
            await broker.publish("order.cancelled", {
                "order_id": order_id, "email": order["email"],
                "reason": "payment_declined"},
                message_id=f"{order_id}:cancelled")


@asynccontextmanager
async def lifespan(app):
    await broker.start()
    await broker.subscribe(
        "order.saga",
        ["stock.reserved", "stock.rejected", "payment.succeeded", "payment.failed"],
        handle_event,
    )
    yield
    await broker.stop()


app = FastAPI(title="Order service", lifespan=lifespan)


@app.get("/health")
def health():
    conn.execute("SELECT 1")
    ready = broker.connection is not None and not broker.connection.is_closed
    if not ready:
        raise HTTPException(503, "broker not connected")
    return {"status": "ok"}


@app.post("/orders", status_code=201)
async def place_order(
    body: NewOrder,
    idempotency_key: str | None = Header(default=None),
    x_correlation_id: str | None = Header(default=None),
):
    # The gateway normally creates this ID; we create one if a client calls directly.
    correlation_id_var.set(x_correlation_id or str(uuid.uuid4()))

    if idempotency_key:  # a retried request returns the same order, not a new one
        existing = conn.execute(
            "SELECT order_id FROM idempotency_keys WHERE key = ?", (idempotency_key,)
        ).fetchone()
        if existing:
            log("duplicate_request", order_id=existing["order_id"])
            row = conn.execute("SELECT * FROM orders WHERE id = ?",
                               (existing["order_id"],)).fetchone()
            return {"order_id": row["id"], "status": row["status"],
                    "correlation_id": correlation_id_var.get()}

    order_id = str(uuid.uuid4())
    items = [i.model_dump() for i in body.items]
    conn.execute(
        "INSERT INTO orders (id, customer_id, email, items, amount, status) "
        "VALUES (?, ?, ?, ?, ?, 'PENDING')",
        (order_id, body.customer_id, body.email, json.dumps(items), body.amount),
    )
    if idempotency_key:
        conn.execute("INSERT INTO idempotency_keys VALUES (?, ?)",
                     (idempotency_key, order_id))
    log("order_created", order_id=order_id, amount=body.amount)

    # Announce the fact (Notification, Recommendation) and start the saga.
    await broker.publish("order.placed", {
        "order_id": order_id, "customer_id": body.customer_id,
        "email": body.email, "items": items, "amount": body.amount},
        message_id=f"{order_id}:placed")
    await broker.publish("stock.reserve", {"order_id": order_id, "items": items},
                         message_id=f"{order_id}:reserve")
    return {"order_id": order_id, "status": "PENDING",
            "correlation_id": correlation_id_var.get()}


@app.get("/orders/{order_id}")
def get_order(order_id: str):
    row = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "order not found")
    return {"order_id": row["id"], "status": row["status"], "reason": row["reason"],
            "amount": row["amount"], "items": json.loads(row["items"])}
