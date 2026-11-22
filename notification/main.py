"""Notification service: a pure subscriber. Checkout never calls it, so if it is
slow or crashes, events wait in its queue and orders still complete."""
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from common import db as dbmod
from common.logs import log
from common.mq import Broker

conn = dbmod.connect()
conn.execute(
    "CREATE TABLE IF NOT EXISTS notifications ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT, email TEXT, subject TEXT)"
)
broker = Broker(conn)

SUBJECTS = {
    "order.placed": "We received your order",
    "order.confirmed": "Your order is confirmed",
    "order.cancelled": "Your order was cancelled",
}


async def handle_event(routing_key, body):
    subject = SUBJECTS[routing_key]
    # Real email sending would go here. We store a record and log it instead.
    conn.execute("INSERT INTO notifications (order_id, email, subject) VALUES (?,?,?)",
                 (body["order_id"], body.get("email", ""), subject))
    log("notification_sent", order_id=body["order_id"], subject=subject)


@asynccontextmanager
async def lifespan(app):
    await broker.start()
    await broker.subscribe("notification.events", list(SUBJECTS), handle_event)
    yield
    await broker.stop()


app = FastAPI(title="Notification service", lifespan=lifespan)


@app.get("/health")
def health():
    if broker.connection is None or broker.connection.is_closed:
        raise HTTPException(503, "broker not connected")
    return {"status": "ok"}


@app.get("/notifications")
def notifications():
    return [dict(r) for r in conn.execute("SELECT * FROM notifications")]
