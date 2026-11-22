"""Inventory service: reserves stock (saga step) and releases it (compensation)."""
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from common import db as dbmod
from common.logs import log
from common.mq import Broker

conn = dbmod.connect()
conn.executescript(
    """
    CREATE TABLE IF NOT EXISTS stock (sku TEXT PRIMARY KEY, qty INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS reservations (
        order_id TEXT PRIMARY KEY, items TEXT NOT NULL, status TEXT NOT NULL);
    """
)
for sku, qty in (("LAPTOP-15", 5), ("KETTLE-1", 20), ("HEADSET-2", 2)):
    conn.execute("INSERT OR IGNORE INTO stock VALUES (?, ?)", (sku, qty))
broker = Broker(conn)


def reserve(order_id, items):
    """All-or-nothing reservation inside ONE local transaction."""
    existing = conn.execute("SELECT status FROM reservations WHERE order_id = ?",
                            (order_id,)).fetchone()
    if existing:  # replay: answer again, change nothing
        return existing["status"] == "RESERVED"
    conn.execute("BEGIN")
    try:
        for it in items:
            row = conn.execute("SELECT qty FROM stock WHERE sku = ?",
                               (it["sku"],)).fetchone()
            if row is None or row["qty"] < it["qty"]:
                conn.execute("ROLLBACK")
                conn.execute("INSERT INTO reservations VALUES (?, ?, 'REJECTED')",
                             (order_id, json.dumps(items)))
                return False
        for it in items:
            conn.execute("UPDATE stock SET qty = qty - ? WHERE sku = ?",
                         (it["qty"], it["sku"]))
        conn.execute("INSERT INTO reservations VALUES (?, ?, 'RESERVED')",
                     (order_id, json.dumps(items)))
        conn.execute("COMMIT")
        return True
    except Exception:
        conn.execute("ROLLBACK")
        raise


def release(order_id):
    row = conn.execute("SELECT * FROM reservations WHERE order_id = ?",
                       (order_id,)).fetchone()
    if row is None or row["status"] != "RESERVED":
        return False  # nothing to give back, or already released
    conn.execute("BEGIN")
    for it in json.loads(row["items"]):
        conn.execute("UPDATE stock SET qty = qty + ? WHERE sku = ?",
                     (it["qty"], it["sku"]))
    conn.execute("UPDATE reservations SET status = 'RELEASED' WHERE order_id = ?",
                 (order_id,))
    conn.execute("COMMIT")
    return True


async def handle_event(routing_key, body):
    order_id = body["order_id"]
    if routing_key == "stock.reserve":
        ok = reserve(order_id, body["items"])
        log("stock_reserve", order_id=order_id, reserved=ok)
        await broker.publish("stock.reserved" if ok else "stock.rejected",
                             {"order_id": order_id},
                             message_id=f"{order_id}:stock-result")
    elif routing_key == "stock.release":
        released = release(order_id)
        log("stock_release", order_id=order_id, released=released)


@asynccontextmanager
async def lifespan(app):
    await broker.start()
    await broker.subscribe("inventory.commands",
                           ["stock.reserve", "stock.release"], handle_event)
    yield
    await broker.stop()


app = FastAPI(title="Inventory service", lifespan=lifespan)


@app.get("/health")
def health():
    if broker.connection is None or broker.connection.is_closed:
        raise HTTPException(503, "broker not connected")
    return {"status": "ok"}


@app.get("/stock")
def stock():
    return {r["sku"]: r["qty"] for r in conn.execute("SELECT * FROM stock")}
