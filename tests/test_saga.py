"""Saga test (integration level, no Docker): runs Order, Inventory, Payment and
Notification together over an in-memory stand-in for RabbitMQ.
It checks the happy path, the compensating action, and duplicate delivery."""
import asyncio
import importlib
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest  # noqa: E402

from common import db as dbmod  # noqa: E402
from common import mq  # noqa: E402
from common.logs import correlation_id_var  # noqa: E402


class Bus:
    def __init__(self, duplicate=False):
        self.queue = []          # (routing_key, body, message_id, correlation_id)
        self.subs = []           # (routing_keys, handler, broker)
        self.duplicate = duplicate
        self.log = []

    async def drain(self):
        while self.queue:
            key, body, mid, cid = self.queue.pop(0)
            self.log.append(key)
            for keys, handler, broker in list(self.subs):
                if key in keys:
                    for _ in range(2 if self.duplicate else 1):
                        correlation_id_var.set(cid)
                        seen = broker.db.execute(
                            "SELECT 1 FROM processed_messages WHERE message_id=?",
                            (mid,)).fetchone()
                        if seen:
                            continue  # idempotent consumer skips the duplicate
                        await handler(key, body)
                        broker.db.execute(
                            "INSERT OR IGNORE INTO processed_messages VALUES (?)", (mid,))


BUS = None


class FakeBroker:
    def __init__(self, conn):
        self.db = conn
        self.connection = None

    async def start(self): pass
    async def stop(self): pass

    async def publish(self, routing_key, body, message_id=None):
        body = json.loads(json.dumps(body))
        BUS.queue.append((routing_key, body, message_id or str(id(body)),
                          correlation_id_var.get()))

    async def subscribe(self, queue_name, routing_keys, handler):
        BUS.subs.append((set(routing_keys), handler, self))


def load_services():
    """Import each service fresh, with its own database file and the fake broker."""
    mq.Broker = FakeBroker
    mods = {}
    for name in ("order", "inventory", "payment", "notification"):
        dbmod.DB_PATH = os.path.join(tempfile.mkdtemp(), f"{name}.db")
        sys.modules.pop(f"{name}.main", None)
        mods[name] = importlib.import_module(f"{name}.main")
    return mods


async def run_order(mods, amount, qty=1, sku="KETTLE-1"):
    async with mods["order"].lifespan(None), mods["inventory"].lifespan(None), \
            mods["payment"].lifespan(None), mods["notification"].lifespan(None):
        body = mods["order"].NewOrder(customer_id="c1", email="a@b.mt",
                                      items=[{"sku": sku, "qty": qty}], amount=amount)
        res = await mods["order"].place_order(body, idempotency_key=None,
                                              x_correlation_id="corr-123")
        await BUS.drain()
        return res["order_id"]


def status(mods, order_id):
    return mods["order"].get_order(order_id)["status"]


def kettles(mods):
    return mods["inventory"].conn.execute(
        "SELECT qty FROM stock WHERE sku='KETTLE-1'").fetchone()["qty"]


@pytest.mark.parametrize("duplicate", [False, True])
def test_happy_path_confirms_order(duplicate):
    global BUS
    BUS = Bus(duplicate=duplicate)
    mods = load_services()
    before = kettles(mods)
    oid = asyncio.run(run_order(mods, amount=100))
    assert status(mods, oid) == "CONFIRMED"
    assert kettles(mods) == before - 1  # stock reduced exactly once
    subjects = [r["subject"] for r in mods["notification"].conn.execute(
        "SELECT subject FROM notifications")]
    assert subjects.count("Your order is confirmed") == 1


@pytest.mark.parametrize("duplicate", [False, True])
def test_payment_failure_runs_compensation(duplicate):
    global BUS
    BUS = Bus(duplicate=duplicate)
    mods = load_services()
    before = kettles(mods)
    oid = asyncio.run(run_order(mods, amount=5000))  # provider declines
    assert status(mods, oid) == "CANCELLED"
    assert kettles(mods) == before  # reserved stock was released
    assert "stock.release" in BUS.log


def test_out_of_stock_cancels_without_charging():
    global BUS
    BUS = Bus()
    mods = load_services()
    oid = asyncio.run(run_order(mods, amount=100, qty=99))
    assert status(mods, oid) == "CANCELLED"
    assert "payment.charge" not in BUS.log
