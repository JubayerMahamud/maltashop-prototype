"""Unit tests (pyramid base). They use a throwaway SQLite file, no broker needed."""
import os
import sys
import tempfile

os.environ["DB_PATH"] = os.path.join(tempfile.mkdtemp(), "test.db")
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from inventory import main as inventory  # noqa: E402
from payment import main as payment  # noqa: E402


def stock(sku):
    return inventory.conn.execute("SELECT qty FROM stock WHERE sku=?", (sku,)).fetchone()["qty"]


def test_reserve_reduces_stock():
    before = stock("KETTLE-1")
    assert inventory.reserve("o-1", [{"sku": "KETTLE-1", "qty": 2}]) is True
    assert stock("KETTLE-1") == before - 2


def test_reserve_is_idempotent():
    inventory.reserve("o-2", [{"sku": "KETTLE-1", "qty": 1}])
    after_first = stock("KETTLE-1")
    inventory.reserve("o-2", [{"sku": "KETTLE-1", "qty": 1}])  # replayed command
    assert stock("KETTLE-1") == after_first


def test_reserve_rejects_when_not_enough_stock_and_changes_nothing():
    before = stock("HEADSET-2")
    assert inventory.reserve("o-3", [{"sku": "KETTLE-1", "qty": 1},
                                     {"sku": "HEADSET-2", "qty": 99}]) is False
    assert stock("HEADSET-2") == before


def test_release_returns_stock_once():
    inventory.reserve("o-4", [{"sku": "LAPTOP-15", "qty": 1}])
    after_reserve = stock("LAPTOP-15")
    assert inventory.release("o-4") is True
    assert stock("LAPTOP-15") == after_reserve + 1
    assert inventory.release("o-4") is False  # second release does nothing
    assert stock("LAPTOP-15") == after_reserve + 1


def test_provider_declines_large_amounts():
    assert payment.charge_provider("x", 100.0) is True
    assert payment.charge_provider("x", 5000.0) is False
