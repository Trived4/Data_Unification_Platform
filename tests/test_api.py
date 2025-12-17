import os
import sys
import sqlite3
import pytest

# If tests live inside build/tests, add the parent ("build") to sys.path *before* importing api
CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from fastapi.testclient import TestClient
from api import app, DB_PATH  # DB_PATH points to build/unified.db

client = TestClient(app)


@pytest.fixture(scope="session", autouse=True)
def ensure_unified_db():
    """
    Make sure unified.db exists and has the core tables before running any tests.
    If not, skip the whole test session with a clear message instead of failing
    with sqlite OperationalError.
    """
    if not os.path.exists(DB_PATH):
        pytest.skip(
            f"unified.db not found at {DB_PATH}. "
            "Run: python unify_pos.py --data-dir ./data --db-path ./unified.db"
        )

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Check for one of the key tables
    cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name = 'fact_transaction_header'
        """
    )
    exists = cur.fetchone() is not None
    conn.close()

    if not exists:
        pytest.skip(
            "unified.db exists but schema not initialized. "
            "Run: python unify_pos.py --data-dir ./data --db-path ./unified.db"
        )


def test_revenue_by_location():
    resp = client.get("/revenue_by_location")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


def test_service_popularity_overall():
    resp = client.get("/service_popularity")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


def test_customer_engagement_by_membership():
    resp = client.get("/customer_engagement_by_membership")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


def test_summary_all():
    resp = client.get("/summary_all")
    assert resp.status_code == 200
    data = resp.json()
    assert "metrics" in data
    assert "analysis" in data
    assert "revenue_by_location" in data["metrics"]


def test_nl_query_all():
    resp = client.get("/nl_query", params={"q": "give me everything", "all": True})
    assert resp.status_code == 200
    data = resp.json()
    assert data["type"] == "combined"
    assert "data" in data
    assert "metrics" in data["data"]


def test_nl_query_highest_revenue_location():
    resp = client.get(
        "/nl_query",
        params={"q": "Which location has the highest revenue?"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["intent"] in ("highest_revenue_location", "revenue_by_location")
