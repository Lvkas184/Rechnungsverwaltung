"""Unit tests for reminder (Mahnung) logic."""

import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import mahnung


def _make_conn(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def test_run_mahnung_respects_manual_override(tmp_path, monkeypatch):
    db_path = tmp_path / "mahnung_test.db"
    conn = _make_conn(db_path)
    conn.execute(
        """
        CREATE TABLE invoices(
          invoice_id INTEGER PRIMARY KEY,
          issue_date TEXT,
          status TEXT,
          reminder_status TEXT,
          reminder_date TEXT,
          reminder_manual INTEGER DEFAULT 0
        )
        """
    )
    old_issue_date = (datetime.utcnow().date() - timedelta(days=90)).isoformat()
    conn.execute(
        """
        INSERT INTO invoices(invoice_id, issue_date, status, reminder_status, reminder_date, reminder_manual)
        VALUES (100001, ?, 'Offen', '1. Mahnung', '2026-03-01', 1)
        """,
        (old_issue_date,),
    )
    conn.execute(
        """
        INSERT INTO invoices(invoice_id, issue_date, status, reminder_status, reminder_date, reminder_manual)
        VALUES (100002, ?, 'Offen', NULL, NULL, 0)
        """,
        (old_issue_date,),
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(mahnung, "init_db", lambda: None)
    monkeypatch.setattr(mahnung, "get_db", lambda: _make_conn(db_path))
    monkeypatch.setattr(
        mahnung,
        "load_params",
        lambda path=mahnung.PARAM_PATH: {"due_days_1": 10, "due_days_2": 30},
    )

    mahnung.run_mahnung()

    conn = _make_conn(db_path)
    manual = conn.execute(
        "SELECT reminder_status, reminder_date FROM invoices WHERE invoice_id = 100001"
    ).fetchone()
    auto = conn.execute(
        "SELECT reminder_status, reminder_date FROM invoices WHERE invoice_id = 100002"
    ).fetchone()
    conn.close()

    assert manual["reminder_status"] == "1. Mahnung"
    assert manual["reminder_date"] == "2026-03-01"
    assert auto["reminder_status"] == "2. Mahnung"
    assert auto["reminder_date"] == datetime.utcnow().date().isoformat()
