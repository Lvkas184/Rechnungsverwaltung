"""Unit tests for reminder (Mahnung) logic."""

import sqlite3
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src import mahnung
from src.db import init_db


def _make_conn(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def test_run_mahnung_respects_manual_override(tmp_path, monkeypatch):
    db_path = tmp_path / "mahnung_test.db"
    init_db(db_path)
    conn = _make_conn(db_path)
    old_issue_date = (datetime.utcnow().date() - timedelta(days=90)).isoformat()
    conn.execute(
        """
        INSERT INTO invoices(invoice_id, issue_date, amount_gross, status, reminder_status, reminder_date, reminder_manual)
        VALUES (100001, ?, 100.0, 'Offen', '1. Mahnung', '2026-03-01', 1)
        """,
        (old_issue_date,),
    )
    conn.execute(
        """
        INSERT INTO invoices(invoice_id, issue_date, amount_gross, status, reminder_status, reminder_date, reminder_manual)
        VALUES (100002, ?, 100.0, 'Offen', NULL, NULL, 0)
        """,
        (old_issue_date,),
    )
    conn.execute(
        """
        INSERT INTO invoices(invoice_id, issue_date, amount_gross, status, reminder_status, reminder_date, reminder_manual)
        VALUES (100003, ?, 100.0, 'Offen', '1. Mahnung', '2026-03-01', 0)
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
        lambda path=mahnung.PARAM_PATH: {"due_days_1": 10, "due_days_2": 30, "due_days_3": 60},
    )

    mahnung.run_mahnung()

    conn = _make_conn(db_path)
    manual = conn.execute(
        "SELECT reminder_status, reminder_date FROM invoices WHERE invoice_id = 100001"
    ).fetchone()
    auto = conn.execute(
        "SELECT reminder_status, reminder_date FROM invoices WHERE invoice_id = 100002"
    ).fetchone()
    progressed = conn.execute(
        "SELECT reminder_status, reminder_date FROM invoices WHERE invoice_id = 100003"
    ).fetchone()
    progressed_history = conn.execute(
        """
        SELECT reminder_status, reminder_date
        FROM invoice_reminders
        WHERE invoice_id = 100003
        ORDER BY reminder_entry_id
        """
    ).fetchall()
    conn.close()

    assert manual["reminder_status"] == "1. Mahnung"
    assert manual["reminder_date"] == "2026-03-01"
    assert auto["reminder_status"] == "3. Mahnung"
    assert auto["reminder_date"] == datetime.utcnow().date().isoformat()
    assert progressed["reminder_status"] == "3. Mahnung"
    assert progressed_history[0]["reminder_status"] == "1. Mahnung"
    assert progressed_history[0]["reminder_date"] == "2026-03-01"
    assert progressed_history[1]["reminder_status"] == "3. Mahnung"
