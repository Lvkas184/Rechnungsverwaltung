"""Tests fuer den mehrstufigen Mahnverlauf."""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.db import init_db
from src.reminders import clear_invoice_reminders, fetch_invoice_reminder_history, save_invoice_reminder


def _connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def test_manual_reminders_are_stored_as_history_and_latest_stage_wins(tmp_path):
    db_path = tmp_path / "reminder_history.db"
    init_db(db_path)

    conn = _connect(db_path)
    conn.execute(
        """
        INSERT INTO invoices(invoice_id, name, amount_gross, status, reminder_status, reminder_date, reminder_manual)
        VALUES (260001, 'Testkunde', 100.0, 'Offen', '1. Mahnung', '2026-03-01', 1)
        """
    )

    save_invoice_reminder(
        conn,
        260001,
        "2. Mahnung",
        "2026-04-01",
        manual_entry=1,
        manual_override=1,
    )
    conn.commit()

    history = fetch_invoice_reminder_history(conn, 260001)
    invoice = conn.execute(
        "SELECT reminder_status, reminder_date, reminder_manual FROM invoices WHERE invoice_id = 260001"
    ).fetchone()
    conn.close()

    assert [row["reminder_status"] for row in history] == ["1. Mahnung", "2. Mahnung"]
    assert [row["reminder_date"] for row in history] == ["2026-03-01", "2026-04-01"]
    assert invoice["reminder_status"] == "2. Mahnung"
    assert invoice["reminder_date"] == "2026-04-01"
    assert invoice["reminder_manual"] == 1


def test_clear_invoice_reminders_removes_history_and_current_state(tmp_path):
    db_path = tmp_path / "reminder_clear.db"
    init_db(db_path)

    conn = _connect(db_path)
    conn.execute(
        """
        INSERT INTO invoices(invoice_id, name, amount_gross, status)
        VALUES (260002, 'Testkunde', 100.0, 'Offen')
        """
    )
    save_invoice_reminder(
        conn,
        260002,
        "1. Mahnung",
        "2026-03-10",
        manual_entry=1,
        manual_override=1,
    )
    clear_invoice_reminders(conn, 260002, manual_override=1)
    conn.commit()

    invoice = conn.execute(
        "SELECT reminder_status, reminder_date, reminder_manual FROM invoices WHERE invoice_id = 260002"
    ).fetchone()
    history_count = conn.execute(
        "SELECT COUNT(*) FROM invoice_reminders WHERE invoice_id = 260002"
    ).fetchone()[0]
    conn.close()

    assert invoice["reminder_status"] is None
    assert invoice["reminder_date"] is None
    assert invoice["reminder_manual"] == 1
    assert history_count == 0
