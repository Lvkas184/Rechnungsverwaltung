"""Integration tests for manual invoice amount updates in detail view."""

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app as app_module
import src.db as db_module
from src.db import init_db


def _connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@pytest.fixture
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "invoice_amount_update.db"
    init_db(db_path)

    def _get_db(*_args, **_kwargs):
        return _connect(db_path)

    monkeypatch.setattr(app_module, "get_db", _get_db)
    monkeypatch.setattr(app_module, "init_db", lambda: init_db(db_path))
    monkeypatch.setattr(db_module, "get_db", _get_db)
    if hasattr(app_module.app, "_db_initialized"):
        delattr(app_module.app, "_db_initialized")

    with app_module.app.test_client() as client:
        yield client, db_path


def test_rechnung_update_betrag_recomputes_auto_status(client):
    test_client, db_path = client

    conn = _connect(db_path)
    conn.execute(
        """
        INSERT INTO invoices(invoice_id, name, amount_gross, status, status_manual, paid_sum_eur, deviation_eur)
        VALUES (260001, 'Testkunde', 120.0, 'Offen', 0, 0.0, -120.0)
        """
    )
    conn.execute(
        """
        INSERT INTO payments(invoice_id, source, booking_date, value_date, amount_eur, reference_text, beneficiary_name, matched)
        VALUES (260001, 'VoBa Pur', '2026-04-04', '2026-04-04', 100.0, 'RE 260001', 'Testkunde', 1)
        """
    )
    conn.commit()
    conn.close()

    response = test_client.post("/rechnungen/260001/betrag", data={"amount_gross": "100,00"})
    assert response.status_code == 302

    conn = _connect(db_path)
    row = conn.execute(
        "SELECT amount_gross, paid_sum_eur, deviation_eur, status FROM invoices WHERE invoice_id = 260001"
    ).fetchone()
    conn.close()

    assert row["amount_gross"] == 100.0
    assert row["paid_sum_eur"] == 100.0
    assert row["deviation_eur"] == 0.0
    assert row["status"] == "Bezahlt"


def test_rechnung_update_betrag_keeps_manual_status(client):
    test_client, db_path = client

    conn = _connect(db_path)
    conn.execute(
        """
        INSERT INTO invoices(invoice_id, name, amount_gross, status, status_manual, paid_sum_eur, deviation_eur)
        VALUES (260002, 'Manuell', 120.0, 'In Klärung', 1, 0.0, -120.0)
        """
    )
    conn.execute(
        """
        INSERT INTO payments(invoice_id, source, booking_date, value_date, amount_eur, reference_text, beneficiary_name, matched)
        VALUES (260002, 'VoBa Pur', '2026-04-04', '2026-04-04', 100.0, 'RE 260002', 'Manuell', 1)
        """
    )
    conn.commit()
    conn.close()

    response = test_client.post("/rechnungen/260002/betrag", data={"amount_gross": "100,00"})
    assert response.status_code == 302

    conn = _connect(db_path)
    row = conn.execute(
        "SELECT amount_gross, paid_sum_eur, deviation_eur, status, status_manual FROM invoices WHERE invoice_id = 260002"
    ).fetchone()
    conn.close()

    assert row["amount_gross"] == 100.0
    assert row["paid_sum_eur"] == 100.0
    assert row["deviation_eur"] == 0.0
    assert row["status_manual"] == 1
    assert row["status"] == "In Klärung"
