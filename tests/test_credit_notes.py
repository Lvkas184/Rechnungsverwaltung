"""Integration tests for credit-note document type workflows."""

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
    db_path = tmp_path / "credit_notes.db"
    init_db(db_path)

    def _get_db(*_args, **_kwargs):
        return _connect(db_path)

    monkeypatch.setattr(app_module, "get_db", _get_db)
    monkeypatch.setattr(app_module, "init_db", lambda: init_db(db_path))
    monkeypatch.setattr(db_module, "get_db", _get_db)
    if hasattr(app_module.app, "_db_initialized"):
        delattr(app_module.app, "_db_initialized")

    with app_module.app.test_client() as test_client:
        yield test_client, db_path


def test_rechnungen_route_filters_document_type(client):
    test_client, db_path = client

    conn = _connect(db_path)
    conn.execute(
        """
        INSERT INTO invoices(invoice_id, name, document_type, amount_gross, status)
        VALUES (260001, 'Normale Rechnung', 'rechnung', 100.0, 'Offen')
        """
    )
    conn.execute(
        """
        INSERT INTO invoices(invoice_id, name, document_type, amount_gross, status)
        VALUES (260002, 'Kunden-Gutschrift', 'gutschrift', 25.0, 'Gutschrift')
        """
    )
    conn.commit()
    conn.close()

    response_rechnung = test_client.get("/rechnungen")
    assert response_rechnung.status_code == 200
    html_rechnung = response_rechnung.get_data(as_text=True)
    assert "Normale Rechnung" in html_rechnung
    assert "Kunden-Gutschrift" not in html_rechnung

    response_gutschrift = test_client.get("/rechnungen?doc_type=gutschrift")
    assert response_gutschrift.status_code == 200
    html_gutschrift = response_gutschrift.get_data(as_text=True)
    assert "Normale Rechnung" not in html_gutschrift
    assert "Kunden-Gutschrift" in html_gutschrift
    assert "Zuordnung" in html_gutschrift
    assert "Nicht zugeordnet" in html_gutschrift


def test_rechnung_typ_toggle_updates_document_type_and_reminders(client):
    test_client, db_path = client

    conn = _connect(db_path)
    conn.execute(
        """
        INSERT INTO invoices(
            invoice_id, name, document_type, amount_gross, status,
            reminder_status, reminder_date, reminder_manual
        )
        VALUES (260010, 'Typwechsel', 'rechnung', 100.0, 'Offen', '2. Mahnung', '2026-04-01', 1)
        """
    )
    conn.execute(
        """
        INSERT INTO invoice_reminders(invoice_id, reminder_status, reminder_date, manual_entry)
        VALUES (260010, '2. Mahnung', '2026-04-01', 1)
        """
    )
    conn.commit()
    conn.close()

    response = test_client.post(
        "/rechnungen/260010/typ",
        data={"document_type": "gutschrift"},
    )
    assert response.status_code == 302

    conn = _connect(db_path)
    row = conn.execute(
        """
        SELECT document_type, status, credit_target_invoice_id,
               reminder_status, reminder_date, reminder_manual
        FROM invoices
        WHERE invoice_id = 260010
        """
    ).fetchone()
    reminder_count = conn.execute(
        "SELECT COUNT(*) FROM invoice_reminders WHERE invoice_id = 260010"
    ).fetchone()[0]
    conn.close()

    assert row["document_type"] == "gutschrift"
    assert row["status"] == "Gutschrift"
    assert row["credit_target_invoice_id"] is None
    assert row["reminder_status"] is None
    assert row["reminder_date"] is None
    assert int(row["reminder_manual"] or 0) == 0
    assert reminder_count == 0

    response_back = test_client.post(
        "/rechnungen/260010/typ",
        data={"document_type": "rechnung"},
    )
    assert response_back.status_code == 302

    conn = _connect(db_path)
    row_back = conn.execute(
        "SELECT document_type, credit_target_invoice_id FROM invoices WHERE invoice_id = 260010"
    ).fetchone()
    conn.close()

    assert row_back["document_type"] == "rechnung"
    assert row_back["credit_target_invoice_id"] is None


def test_gutschrift_assignment_changes_target_paid_sum_and_status(client):
    test_client, db_path = client

    conn = _connect(db_path)
    conn.execute(
        """
        INSERT INTO invoices(invoice_id, name, document_type, amount_gross, status, paid_sum_eur)
        VALUES (260100, 'Zielrechnung', 'rechnung', 100.0, 'Offen', 0.0)
        """
    )
    conn.execute(
        """
        INSERT INTO invoices(invoice_id, name, document_type, amount_gross, status)
        VALUES (260101, 'Verrechenbare Gutschrift', 'gutschrift', 100.0, 'Gutschrift')
        """
    )
    conn.commit()
    conn.close()

    response_assign = test_client.post(
        "/rechnungen/260101/gutschrift-zuordnung",
        data={"target_invoice_id": "260100"},
    )
    assert response_assign.status_code == 302

    conn = _connect(db_path)
    credit = conn.execute(
        "SELECT credit_target_invoice_id FROM invoices WHERE invoice_id = 260101"
    ).fetchone()
    target = conn.execute(
        "SELECT paid_sum_eur, payment_count, status FROM invoices WHERE invoice_id = 260100"
    ).fetchone()
    conn.close()

    assert credit["credit_target_invoice_id"] == 260100
    assert target["paid_sum_eur"] == 100.0
    assert target["payment_count"] == 1
    assert target["status"] == "Gutschrift"

    response_gutschrift = test_client.get("/rechnungen?doc_type=gutschrift")
    html_gutschrift = response_gutschrift.get_data(as_text=True)
    assert "✅ Zugeordnet" in html_gutschrift
    assert "Rechnung #260100" in html_gutschrift

    response_clear = test_client.post(
        "/rechnungen/260101/gutschrift-zuordnung",
        data={"target_invoice_id": ""},
    )
    assert response_clear.status_code == 302

    conn = _connect(db_path)
    credit_cleared = conn.execute(
        "SELECT credit_target_invoice_id FROM invoices WHERE invoice_id = 260101"
    ).fetchone()
    target_cleared = conn.execute(
        "SELECT paid_sum_eur, payment_count, status FROM invoices WHERE invoice_id = 260100"
    ).fetchone()
    conn.close()

    assert credit_cleared["credit_target_invoice_id"] is None
    assert target_cleared["paid_sum_eur"] == 0.0
    assert target_cleared["payment_count"] == 0
    assert target_cleared["status"] == "Offen"


def test_manual_payment_assignment_rejects_credit_note_target(client):
    test_client, db_path = client

    conn = _connect(db_path)
    conn.execute(
        """
        INSERT INTO invoices(invoice_id, name, document_type, amount_gross, status)
        VALUES (260201, 'Nur Gutschrift', 'gutschrift', 50.0, 'Gutschrift')
        """
    )
    conn.execute(
        """
        INSERT INTO payments(source, booking_date, value_date, amount_eur, reference_text, beneficiary_name, matched)
        VALUES ('VoBa Pur', '2026-04-06', '2026-04-06', 50.0, 'Test', 'Kunde', 0)
        """
    )
    payment_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()

    response = test_client.post(
        f"/zahlungen/{payment_id}/manual/assign",
        data={"invoice_id": "260201"},
    )
    assert response.status_code == 302

    conn = _connect(db_path)
    payment = conn.execute(
        "SELECT invoice_id, matched, match_rule FROM payments WHERE payment_id = ?",
        (payment_id,),
    ).fetchone()
    conn.close()

    assert payment["invoice_id"] is None
    assert int(payment["matched"] or 0) == 0
    assert payment["match_rule"] is None
