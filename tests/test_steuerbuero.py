"""Integration tests for Steuerbuero change tracking page."""

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app as app_module
from src.db import init_db


def _connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@pytest.fixture
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "steuerbuero.db"
    init_db(db_path)

    def _get_db():
        return _connect(db_path)

    monkeypatch.setattr(app_module, "get_db", _get_db)
    monkeypatch.setattr(app_module, "init_db", lambda: init_db(db_path))
    if hasattr(app_module.app, "_db_initialized"):
        delattr(app_module.app, "_db_initialized")

    with app_module.app.test_client() as client:
        yield client, db_path


def test_steuerbuero_default_view_shows_only_open_and_origin_labels(client):
    test_client, db_path = client
    conn = _connect(db_path)
    conn.executemany(
        """
        INSERT INTO manual_change_log(
            entry_origin, is_resolved, change_scope, action_code, action_label, changed_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            ("auto", 0, "invoice", "invoice_amount_update", "Auto offen", "2026-04-05T10:00:00"),
            ("manual", 0, "payment", "manual_custom_entry", "Manuell offen", "2026-04-06T10:00:00"),
            ("auto", 1, "invoice", "invoice_status_manual_set", "Auto abgehakt", "2026-04-07T10:00:00"),
            ("auto", 0, "payment", "manual_single", "Ausgeblendet", "2026-04-08T10:00:00"),
        ],
    )
    conn.commit()
    conn.close()

    response = test_client.get("/steuerbuero?month=2026-04")
    assert response.status_code == 200
    html = response.get_data(as_text=True)

    assert "Auto offen" in html
    assert "Manuell offen" in html
    assert "Auto abgehakt" not in html
    assert "Ausgeblendet" not in html
    assert "Automatisch" in html
    assert "Manuell" in html
    assert "Erstellt durch" not in html


def test_steuerbuero_add_entry_persists_manual_and_resolved(client):
    test_client, db_path = client
    conn = _connect(db_path)
    conn.execute(
        """
        INSERT INTO invoices(invoice_id, name, amount_gross, paid_sum_eur, status)
        VALUES (261085, 'Test', 100.0, 0.0, 'Offen')
        """
    )
    conn.commit()
    conn.close()

    response = test_client.post(
        "/steuerbuero/add",
        data={
            "month": "2026-04",
            "status": "all",
            "action_label": "Neuer Hinweis",
            "change_scope": "note",
            "invoice_id": "261085",
            "before_value": "alt",
            "after_value": "neu",
            "note": "mit Steuerbuero klaeren",
            "changed_at": "2026-04-15T14:30",
            "is_resolved": "1",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302

    conn = _connect(db_path)
    row = conn.execute(
        """
        SELECT entry_origin, is_resolved, action_label, invoice_id, before_value, after_value, note
        FROM manual_change_log
        WHERE action_label = 'Neuer Hinweis'
        """
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["entry_origin"] == "manual"
    assert int(row["is_resolved"] or 0) == 1
    assert row["invoice_id"] == 261085
    assert row["before_value"] == "alt"
    assert row["after_value"] == "neu"
    assert row["note"] == "mit Steuerbuero klaeren"


def test_steuerbuero_update_and_toggle_resolved(client):
    test_client, db_path = client
    conn = _connect(db_path)
    conn.execute(
        """
        INSERT INTO payments(payment_id, source, booking_date, value_date, amount_eur, reference_text, beneficiary_name, matched)
        VALUES (3001, 'VoBa Pur', '2026-04-01', '2026-04-01', 50.0, 'Ref', 'Name', 0)
        """
    )
    conn.execute(
        """
        INSERT INTO manual_change_log(
            entry_origin, is_resolved, change_scope, action_code, action_label, changed_at
        ) VALUES ('auto', 0, 'invoice', 'invoice_amount_update', 'Vor Update', '2026-04-10T10:00:00')
        """
    )
    change_id = conn.execute("SELECT change_id FROM manual_change_log WHERE action_label = 'Vor Update'").fetchone()[
        "change_id"
    ]
    conn.commit()
    conn.close()

    response = test_client.post(
        "/steuerbuero/update",
        data={
            "change_id": str(change_id),
            "month": "2026-04",
            "status": "all",
            "action_label": "Nach Update",
            "change_scope": "payment",
            "payment_id": "3001",
            "note": "bearbeitet",
            "changed_at": "2026-04-11T11:11",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302

    response = test_client.post(
        "/steuerbuero/toggle-resolved",
        data={
            "change_id": str(change_id),
            "month": "2026-04",
            "status": "all",
            "resolved": "1",
        },
        follow_redirects=False,
    )
    assert response.status_code == 302

    conn = _connect(db_path)
    row = conn.execute(
        """
        SELECT entry_origin, change_scope, payment_id, action_label, note, is_resolved, resolved_at
        FROM manual_change_log
        WHERE change_id = ?
        """,
        (change_id,),
    ).fetchone()
    conn.close()

    assert row["entry_origin"] == "auto"
    assert row["change_scope"] == "payment"
    assert row["payment_id"] == 3001
    assert row["action_label"] == "Nach Update"
    assert row["note"] == "bearbeitet"
    assert int(row["is_resolved"] or 0) == 1
    assert row["resolved_at"]
