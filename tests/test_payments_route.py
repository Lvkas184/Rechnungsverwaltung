"""Integration tests fuer Zahlungslisten-Filter."""

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
    db_path = tmp_path / "payments_route.db"
    init_db(db_path)

    def _get_db():
        return _connect(db_path)

    monkeypatch.setattr(app_module, "get_db", _get_db)
    monkeypatch.setattr(app_module, "init_db", lambda: init_db(db_path))
    if hasattr(app_module.app, "_db_initialized"):
        delattr(app_module.app, "_db_initialized")

    with app_module.app.test_client() as client:
        yield client, db_path


def test_zahlungen_bank_filter_limits_rows_and_preserves_dropdown(client):
    test_client, db_path = client

    conn = _connect(db_path)
    conn.execute(
        """
        INSERT INTO payments(source, booking_date, value_date, amount_eur, reference_text, beneficiary_name, matched)
        VALUES ('VoBa Pur', '2026-04-01', '2026-04-01', 100.0, 'Pur Zahlung', 'Alpha', 0)
        """
    )
    conn.execute(
        """
        INSERT INTO payments(source, booking_date, value_date, amount_eur, reference_text, beneficiary_name, matched)
        VALUES ('Sparkasse', '2026-04-02', '2026-04-02', 200.0, 'Sparkassen Zahlung', 'Beta', 0)
        """
    )
    conn.commit()
    conn.close()

    response = test_client.get("/zahlungen?show=all&bank=VoBa+Pur")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Pur Zahlung" in html
    assert "Sparkassen Zahlung" not in html
    assert '<option value="VoBa Pur" selected>' in html
    assert '<option value="Sparkasse"' in html


def test_negative_payment_is_automatically_effective_matched(client):
    test_client, db_path = client

    conn = _connect(db_path)
    conn.execute(
        """
        INSERT INTO payments(source, booking_date, value_date, amount_eur, reference_text, beneficiary_name, matched)
        VALUES ('VoBa Pur', '2026-04-03', '2026-04-03', -55.0, 'Ausgangszahlung', 'Lieferant', 0)
        """
    )
    conn.commit()
    conn.close()

    response = test_client.get("/zahlungen?show=all")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Ausgangszahlung" in html
    assert "✅ Zugeordnet" in html


def test_negative_payment_not_in_unmatched_filter(client):
    test_client, db_path = client

    conn = _connect(db_path)
    conn.execute(
        """
        INSERT INTO payments(source, booking_date, value_date, amount_eur, reference_text, beneficiary_name, matched)
        VALUES ('VoBa Pur', '2026-04-03', '2026-04-03', -12.0, 'Negativ', 'Lieferant', 0)
        """
    )
    conn.execute(
        """
        INSERT INTO payments(source, booking_date, value_date, amount_eur, reference_text, beneficiary_name, matched)
        VALUES ('VoBa Pur', '2026-04-03', '2026-04-03', 120.0, 'Positiv Offen', 'Kunde', 0)
        """
    )
    conn.commit()
    conn.close()

    response = test_client.get("/zahlungen?show=all&filter=unmatched")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Positiv Offen" in html
    assert "Negativ" not in html
