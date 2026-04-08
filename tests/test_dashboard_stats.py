"""Integration tests for dashboard KPI counts."""

import re
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
    db_path = tmp_path / "dashboard_stats.db"
    init_db(db_path)

    def _get_db():
        return _connect(db_path)

    monkeypatch.setattr(app_module, "get_db", _get_db)
    monkeypatch.setattr(app_module, "init_db", lambda: init_db(db_path))
    if hasattr(app_module.app, "_db_initialized"):
        delattr(app_module.app, "_db_initialized")

    with app_module.app.test_client() as client:
        yield client, db_path


def test_dashboard_excludes_ausgebucht_and_skonto_from_open_kpis(client):
    test_client, db_path = client
    conn = _connect(db_path)
    conn.executemany(
        """
        INSERT INTO invoices(invoice_id, name, amount_gross, paid_sum_eur, status)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (260001, "A Offen", 100.0, 0.0, "Offen"),
            (260002, "B Klaerung", 200.0, 0.0, "In Klärung"),
            (260003, "C Ausgebucht", 300.0, 0.0, "ausgebucht"),
            (260004, "D Skonto", 400.0, 0.0, "Skonto"),
        ],
    )
    conn.commit()
    conn.close()

    response = test_client.get("/")
    assert response.status_code == 200
    html = response.get_data(as_text=True)

    open_match = re.search(
        r"Offene Rechnungen</div>\s*<div class=\"stat-icon\">.*?</div>\s*</div>\s*<div class=\"stat-number\">(\d+)</div>",
        html,
        re.S,
    )
    assert open_match, "Konnte KPI 'Offene Rechnungen' nicht im HTML finden."
    assert int(open_match.group(1)) == 2

    sum_match = re.search(
        r"Offener Betrag gesamt</div>\s*<div class=\"stat-icon\">.*?</div>\s*</div>\s*<div class=\"stat-number\">([^<]+)</div>",
        html,
        re.S,
    )
    assert sum_match, "Konnte KPI 'Offener Betrag gesamt' nicht im HTML finden."
    assert "300,00 €" in sum_match.group(1)

