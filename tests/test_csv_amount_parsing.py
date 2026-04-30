"""Tests for robust amount parsing in bank CSV imports."""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.csv_import import _parse_amount, import_voba_kraichgau_csv
from src.db import init_db


def _connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def test_parse_amount_handles_grouped_thousands_without_cents():
    assert _parse_amount("17.850") == 17850.0
    assert _parse_amount("1.000") == 1000.0
    assert _parse_amount("-1.000") == -1000.0
    assert _parse_amount("1.563,07") == 1563.07
    assert _parse_amount("297,5") == 297.5


def test_bank_import_parses_dot_grouped_thousands_correctly(tmp_path):
    db_path = tmp_path / "bank_amounts.db"
    init_db(db_path)

    csv_data = (
        "Buchungsdatum;Valutadatum;Empfängername/Auftraggeber;IBAN/Kontonummer;Verwendungszweck;Betrag in EUR\n"
        "26.03.2026;26.03.2026;Clemens Lichter;DE001;Test 1;17.850\n"
        "09.04.2026;09.04.2026;Andreas Rapp;DE002;Test 2;1.000\n"
        "08.04.2026;08.04.2026;Jutta Nagel;DE003;Test 3;297,5\n"
    )

    res = import_voba_kraichgau_csv(csv_data, db_path=db_path, filename="kraichgau.csv", created_by="test")
    assert res["error"] is None
    assert res["imported"] == 3

    conn = _connect(db_path)
    rows = conn.execute(
        """
        SELECT beneficiary_name, amount_eur
        FROM payments
        ORDER BY payment_id
        """
    ).fetchall()
    conn.close()

    assert len(rows) == 3
    assert rows[0]["beneficiary_name"] == "Clemens Lichter"
    assert rows[0]["amount_eur"] == 17850.0
    assert rows[1]["beneficiary_name"] == "Andreas Rapp"
    assert rows[1]["amount_eur"] == 1000.0
    assert rows[2]["beneficiary_name"] == "Jutta Nagel"
    assert rows[2]["amount_eur"] == 297.5
