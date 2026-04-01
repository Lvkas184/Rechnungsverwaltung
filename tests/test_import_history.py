"""Tests fuer Import-Historie und batchweises Rollback."""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.csv_import import import_datev_rechnungen, import_legacy_invoices_csv, import_sparkasse_csv
from src.db import init_db
from src.import_history import rollback_import_batch


def _connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def test_payment_import_rollback_only_removes_that_batch(tmp_path):
    db_path = tmp_path / "import_history_payments.db"
    init_db(db_path)

    csv_one = (
        "Buchungsdatum;Valutadatum;Empfängername/Auftraggeber;IBAN/Kontonummer;Verwendungszweck;Betrag in EUR\n"
        "01.04.2026;01.04.2026;Max Mustermann;DE001;Erster Import;100,00\n"
    )
    csv_two = (
        "Buchungsdatum;Valutadatum;Empfängername/Auftraggeber;IBAN/Kontonummer;Verwendungszweck;Betrag in EUR\n"
        "02.04.2026;02.04.2026;Erika Musterfrau;DE002;Zweiter Import;200,00\n"
    )

    res_one = import_sparkasse_csv(csv_one, db_path=db_path, filename="one.csv", created_by="test")
    res_two = import_sparkasse_csv(csv_two, db_path=db_path, filename="two.csv", created_by="test")

    rollback = rollback_import_batch(res_one["import_batch_id"], db_path=db_path)
    assert rollback["ok"]

    conn = _connect(db_path)
    rows = conn.execute(
        "SELECT beneficiary_name, amount_eur, reference_text FROM payments ORDER BY payment_id"
    ).fetchall()
    conn.close()

    assert len(rows) == 1
    assert rows[0]["beneficiary_name"] == "Erika Musterfrau"
    assert rows[0]["amount_eur"] == 200.0
    assert rows[0]["reference_text"] == "Zweiter Import"
    assert res_two["import_batch_id"] != res_one["import_batch_id"]


def test_invoice_import_rollback_restores_previous_master_data(tmp_path):
    db_path = tmp_path / "import_history_invoices.db"
    init_db(db_path)

    conn = _connect(db_path)
    conn.execute(
        """
        INSERT INTO invoices(
            invoice_id, name, remark, amount_gross, issue_date, due_date,
            status, status_manual, paid_sum_eur, payment_count
        ) VALUES (260001, 'Alter Name', 'Bemerkung bleibt', 100.0, '2026-01-05', '2026-02-05',
                  'Offen', 1, 0, 0)
        """
    )
    conn.commit()
    conn.close()

    csv_data = (
        "Rechnungsnummer;Betrag_Brutto;Name\n"
        "260001;200,00;Neuer Name\n"
    )

    res = import_legacy_invoices_csv(csv_data, db_path=db_path, filename="inv.csv", created_by="test")
    rollback = rollback_import_batch(res["import_batch_id"], db_path=db_path)
    assert rollback["ok"]

    conn = _connect(db_path)
    row = conn.execute(
        """
        SELECT invoice_id, name, remark, amount_gross, issue_date, due_date, status_manual
        FROM invoices
        WHERE invoice_id = 260001
        """
    ).fetchone()
    conn.close()

    assert row["name"] == "Alter Name"
    assert row["remark"] == "Bemerkung bleibt"
    assert row["amount_gross"] == 100.0
    assert row["issue_date"] == "2026-01-05"
    assert row["due_date"] == "2026-02-05"
    assert row["status_manual"] == 1


def test_invoice_insert_rollback_blocked_when_later_payment_depends_on_it(tmp_path):
    db_path = tmp_path / "import_history_blocked.db"
    init_db(db_path)

    datev_csv = (
        "Umsatz;Soll/Haben-Kz;Belegdatum;Belegfeld1;Buchungstext\n"
        "123,45;S;01.04.2026;260555;Neu importierte Rechnung\n"
    )
    res = import_datev_rechnungen(datev_csv, db_path=db_path, filename="datev.csv", created_by="test")

    conn = _connect(db_path)
    conn.execute(
        """
        INSERT INTO payments(invoice_id, source, booking_date, value_date, amount_eur, reference_text, beneficiary_name, matched)
        VALUES (260555, 'Spaeter', '2026-04-02', '2026-04-02', 123.45, 'spaeterer import', 'Abhaengige Zahlung', 1)
        """
    )
    conn.commit()
    conn.close()

    rollback = rollback_import_batch(res["import_batch_id"], db_path=db_path)
    assert not rollback["ok"]
    assert "Rechnung #260555" in rollback["error"]

