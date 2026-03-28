import glob
import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path

DB = "rechnungsverwaltung.db"


def create_schema() -> None:
    with sqlite3.connect(DB) as conn:
        conn.executescript(Path("schema/schema.sql").read_text(encoding="utf-8"))
        conn.commit()


def serial_to_iso(value):
    if value in (None, "", " "):
        return None
    try:
        n = float(value)
    except (TypeError, ValueError):
        return str(value)

    origin = date(1899, 12, 30)
    try:
        return (origin + timedelta(days=int(n))).isoformat()
    except Exception:
        return str(value)


def import_data() -> None:
    with sqlite3.connect(DB) as conn:
        def insert_invoices(rows, header):
            for r in rows:
                d = {header[i]: (r[i] if i < len(r) else None) for i in range(len(header))}
                invoice_id = d.get("Rechnungsnummer") or d.get("invoice_id")
                if not invoice_id:
                    continue
                conn.execute(
                    """
                    INSERT OR REPLACE INTO invoices(
                      invoice_id, name, amount_gross, issue_date, status, deviation_eur,
                      paid_sum_eur, last_payment_date, payment_count, action,
                      reminder_status, reminder_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        invoice_id,
                        d.get("name") or d.get("Name"),
                        d.get("betrag_brutto") or d.get("Betrag_Brutto"),
                        serial_to_iso(d.get("issue_date") or d.get("Rechnungsdatum")),
                        d.get("status"),
                        d.get("abweichung_eur"),
                        d.get("gezahlt_sum_eur"),
                        serial_to_iso(d.get("letzte_zahlung")),
                        d.get("anzahl_zahlungen"),
                        d.get("maßnahme") or d.get("bemerkung"),
                        d.get("mahnung_status"),
                        serial_to_iso(d.get("Datum Mahnung")),
                    ),
                )

        def insert_payments(rows, header):
            for r in rows:
                d = {header[i]: (r[i] if i < len(r) else None) for i in range(len(header))}
                invoice_id = d.get("Rechnungsnummer") or d.get("ReNr") or None
                conn.execute(
                    """
                    INSERT INTO payments(
                      invoice_id, source, booking_date, value_date, amount_eur,
                      reference_text, iban, beneficiary_name, matched
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        invoice_id,
                        d.get("Source") or d.get("source") or "import",
                        serial_to_iso(d.get("Buchungsdatum")),
                        serial_to_iso(d.get("Valutadatum")),
                        d.get("Betrag_eur") or d.get("amount"),
                        d.get("Verwendungszweck") or d.get("reference"),
                        d.get("IBAN/Kontonummer"),
                        d.get("Name"),
                        1 if invoice_id else 0,
                    ),
                )

        for path in glob.glob("data/*.json"):
            with open(path, encoding="utf-8") as f:
                j = json.load(f)
            title = j["title"]
            rows = j["values"]
            if not rows:
                continue
            header = rows[0]
            data_rows = rows[1:]

            if title in ("Alle Rechnungen", "Rechnungsausgangsbuch"):
                insert_invoices(data_rows, header)
            elif title in ("Zahlungen", "Kontoauszüge Sparkasse", "Import Sparkasse"):
                insert_payments(data_rows, header)
            elif title == "Manuelle ReNr Map":
                for r in data_rows:
                    d = {header[i]: (r[i] if i < len(r) else None) for i in range(len(header))}
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO manual_map(signature, mapped_invoice_id, updated_at, notes)
                        VALUES (?, ?, ?, ?)
                        """,
                        (
                            d.get("Signatur"),
                            d.get("ReNr"),
                            serial_to_iso(d.get("Zuletzt aktualisiert")),
                            None,
                        ),
                    )

        conn.commit()


if __name__ == "__main__":
    create_schema()
    import_data()
    print(f"Import abgeschlossen. DB: {DB}")
