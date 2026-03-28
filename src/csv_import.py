"""CSV-Import für DATEV-Rechnungsausgangsbuch und Kontoauszüge.

Formate:
- DATEV EXTF (Rechnungsausgangsbuch)
- Bank-Kontoauszug CSV (Sparkasse / VoBa Kraichgau / VoBa Pur — gleiches Format)

Für CAMT.053 (XML) kann später ein weiterer Parser ergänzt werden.
"""

import csv
import io
import re
from datetime import datetime, timedelta

from src.db import get_db

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_date(value):
    """Parse German/ISO dates to ISO string."""
    if not value or not str(value).strip():
        return None
    value = str(value).strip()
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d.%m.%y", "%Y%m%d"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    # Excel serial number
    try:
        n = float(value)
        return (datetime(1899, 12, 30).date() + timedelta(days=int(n))).isoformat()
    except (ValueError, TypeError):
        pass
    return value


def _parse_amount(value):
    """Parse German amount (1.234,56) or plain float."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    value = str(value).strip()
    if not value:
        return None
    # German: 1.234,56  →  1234.56
    if "," in value and "." in value:
        if value.index(".") < value.index(","):
            value = value.replace(".", "").replace(",", ".")
        else:
            value = value.replace(",", "")
    elif "," in value:
        value = value.replace(",", ".")
    # Remove currency symbols
    value = re.sub(r"[€\s]", "", value)
    try:
        return float(value)
    except ValueError:
        return None


def _read_csv(file_content):
    """Read CSV content, auto-detect encoding and delimiter.

    Returns (header, data_rows).
    """
    if isinstance(file_content, bytes):
        try:
            text = file_content.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = file_content.decode("latin-1")
    else:
        text = file_content

    # Detect delimiter
    first_line = text.split("\n")[0]
    delimiter = ";" if first_line.count(";") >= first_line.count(",") else ","

    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    rows = list(reader)
    if not rows:
        return [], []
    return rows[0], rows[1:]


# ---------------------------------------------------------------------------
# DATEV EXTF Import (Rechnungsausgangsbuch)
# ---------------------------------------------------------------------------

def import_datev_rechnungen(file_content, db_path=None):
    """Import invoices from DATEV EXTF export.

    DATEV format:
    - Row 1: EXTF header line (metadata — skip)
    - Row 2: Column headers
    - Row 3+: Data

    Key columns (0-indexed):
      A (0): Umsatz — amount
      B (1): Soll/Haben KZ — S=Forderung, H=Gutschrift
      K (10): Belegdatum — document date (DDMM or DDMMJJJJ)
      L (11): Belegfeld1 — invoice number (Rechnungsnummer!)
      N (13): Buchungstext — customer name
      P (15): Diverse Adressnummer — customer number
    """
    raw_header, raw_data = _read_csv(file_content)
    if not raw_data:
        return {"imported": 0, "skipped": 0, "error": "Leere CSV-Datei"}

    # DATEV EXTF: first row starts with "EXTF" — that's metadata, skip it.
    # The actual headers are in raw_data[0], data starts at raw_data[1].
    if raw_header and raw_header[0].strip().upper() == "EXTF":
        if len(raw_data) < 2:
            return {"imported": 0, "skipped": 0, "error": "DATEV-Datei hat keine Daten"}
        header = [h.strip() for h in raw_data[0]]
        data = raw_data[1:]
    else:
        # Fallback: maybe no EXTF header, treat first row as header
        header = [h.strip() for h in raw_header]
        data = raw_data

    # Map columns by name (flexible)
    col_map = {h.lower(): i for i, h in enumerate(header)}

    def find_col(*names):
        for n in names:
            if n.lower() in col_map:
                return col_map[n.lower()]
        return None

    idx_amount = find_col("Umsatz", "Umsatz (ohne Soll/Haben-Kz)")
    idx_sh = find_col("Soll/Haben-Kz", "Soll/Haben KZ")
    idx_date = find_col("Belegdatum")
    idx_invnr = find_col("Belegfeld1", "Belegfeld 1")
    idx_text = find_col("Buchungstext")
    idx_addr = find_col("Diverse Adressnummer")
    idx_due = find_col("Zugeordnete Fälligkeit", "Zugeordnete Faelligkeit")
    idx_konto = find_col("Konto")
    idx_gegenkonto = find_col("Gegenkonto")

    if idx_invnr is None and idx_amount is None:
        return {"imported": 0, "skipped": 0,
                "error": f"Spalten 'Belegfeld1'/'Umsatz' nicht gefunden. Vorhandene: {', '.join(header[:20])}"}

    conn = get_db(db_path)
    imported = 0
    skipped = 0

    for row in data:
        # Skip empty rows
        if not row or all(c.strip() == "" for c in row):
            continue

        def val(idx):
            if idx is None or idx >= len(row):
                return None
            return row[idx].strip() if row[idx] else None

        inv_nr = val(idx_invnr)
        if not inv_nr:
            skipped += 1
            continue

        # Try to parse invoice number as integer
        try:
            inv_nr = int(float(inv_nr))
        except (ValueError, TypeError):
            # Keep as string if not numeric
            try:
                inv_nr = int(re.sub(r"[^\d]", "", str(inv_nr)))
            except (ValueError, TypeError):
                skipped += 1
                continue

        amount = _parse_amount(val(idx_amount))
        sh_kz = val(idx_sh)
        # If H (Haben/Gutschrift), amount might be negative intent
        if sh_kz and sh_kz.upper() == "H" and amount and amount > 0:
            amount = -amount

        name = val(idx_text)
        date = _parse_date(val(idx_date))
        due_date = _parse_date(val(idx_due))

        conn.execute(
            """INSERT OR REPLACE INTO invoices(invoice_id, name, amount_gross, issue_date, due_date,
                   status, paid_sum_eur, payment_count)
               VALUES (?, ?, ?,  ?, ?,
                       COALESCE((SELECT status FROM invoices WHERE invoice_id = ?), 'Offen'),
                       COALESCE((SELECT paid_sum_eur FROM invoices WHERE invoice_id = ?), 0),
                       COALESCE((SELECT payment_count FROM invoices WHERE invoice_id = ?), 0))""",
            (inv_nr, name, abs(amount) if amount else None, date, due_date,
             inv_nr, inv_nr, inv_nr),
        )
        imported += 1

    conn.commit()
    conn.close()
    return {"imported": imported, "skipped": skipped, "error": None}


# ---------------------------------------------------------------------------
# Bank-Kontoauszug CSV Import
# ---------------------------------------------------------------------------

def import_bank_csv(file_content, source_name, db_path=None):
    """Import bank statement CSV.

    All three banks (Sparkasse, VoBa Kraichgau, VoBa Pur) share the same format:
      Automat | Sammlerauflösung | Buchungsdatum | Valutadatum |
      Empfängername/Auftraggeber | IBAN/Kontonummer | BIC/BLZ |
      Verwendungszweck | Betrag in EUR | Notiz | Anzahl Belege | Geprüft

    The bank name is NOT in the CSV — the user selects it via the upload button.
    """
    header, data = _read_csv(file_content)
    if not header or not data:
        return {"imported": 0, "skipped": 0, "error": "Leere CSV-Datei"}

    header_clean = [h.strip() for h in header]
    col_map = {h.lower(): i for i, h in enumerate(header_clean)}

    def find_col(*names):
        for n in names:
            if n.lower() in col_map:
                return col_map[n.lower()]
        return None

    idx_buchung = find_col("Buchungsdatum", "Buchungstag")
    idx_valuta = find_col("Valutadatum", "Wertstellung")
    idx_name = find_col("Empfängername/Auftraggeber", "Empfängername", "Auftraggeber", "Name")
    idx_iban = find_col("IBAN/Kontonummer", "IBAN")
    idx_bic = find_col("BIC/BLZ", "BIC")
    idx_ref = find_col("Verwendungszweck", "Buchungstext")
    idx_amount = find_col("Betrag in EUR", "Betrag", "Umsatz")

    if idx_amount is None:
        return {"imported": 0, "skipped": 0,
                "error": f"Spalte 'Betrag in EUR' nicht gefunden. Vorhandene: {', '.join(header_clean)}"}

    conn = get_db(db_path)
    imported = 0
    skipped = 0

    for row in data:
        if not row or all(c.strip() == "" for c in row):
            continue

        def val(idx):
            if idx is None or idx >= len(row):
                return None
            return row[idx].strip() if row[idx] else None

        amount = _parse_amount(val(idx_amount))
        if amount is None:
            skipped += 1
            continue

        booking_date = _parse_date(val(idx_buchung))
        valuta_date = _parse_date(val(idx_valuta))
        name = val(idx_name)
        iban = val(idx_iban)
        reference = val(idx_ref)

        conn.execute(
            """INSERT INTO payments(invoice_id, source, booking_date, value_date,
                 amount_eur, reference_text, iban, beneficiary_name, matched)
               VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, 0)""",
            (source_name, booking_date, valuta_date, amount, reference, iban, name),
        )
        imported += 1

    conn.commit()
    conn.close()
    return {"imported": imported, "skipped": skipped, "error": None}


# ---------------------------------------------------------------------------
# Convenience wrappers (one per bank)
# ---------------------------------------------------------------------------

def import_sparkasse_csv(file_content, db_path=None):
    return import_bank_csv(file_content, "Sparkasse", db_path)

def import_voba_kraichgau_csv(file_content, db_path=None):
    return import_bank_csv(file_content, "VoBa Kraichgau", db_path)

def import_voba_pur_csv(file_content, db_path=None):
    return import_bank_csv(file_content, "VoBa Pur", db_path)
