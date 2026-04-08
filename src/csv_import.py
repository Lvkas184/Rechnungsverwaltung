"""CSV-Import für DATEV-Rechnungsausgangsbuch und Kontoauszüge.

Formate:
- DATEV EXTF (Rechnungsausgangsbuch)
- Bank-Kontoauszug CSV (Sparkasse / VoBa Kraichgau / VoBa Pur — gleiches Format)

Für CAMT.053 (XML) kann später ein weiterer Parser ergänzt werden.
"""

import csv
import io
import re
import encodings.utf_8_sig  # Force PyInstaller to bundle this encoding
import encodings.cp1252     # Force PyInstaller to bundle cp1252
from datetime import datetime, timedelta

from src.db import get_db
from src.import_history import begin_import_batch, finish_import_batch, record_invoice_import, record_payment_import
from src.invoice_rules import classify_special_invoice_status
from src.payment_rules import is_akonto_payment, is_schadensrechnung_payment

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

def import_datev_rechnungen(file_content, db_path=None, filename=None, created_by="upload"):
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
    import_batch_id = begin_import_batch(
        conn,
        "rechnungen_datev",
        "Rechnungen (DATEV)",
        filename=filename,
        created_by=created_by,
    )
    imported = 0
    skipped = 0
    touched_fields = ["name", "amount_gross", "issue_date", "document_type"]
    if idx_due is not None:
        touched_fields.append("due_date")

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
        is_credit_note = bool(sh_kz and sh_kz.upper() == "H")
        document_type = "gutschrift" if is_credit_note else "rechnung"
        # If H (Haben/Gutschrift), amount might be negative intent
        if is_credit_note and amount and amount > 0:
            amount = -amount

        name = val(idx_text)
        date = _parse_date(val(idx_date))
        due_date = _parse_date(val(idx_due))
        default_status = "Gutschrift" if document_type == "gutschrift" else (classify_special_invoice_status(inv_nr) or "Offen")
        before_row = conn.execute(
            "SELECT * FROM invoices WHERE invoice_id = ?",
            (inv_nr,),
        ).fetchone()

        if before_row is None:
            conn.execute(
                """
                INSERT INTO invoices(
                    invoice_id, name, document_type, amount_gross, issue_date, due_date,
                    status, paid_sum_eur, payment_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0)
                """,
                (
                    inv_nr,
                    name,
                    document_type,
                    abs(amount) if amount else None,
                    date,
                    due_date,
                    default_status,
                ),
            )
        elif idx_due is not None:
            conn.execute(
                """
                UPDATE invoices
                SET name = ?,
                    document_type = ?,
                    amount_gross = ?,
                    issue_date = ?,
                    due_date = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE invoice_id = ?
                """,
                (
                    name,
                    document_type,
                    abs(amount) if amount else None,
                    date,
                    due_date,
                    inv_nr,
                ),
            )
        else:
            conn.execute(
                """
                UPDATE invoices
                SET name = ?,
                    document_type = ?,
                    amount_gross = ?,
                    issue_date = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE invoice_id = ?
                """,
                (name, document_type, abs(amount) if amount else None, date, inv_nr),
            )

        record_invoice_import(conn, import_batch_id, inv_nr, before_row, touched_fields)
        imported += 1

    finish_import_batch(conn, import_batch_id, imported, skipped)
    conn.commit()
    conn.close()
    return {"imported": imported, "skipped": skipped, "error": None, "import_batch_id": import_batch_id}


# ---------------------------------------------------------------------------
# Bank-Kontoauszug CSV Import
# ---------------------------------------------------------------------------

def import_bank_csv(file_content, source_name, db_path=None, filename=None, created_by="upload"):
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
    import_batch_id = begin_import_batch(
        conn,
        f"bank_{source_name.lower().replace(' ', '_')}",
        source_name,
        filename=filename,
        created_by=created_by,
    )
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
        akonto_flag = 1 if is_akonto_payment(reference) else 0
        schadens_flag = 1 if is_schadensrechnung_payment(reference) else 0

        conn.execute(
            """INSERT INTO payments(invoice_id, source, booking_date, value_date,
                 amount_eur, reference_text, iban, beneficiary_name, matched, akonto, schadensrechnung)
               VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)""",
            (
                source_name,
                booking_date,
                valuta_date,
                amount,
                reference,
                iban,
                name,
                akonto_flag,
                schadens_flag,
            ),
        )
        payment_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        record_payment_import(conn, import_batch_id, payment_id)
        imported += 1

    finish_import_batch(conn, import_batch_id, imported, skipped)
    conn.commit()
    conn.close()
    return {"imported": imported, "skipped": skipped, "error": None, "import_batch_id": import_batch_id}


# ---------------------------------------------------------------------------
# Convenience wrappers (one per bank)
# ---------------------------------------------------------------------------

def import_sparkasse_csv(file_content, db_path=None, filename=None, created_by="upload"):
    return import_bank_csv(file_content, "Sparkasse", db_path, filename=filename, created_by=created_by)

def import_voba_kraichgau_csv(file_content, db_path=None, filename=None, created_by="upload"):
    return import_bank_csv(file_content, "VoBa Kraichgau", db_path, filename=filename, created_by=created_by)

def import_voba_pur_csv(file_content, db_path=None, filename=None, created_by="upload"):
    return import_bank_csv(file_content, "VoBa Pur", db_path, filename=filename, created_by=created_by)


# ---------------------------------------------------------------------------
# Legacy Excel/CSV Import (One-time Migration)
# ---------------------------------------------------------------------------

def import_legacy_csv(file_content, db_path=None, filename=None, created_by="migration"):
    """Import an already processed Excel/CSV sheet from the legacy pipeline.
    
    Expected columns:
    Buchungsdatum, Valutadatum, Betrag_eur, Verwendungszweck, Rechnung Name (or Name),
    Bank, Name_RAB, Rechnungsnummer manuell, ReNr_effektiv
    
    If 'ReNr_effektiv' is filled, the payment is inserted as already matched.
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

    idx_buchung = find_col("buchungsdatum")
    idx_valuta = find_col("valutadatum")
    idx_amount = find_col("betrag_eur", "betrag in eur", "amount")
    idx_ref = find_col("verwendungszweck")
    idx_name = find_col("rechnung name", "name", "empfängername")
    idx_bank = find_col("bank", "source")
    idx_effektiv = find_col("renr_effektiv", "rechnungsnummer effektiv")

    if idx_amount is None or idx_buchung is None:
        return {"imported": 0, "skipped": 0,
                "error": f"Pflichtspalten (Buchungsdatum, Betrag_eur) nicht gefunden. Vorhandene: {', '.join(header_clean)}"}

    conn = get_db(db_path)
    import_batch_id = begin_import_batch(
        conn,
        "legacy_payments",
        "Alt-Daten Zahlungen",
        filename=filename,
        created_by=created_by,
    )
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
        reference = val(idx_ref)
        bank = val(idx_bank) or "Legacy"
        
        # Determine if it's already matched
        raw_renr = val(idx_effektiv)
        matched_invoice_id = None
        is_matched = 0
        match_score = None
        match_rule = None
        
        if raw_renr:
            try:
                # Same cleanup logic as when importing DATEV Invoices
                clean_renr = int(float(str(raw_renr).replace(",", ".")))
                
                # Pruefen ob die Rechnung existiert, um Foreign-Key Fehler zu vermeiden
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM invoices WHERE invoice_id = ?", (clean_renr,))
                if cursor.fetchone():
                    matched_invoice_id = clean_renr
                    is_matched = 1
                    match_score = 1.0
                    match_rule = "Legacy Import"
            except (ValueError, TypeError):
                # Cannot parse invoice ID cleanly, leave unmatched
                pass

        akonto_flag = 1 if is_akonto_payment(reference, matched_invoice_id) else 0
        schadens_flag = 1 if is_schadensrechnung_payment(reference, matched_invoice_id) else 0

        conn.execute(
            """INSERT INTO payments(invoice_id, source, booking_date, value_date,
                 amount_eur, reference_text, iban, beneficiary_name, matched, akonto, schadensrechnung, match_score, match_rule)
               VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?)""",
            (
                matched_invoice_id,
                bank,
                booking_date,
                valuta_date,
                amount,
                reference,
                name,
                is_matched,
                akonto_flag,
                schadens_flag,
                match_score,
                match_rule,
            ),
        )
        payment_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        record_payment_import(conn, import_batch_id, payment_id)
        imported += 1

    finish_import_batch(conn, import_batch_id, imported, skipped)
    conn.commit()
    conn.close()
    return {"imported": imported, "skipped": skipped, "error": None, "import_batch_id": import_batch_id}


def import_legacy_invoices_csv(file_content, db_path=None, filename=None, created_by="migration"):
    """Import an already processed Invoice Excel/CSV sheet from the legacy pipeline.
    
    Expected columns:
    Rechnungsnummer, Betrag_Brutto, Name, Status, Art
    
    If Art is 'Gutschrift', the amount is treated as negative.
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

    idx_invnr = find_col("rechnungsnummer", "renr", "rechnungsnr")
    idx_amount = find_col("betrag_brutto", "betrag", "summe")
    idx_name = find_col("name", "kunde")
    idx_art = find_col("art", "belegart")

    if idx_invnr is None or idx_amount is None:
        return {"imported": 0, "skipped": 0,
                "error": f"Pflichtspalten (Rechnungsnummer, Betrag_Brutto) nicht gefunden. Vorhandene: {', '.join(header_clean)}"}

    conn = get_db(db_path)
    import_batch_id = begin_import_batch(
        conn,
        "legacy_invoices",
        "Alt-Daten Rechnungen",
        filename=filename,
        created_by=created_by,
    )
    imported = 0
    skipped = 0
    touched_fields = ["name", "amount_gross", "document_type"]

    for row in data:
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

        try:
            inv_nr = int(float(str(inv_nr).replace(",", ".")))
        except (ValueError, TypeError):
            try:
                inv_nr = int(re.sub(r"[^\d]", "", str(inv_nr)))
            except (ValueError, TypeError):
                skipped += 1
                continue

        amount = _parse_amount(val(idx_amount))
        art = val(idx_art)
        
        is_credit_note = bool(art and "gutschrift" in str(art).lower())
        document_type = "gutschrift" if is_credit_note else "rechnung"
        if is_credit_note and amount and amount > 0:
            amount = -amount

        name = val(idx_name)
        default_status = "Gutschrift" if document_type == "gutschrift" else (classify_special_invoice_status(inv_nr) or "Offen")
        before_row = conn.execute(
            "SELECT * FROM invoices WHERE invoice_id = ?",
            (inv_nr,),
        ).fetchone()

        if before_row is None:
            conn.execute(
                """
                INSERT INTO invoices(
                    invoice_id, name, document_type, amount_gross, status, paid_sum_eur, payment_count
                )
                VALUES (?, ?, ?, ?, ?, 0, 0)
                """,
                (inv_nr, name, document_type, abs(amount) if amount else None, default_status),
            )
        else:
            conn.execute(
                """
                UPDATE invoices
                SET name = ?,
                    document_type = ?,
                    amount_gross = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE invoice_id = ?
                """,
                (name, document_type, abs(amount) if amount else None, inv_nr),
            )

        record_invoice_import(conn, import_batch_id, inv_nr, before_row, touched_fields)
        imported += 1

    finish_import_batch(conn, import_batch_id, imported, skipped)
    conn.commit()
    conn.close()
    return {"imported": imported, "skipped": skipped, "error": None, "import_batch_id": import_batch_id}
