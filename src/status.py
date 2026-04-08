"""Status-Berechnung für invoices.

Statuses:
- Akonto          (Abschlagsrechnung: Rechnungsnummer 9xxxxx)
- Schadensrechnungen (Rechnungsnummer 8xxxxx)
- Offen           (paid_sum == 0)
- Bezahlt         (Abweichung innerhalb Toleranz)
- Bezahlt mit Mahngebühr (Abweichung entspricht Mahngebühr)
- Überzahlung     (paid_sum > amount_gross + Toleranz)
- Teiloffen/Unterzahlung  (sonst)
"""

import json

from src.db import PARAM_PATH
from src.invoice_rules import classify_special_invoice_status


def load_params(path=PARAM_PATH):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _parse_float_param(value, default):
    try:
        if value is None:
            return float(default)
        if isinstance(value, str):
            cleaned = value.strip().replace("€", "").replace(" ", "").replace(",", ".")
            if cleaned == "":
                return float(default)
            return float(cleaned)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _extract_mahnstufe(reminder_status):
    """Return Mahnstufe 1/2/3 from reminder_status text, else None."""
    value = str(reminder_status or "").strip()
    if value.startswith("1."):
        return 1
    if value.startswith("2."):
        return 2
    if value.startswith("3."):
        return 3
    return None


def _pick_mahngebuehr(inv, fee_1, fee_2, fee_3):
    """Choose expected fee based on invoice reminder status."""
    reminder_status = None
    try:
        reminder_status = inv["reminder_status"]
    except (TypeError, KeyError, IndexError):
        if isinstance(inv, dict):
            reminder_status = inv.get("reminder_status")

    stufe = _extract_mahnstufe(reminder_status)
    if stufe == 1:
        return fee_1
    if stufe == 2:
        return fee_2
    if stufe == 3:
        return fee_3
    return fee_1


def _matches_mahngebuehr(inv, deviation, tolerance, fee_1, fee_2, fee_3):
    """Match configured Mahngebuehr.

    If a Mahnstufe is set on the invoice, only that exact stage fee matches.
    If no Mahnstufe is set, accept any configured reminder fee as fallback.
    """
    reminder_status = None
    try:
        reminder_status = inv["reminder_status"]
    except (TypeError, KeyError, IndexError):
        if isinstance(inv, dict):
            reminder_status = inv.get("reminder_status")

    stufe = _extract_mahnstufe(reminder_status)
    if stufe == 1:
        candidate_fees = [fee_1]
    elif stufe == 2:
        candidate_fees = [fee_2]
    elif stufe == 3:
        candidate_fees = [fee_3]
    else:
        candidate_fees = [fee for fee in (fee_1, fee_2, fee_3) if fee > 0]

    return any(abs(deviation - fee) <= tolerance for fee in candidate_fees)


def _row_value(row, key, default=None):
    try:
        return row[key]
    except (TypeError, KeyError, IndexError):
        if isinstance(row, dict):
            return row.get(key, default)
        return default


def _has_column(conn, table_name, column_name):
    cols = {row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})")}
    return column_name in cols


def _is_fully_settled_by_credit(inv, deviation, tolerance):
    """True, if the invoice is effectively settled by linked credit notes."""
    try:
        credit_applied = float(_row_value(inv, "credit_applied_eur", 0.0) or 0.0)
    except (TypeError, ValueError):
        credit_applied = 0.0
    try:
        amount = float(_row_value(inv, "amount_gross", 0.0) or 0.0)
    except (TypeError, ValueError):
        amount = 0.0

    if credit_applied <= tolerance:
        return False
    # "Durch Gutschrift bezahlt": credit note amount alone covers the invoice.
    return (amount - credit_applied) <= tolerance and abs(float(deviation or 0.0)) <= tolerance


def compute_status_row(inv, tolerance, mahngebuehr_eur=0.0, mahngebuehr_2_eur=None, mahngebuehr_3_eur=None):
    """Compute status and deviation for a single invoice dict/Row."""
    invoice_id = _row_value(inv, "invoice_id")
    document_type = str(_row_value(inv, "document_type", "rechnung") or "rechnung").strip().lower()
    if document_type == "gutschrift":
        return "Gutschrift", 0.0

    fee_1 = max(0.0, _parse_float_param(mahngebuehr_eur, 0.0))
    fee_2 = max(0.0, _parse_float_param(mahngebuehr_2_eur, fee_1)) if mahngebuehr_2_eur is not None else fee_1
    fee_3 = max(0.0, _parse_float_param(mahngebuehr_3_eur, fee_2)) if mahngebuehr_3_eur is not None else fee_2
    paid = _row_value(inv, "paid_sum_eur", 0.0) or 0.0
    amount = _row_value(inv, "amount_gross", 0.0) or 0.0
    deviation = paid - amount
    special_status = classify_special_invoice_status(invoice_id)
    if special_status:
        status = special_status
    elif paid == 0:
        status = "Offen"
    elif abs(deviation) <= tolerance:
        status = "Gutschrift" if _is_fully_settled_by_credit(inv, deviation, tolerance) else "Bezahlt"
    elif deviation > tolerance and _matches_mahngebuehr(inv, deviation, tolerance, fee_1, fee_2, fee_3):
        status = "Bezahlt mit Mahngebühr"
    elif deviation > tolerance:
        status = "Überzahlung"
    else:
        status = "Teiloffen/Unterzahlung"
    return status, deviation


def update_all():
    """Recompute status and deviation_eur for every invoice in the DB."""
    params = load_params()
    tolerance = _parse_float_param(params.get("Toleranz", 0.001), 0.001)
    fee_1 = max(
        0.0,
        _parse_float_param(
            params.get("mahngebuehr_1_eur", params.get("mahngebuehr_eur", 0.0)),
            0.0,
        ),
    )
    fee_2 = max(0.0, _parse_float_param(params.get("mahngebuehr_2_eur", fee_1), fee_1))
    fee_3 = max(0.0, _parse_float_param(params.get("mahngebuehr_3_eur", fee_2), fee_2))
    
    from src.db import get_db
    conn = get_db()

    has_document_type = _has_column(conn, "invoices", "document_type")
    has_credit_target = _has_column(conn, "invoices", "credit_target_invoice_id")

    # Rebuild aggregates from matched payments (+ linked credit notes if available).
    if has_document_type and has_credit_target:
        conn.execute(
            """
            UPDATE invoices
            SET paid_sum_eur = CASE
                WHEN COALESCE(invoices.document_type, 'rechnung') = 'rechnung' THEN (
                    SELECT COALESCE(SUM(amount_eur), 0)
                    FROM payments
                    WHERE payments.invoice_id = invoices.invoice_id
                      AND payments.matched = 1
                ) + (
                    SELECT COALESCE(SUM(amount_gross), 0)
                    FROM invoices credits
                    WHERE COALESCE(credits.document_type, 'rechnung') = 'gutschrift'
                      AND credits.credit_target_invoice_id = invoices.invoice_id
                )
                ELSE 0
            END,
                payment_count = CASE
                WHEN COALESCE(invoices.document_type, 'rechnung') = 'rechnung' THEN (
                    SELECT COUNT(*)
                    FROM payments
                    WHERE payments.invoice_id = invoices.invoice_id
                      AND payments.matched = 1
                ) + (
                    SELECT COUNT(*)
                    FROM invoices credits
                    WHERE COALESCE(credits.document_type, 'rechnung') = 'gutschrift'
                      AND credits.credit_target_invoice_id = invoices.invoice_id
                )
                ELSE 0
            END,
                last_payment_date = CASE
                WHEN COALESCE(invoices.document_type, 'rechnung') = 'rechnung' THEN (
                    SELECT MAX(dt) FROM (
                        SELECT COALESCE(value_date, booking_date, created_at) AS dt
                        FROM payments
                        WHERE payments.invoice_id = invoices.invoice_id
                          AND payments.matched = 1
                        UNION ALL
                        SELECT COALESCE(issue_date, updated_at, created_at) AS dt
                        FROM invoices credits
                        WHERE COALESCE(credits.document_type, 'rechnung') = 'gutschrift'
                          AND credits.credit_target_invoice_id = invoices.invoice_id
                    )
                )
                ELSE NULL
            END
            """
        )
    else:
        conn.execute(
            """
            UPDATE invoices
            SET paid_sum_eur = (
                SELECT COALESCE(SUM(amount_eur), 0)
                FROM payments
                WHERE payments.invoice_id = invoices.invoice_id
                  AND payments.matched = 1
            ),
                payment_count = (
                SELECT COUNT(*)
                FROM payments
                WHERE payments.invoice_id = invoices.invoice_id
                  AND payments.matched = 1
            ),
                last_payment_date = (
                SELECT MAX(COALESCE(value_date, booking_date, created_at))
                FROM payments
                WHERE payments.invoice_id = invoices.invoice_id
                  AND payments.matched = 1
            )
            """
        )
    conn.commit()

    credit_applied_by_invoice = {}
    if has_document_type and has_credit_target:
        for row in conn.execute(
            """
            SELECT credit_target_invoice_id AS invoice_id,
                   COALESCE(SUM(amount_gross), 0) AS credit_applied_eur
            FROM invoices
            WHERE COALESCE(document_type, 'rechnung') = 'gutschrift'
              AND credit_target_invoice_id IS NOT NULL
            GROUP BY credit_target_invoice_id
            """
        ).fetchall():
            credit_applied_by_invoice[int(row["invoice_id"])] = float(row["credit_applied_eur"] or 0.0)

    rows = conn.execute("SELECT * FROM invoices").fetchall()
    
    updated = 0
    for inv in rows:
        inv_data = dict(inv)
        inv_data["credit_applied_eur"] = credit_applied_by_invoice.get(int(inv["invoice_id"]), 0.0)
        status, dev = compute_status_row(inv_data, tolerance, fee_1, fee_2, fee_3)
        if int(inv["status_manual"] or 0) == 1:
            conn.execute(
                "UPDATE invoices SET deviation_eur = ? WHERE invoice_id = ?",
                (dev, inv["invoice_id"]),
            )
        else:
            conn.execute(
                "UPDATE invoices SET status = ?, deviation_eur = ? WHERE invoice_id = ?",
                (status, dev, inv["invoice_id"]),
            )
        updated += 1

    conn.commit()
    conn.close()
    return f"Status von {updated} Rechnungen aktualisiert.", True


if __name__ == "__main__":
    msg, _ = update_all()
    print(msg)
