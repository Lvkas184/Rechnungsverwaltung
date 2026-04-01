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


def compute_status_row(inv, tolerance, mahngebuehr_eur=0.0, mahngebuehr_2_eur=None, mahngebuehr_3_eur=None):
    """Compute status and deviation for a single invoice dict/Row."""
    invoice_id = None
    try:
        invoice_id = inv["invoice_id"]
    except (TypeError, KeyError, IndexError):
        if isinstance(inv, dict):
            invoice_id = inv.get("invoice_id")

    fee_1 = max(0.0, _parse_float_param(mahngebuehr_eur, 0.0))
    fee_2 = max(0.0, _parse_float_param(mahngebuehr_2_eur, fee_1)) if mahngebuehr_2_eur is not None else fee_1
    fee_3 = max(0.0, _parse_float_param(mahngebuehr_3_eur, fee_2)) if mahngebuehr_3_eur is not None else fee_2
    paid = inv["paid_sum_eur"] or 0.0
    amount = inv["amount_gross"] or 0.0
    deviation = paid - amount
    special_status = classify_special_invoice_status(invoice_id)
    if special_status:
        status = special_status
    elif paid == 0:
        status = "Offen"
    elif abs(deviation) <= tolerance:
        status = "Bezahlt"
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
    
    # Rebuild payment aggregates from matched payments.
    conn.execute("""
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
    """)
    conn.commit()

    rows = conn.execute("SELECT * FROM invoices").fetchall()
    
    updated = 0
    for inv in rows:
        status, dev = compute_status_row(inv, tolerance, fee_1, fee_2, fee_3)
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
