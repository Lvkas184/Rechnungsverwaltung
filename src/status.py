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


def compute_status_row(inv, tolerance, mahngebuehr_eur=0.0):
    """Compute status and deviation for a single invoice dict/Row."""
    invoice_id = None
    try:
        invoice_id = inv["invoice_id"]
    except (TypeError, KeyError):
        if isinstance(inv, dict):
            invoice_id = inv.get("invoice_id")

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
    elif deviation > tolerance and mahngebuehr_eur > 0 and abs(deviation - mahngebuehr_eur) <= tolerance:
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
    mahngebuehr_eur = max(0.0, _parse_float_param(params.get("mahngebuehr_eur", 0.0), 0.0))
    
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
        status, dev = compute_status_row(inv, tolerance, mahngebuehr_eur)
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
