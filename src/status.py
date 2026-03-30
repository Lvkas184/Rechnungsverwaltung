"""Status-Berechnung für invoices.

Statuses:
- Offen           (paid_sum == 0)
- Bezahlt         (Abweichung innerhalb Toleranz)
- Überzahlung     (paid_sum > amount_gross + Toleranz)
- Teiloffen/Unterzahlung  (sonst)
"""

import json
import sqlite3

from src.db import PARAM_PATH


def load_params(path=PARAM_PATH):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def compute_status_row(inv, tolerance):
    """Compute status and deviation for a single invoice dict/Row."""
    paid = inv["paid_sum_eur"] or 0.0
    amount = inv["amount_gross"] or 0.0
    deviation = paid - amount
    if paid == 0:
        status = "Offen"
    elif abs(deviation) <= tolerance:
        status = "Bezahlt"
    elif deviation > tolerance:
        status = "Überzahlung"
    else:
        status = "Teiloffen/Unterzahlung"
    return status, deviation


def update_all():
    """Recompute status and deviation_eur for every invoice in the DB."""
    params = load_params()
    tolerance = float(params.get("Toleranz", 0.001))
    
    from src.db import get_db
    conn = get_db()
    
    # Ensure paid_sum_eur is accurately computed from all currently matched payments
    conn.execute("""
        UPDATE invoices
        SET paid_sum_eur = (
            SELECT COALESCE(SUM(amount_eur), 0)
            FROM payments
            WHERE payments.invoice_id = invoices.invoice_id
              AND payments.matched = 1
        )
    """)
    conn.commit()

    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM invoices").fetchall()
    
    updated = 0
    for inv in rows:
        status, dev = compute_status_row(inv, tolerance)
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
