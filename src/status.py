"""Status-Berechnung für invoices."""

import json
import sqlite3

DB = "rechnungsverwaltung.db"
PARAM_PATH = "parameters.json"


def load_params():
    with open(PARAM_PATH, encoding="utf-8") as f:
        return json.load(f)


def compute_status_row(inv, tolerance):
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
    params = load_params()
    tolerance = float(params.get("Toleranz", 0.001))
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("SELECT * FROM invoices").fetchall()
    for inv in rows:
        status, dev = compute_status_row(inv, tolerance)
        conn.execute("UPDATE invoices SET status=?, deviation_eur=? WHERE invoice_id=?", (status, dev, inv["invoice_id"]))

    conn.commit()
    conn.close()


if __name__ == "__main__":
    update_all()
    print("Status aktualisiert.")
