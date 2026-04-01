"""Mahnlogik für offene Rechnungen basierend auf issue_date.

Setzt reminder_status auf '1. Mahnung' oder '2. Mahnung' abhängig
von den konfigurierten Fristen.

Rechnungen mit `reminder_manual = 1` werden nicht automatisch geändert.
"""

import json
from datetime import datetime

from src.db import PARAM_PATH, get_db, init_db


def load_params(path=PARAM_PATH):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def run_mahnung():
    """Evaluate reminder status for every unpaid invoice."""
    p = load_params()
    due_1 = int(p.get("due_days_1", 30))
    due_2 = int(p.get("due_days_2", 60))
    today = datetime.utcnow().date()

    init_db()
    conn = get_db()
    rows = conn.execute("SELECT * FROM invoices").fetchall()

    for inv in rows:
        issue_date = inv["issue_date"]
        if not issue_date:
            continue
        try:
            issue = datetime.fromisoformat(issue_date).date()
        except (ValueError, TypeError):
            continue

        if inv["status"] in ("Bezahlt", "Bezahlt mit Mahngebühr"):
            continue

        if int(inv["reminder_manual"] or 0) == 1:
            continue

        days = (today - issue).days
        if days >= due_2:
            conn.execute(
                "UPDATE invoices SET reminder_status = ?, reminder_date = ? WHERE invoice_id = ?",
                ("2. Mahnung", today.isoformat(), inv["invoice_id"]),
            )
        elif days >= due_1:
            conn.execute(
                "UPDATE invoices SET reminder_status = ?, reminder_date = ? WHERE invoice_id = ?",
                ("1. Mahnung", today.isoformat(), inv["invoice_id"]),
            )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    run_mahnung()
    print("Mahnlauf durchgeführt.")
