"""Mahnlogik für offene Rechnungen basierend auf issue_date.

Setzt reminder_status auf '1. Mahnung', '2. Mahnung' oder '3. Mahnung'
abhängig von den konfigurierten Fristen.

Rechnungen mit `reminder_manual = 1` werden nicht automatisch geändert.
"""

import json
from datetime import datetime

from src.db import PARAM_PATH, get_db, init_db
from src.reminders import advance_automatic_reminder


def load_params(path=PARAM_PATH):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def run_mahnung():
    """Evaluate reminder status for every unpaid invoice."""
    p = load_params()
    due_1 = int(p.get("due_days_1", 30))
    due_2 = int(p.get("due_days_2", 60))
    due_3 = int(p.get("due_days_3", max(due_2 + 30, 90)))
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
        if days >= due_3:
            advance_automatic_reminder(conn, inv, "3. Mahnung", today.isoformat())
        elif days >= due_2:
            advance_automatic_reminder(conn, inv, "2. Mahnung", today.isoformat())
        elif days >= due_1:
            advance_automatic_reminder(conn, inv, "1. Mahnung", today.isoformat())

    conn.commit()
    conn.close()


if __name__ == "__main__":
    run_mahnung()
    print("Mahnlauf durchgeführt.")
