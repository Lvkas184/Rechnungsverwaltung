import json
import sqlite3
from datetime import datetime

DB = "rechnungsverwaltung.db"


def load_params(path="parameters.json"):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def run_mahnung():
    p = load_params()
    due_1 = int(p.get("due_days_1", 30))
    due_2 = int(p.get("due_days_2", 60))
    today = datetime.utcnow().date()

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM invoices").fetchall()

    for inv in rows:
        issue_date = inv["issue_date"]
        if not issue_date:
            continue
        try:
            issue = datetime.fromisoformat(issue_date).date()
        except ValueError:
            continue

        if inv["status"] == "Bezahlt":
            continue

        days = (today - issue).days
        if days > due_2:
            conn.execute(
                "UPDATE invoices SET reminder_status = ?, reminder_date = ? WHERE invoice_id = ?",
                ("2. Mahnung", today.isoformat(), inv["invoice_id"]),
            )
        elif days > due_1:
            conn.execute(
                "UPDATE invoices SET reminder_status = ?, reminder_date = ? WHERE invoice_id = ?",
                ("1. Mahnung", today.isoformat(), inv["invoice_id"]),
            )

    conn.commit()
    conn.close()
