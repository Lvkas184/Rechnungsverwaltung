"""Helper fuer mehrstufige Mahnverlaeufe je Rechnung."""

from datetime import datetime


REMINDER_STAGES = ("1. Mahnung", "2. Mahnung", "3. Mahnung")
REMINDER_STAGE_ORDER = {status: idx for idx, status in enumerate(REMINDER_STAGES, start=1)}


def reminder_stage_number(reminder_status):
    """Return 1/2/3 for known reminder labels, else 0."""
    return REMINDER_STAGE_ORDER.get(str(reminder_status or "").strip(), 0)


def _row_value(row, key, default=None):
    try:
        return row[key]
    except (TypeError, KeyError, IndexError):
        if isinstance(row, dict):
            return row.get(key, default)
    return default


def _current_invoice_reminder_row(invoice_row):
    reminder_status = _row_value(invoice_row, "reminder_status")
    reminder_date = _row_value(invoice_row, "reminder_date")
    if not reminder_status or not reminder_date:
        return None
    return {
        "reminder_entry_id": None,
        "invoice_id": _row_value(invoice_row, "invoice_id"),
        "reminder_status": reminder_status,
        "reminder_date": reminder_date,
        "manual_entry": int(_row_value(invoice_row, "reminder_manual", 0) or 0),
        "is_legacy": 1,
    }


def _sorted_history(rows):
    return sorted(
        rows,
        key=lambda row: (
            reminder_stage_number(_row_value(row, "reminder_status")),
            str(_row_value(row, "reminder_date") or ""),
            int(_row_value(row, "reminder_entry_id", 0) or 0),
        ),
    )


def fetch_invoice_reminder_history(conn, invoice_id, invoice_row=None):
    """Return reminder history for an invoice in ascending stage order."""
    rows = conn.execute(
        """
        SELECT reminder_entry_id, invoice_id, reminder_status, reminder_date, manual_entry
        FROM invoice_reminders
        WHERE invoice_id = ?
        """,
        (invoice_id,),
    ).fetchall()
    if rows:
        return _sorted_history(rows)

    if invoice_row is None:
        invoice_row = conn.execute(
            """
            SELECT invoice_id, reminder_status, reminder_date, reminder_manual
            FROM invoices
            WHERE invoice_id = ?
            """,
            (invoice_id,),
        ).fetchone()
    fallback = _current_invoice_reminder_row(invoice_row)
    return [fallback] if fallback else []


def _backfill_current_invoice_reminder(conn, invoice_row):
    """Persist existing invoice reminder fields as first history row if needed."""
    if invoice_row is None:
        return
    invoice_id = _row_value(invoice_row, "invoice_id")
    if not invoice_id:
        return
    existing = conn.execute(
        "SELECT COUNT(*) FROM invoice_reminders WHERE invoice_id = ?",
        (invoice_id,),
    ).fetchone()[0]
    if existing:
        return

    current = _current_invoice_reminder_row(invoice_row)
    if not current:
        return

    conn.execute(
        """
        INSERT INTO invoice_reminders(invoice_id, reminder_status, reminder_date, manual_entry, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            invoice_id,
            current["reminder_status"],
            current["reminder_date"],
            current["manual_entry"],
            datetime.utcnow().isoformat(),
            datetime.utcnow().isoformat(),
        ),
    )


def _latest_history_row(conn, invoice_id, invoice_row=None):
    history = fetch_invoice_reminder_history(conn, invoice_id, invoice_row=invoice_row)
    if not history:
        return None
    return max(
        history,
        key=lambda row: (
            reminder_stage_number(_row_value(row, "reminder_status")),
            str(_row_value(row, "reminder_date") or ""),
            int(_row_value(row, "reminder_entry_id", 0) or 0),
        ),
    )


def clear_invoice_reminders(conn, invoice_id, manual_override=None):
    """Delete full reminder history and clear current invoice reminder fields."""
    now = datetime.utcnow().isoformat()
    conn.execute("DELETE FROM invoice_reminders WHERE invoice_id = ?", (invoice_id,))

    query = """
        UPDATE invoices
        SET reminder_status = NULL,
            reminder_date = NULL,
            updated_at = ?
    """
    params = [now]
    if manual_override is not None:
        query += ", reminder_manual = ?"
        params.append(1 if manual_override else 0)
    query += " WHERE invoice_id = ?"
    params.append(invoice_id)
    conn.execute(query, params)


def save_invoice_reminder(
    conn,
    invoice_id,
    reminder_status,
    reminder_date,
    *,
    manual_entry=0,
    manual_override=None,
):
    """Insert or update one Mahnstufe and sync latest state onto invoices."""
    reminder_status = str(reminder_status or "").strip()
    if not reminder_status:
        clear_invoice_reminders(conn, invoice_id, manual_override=manual_override)
        return None

    invoice_row = conn.execute(
        """
        SELECT invoice_id, reminder_status, reminder_date, reminder_manual
        FROM invoices
        WHERE invoice_id = ?
        """,
        (invoice_id,),
    ).fetchone()
    _backfill_current_invoice_reminder(conn, invoice_row)

    now = datetime.utcnow().isoformat()
    existing = conn.execute(
        """
        SELECT reminder_entry_id
        FROM invoice_reminders
        WHERE invoice_id = ? AND reminder_status = ?
        """,
        (invoice_id, reminder_status),
    ).fetchone()
    if existing:
        conn.execute(
            """
            UPDATE invoice_reminders
            SET reminder_date = ?,
                manual_entry = ?,
                updated_at = ?
            WHERE reminder_entry_id = ?
            """,
            (reminder_date, int(manual_entry or 0), now, existing["reminder_entry_id"]),
        )
    else:
        conn.execute(
            """
            INSERT INTO invoice_reminders(invoice_id, reminder_status, reminder_date, manual_entry, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (invoice_id, reminder_status, reminder_date, int(manual_entry or 0), now, now),
        )

    latest = _latest_history_row(conn, invoice_id)
    query = """
        UPDATE invoices
        SET reminder_status = ?,
            reminder_date = ?,
            updated_at = ?
    """
    params = [
        _row_value(latest, "reminder_status"),
        _row_value(latest, "reminder_date"),
        now,
    ]
    if manual_override is not None:
        query += ", reminder_manual = ?"
        params.append(1 if manual_override else 0)
    query += " WHERE invoice_id = ?"
    params.append(invoice_id)
    conn.execute(query, params)
    return latest


def advance_automatic_reminder(conn, invoice_row, target_status, reminder_date):
    """Append only when the automatic Mahnlauf reaches a higher stage."""
    invoice_id = _row_value(invoice_row, "invoice_id")
    if not invoice_id or not target_status:
        return None

    _backfill_current_invoice_reminder(conn, invoice_row)
    current = _latest_history_row(conn, invoice_id, invoice_row=invoice_row)
    if reminder_stage_number(_row_value(current, "reminder_status")) >= reminder_stage_number(target_status):
        return current

    return save_invoice_reminder(
        conn,
        invoice_id,
        target_status,
        reminder_date,
        manual_entry=0,
        manual_override=0,
    )
