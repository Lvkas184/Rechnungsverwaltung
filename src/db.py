"""Zentrale Datenbankverbindung für Rechnungsverwaltung."""

import os
import sys
import sqlite3
import json
import shutil

# Determine data storage location
if getattr(sys, 'frozen', False):
    # Running as bundled executable (e.g., via PyInstaller)
    # Store database in the user's Documents folder so it persists across updates
    USER_DOCS = os.path.join(os.path.expanduser("~"), "Documents", "Rechnungsverwaltung_Daten")
    os.makedirs(USER_DOCS, exist_ok=True)
    DB_PATH = os.path.join(USER_DOCS, "rechnungsverwaltung.db")
    # PyInstaller extracts bundled files to sys._MEIPASS
    SCHEMA_PATH = os.path.join(sys._MEIPASS, "schema", "schema.sql")
    BUNDLED_PARAM_PATH = os.path.join(sys._MEIPASS, "parameters.json")
    PARAM_PATH = os.path.join(USER_DOCS, "parameters.json")
    if not os.path.exists(PARAM_PATH):
        try:
            shutil.copy2(BUNDLED_PARAM_PATH, PARAM_PATH)
        except Exception:
            with open(PARAM_PATH, "w", encoding="utf-8") as f:
                json.dump({}, f)
else:
    # Running from source code
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    DB_PATH = os.environ.get("RECHNUNGSVERWALTUNG_DB", os.path.join(BASE_DIR, "rechnungsverwaltung.db"))
    SCHEMA_PATH = os.path.join(BASE_DIR, "schema", "schema.sql")
    PARAM_PATH = os.path.join(BASE_DIR, "parameters.json")


def get_db(db_path=None):
    """Return a new SQLite connection with Row factory enabled."""
    conn = sqlite3.connect(db_path or DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path=None):
    """Create all tables from schema.sql if they don't exist."""
    conn = get_db(db_path)
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        conn.executescript(f.read())
    _run_lightweight_migrations(conn)
    conn.commit()
    conn.close()


def _ensure_column(conn, table_name, column_name, column_sql):
    """Add a column if missing (safe for repeated startup execution)."""
    cols = {row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})")}
    if column_name not in cols:
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")


def _run_lightweight_migrations(conn):
    """Apply additive schema migrations for older local databases."""
    _ensure_column(conn, "invoices", "remark", "remark TEXT")
    _ensure_column(conn, "invoices", "status_manual", "status_manual INTEGER DEFAULT 0")
    _ensure_column(conn, "invoices", "reminder_status", "reminder_status TEXT")
    _ensure_column(conn, "invoices", "reminder_date", "reminder_date TEXT")
    _ensure_column(conn, "invoices", "reminder_manual", "reminder_manual INTEGER DEFAULT 0")
    _ensure_column(conn, "payments", "parent_payment_id", "parent_payment_id INTEGER")
    _ensure_column(conn, "payments", "remark", "remark TEXT")
    _ensure_column(conn, "payments", "akonto", "akonto INTEGER DEFAULT 0")
    _ensure_column(conn, "payments", "schadensrechnung", "schadensrechnung INTEGER DEFAULT 0")
    _ensure_column(conn, "payments", "status_manual", "status_manual INTEGER DEFAULT 0")
    _ensure_column(conn, "payments", "status_override", "status_override TEXT")
    _backfill_special_payment_flags(conn)


def _backfill_special_payment_flags(conn):
    """Classify existing payments as Akonto/Schadensrechnung from text/id."""
    try:
        from src.payment_rules import is_akonto_payment, is_schadensrechnung_payment
    except Exception:
        return

    rows = conn.execute(
        "SELECT payment_id, invoice_id, reference_text, akonto, schadensrechnung FROM payments"
    ).fetchall()
    for row in rows:
        akonto_flag = 1 if is_akonto_payment(row["reference_text"], row["invoice_id"]) else 0
        schadens_flag = (
            1 if is_schadensrechnung_payment(row["reference_text"], row["invoice_id"]) else 0
        )
        if int(row["akonto"] or 0) != akonto_flag or int(row["schadensrechnung"] or 0) != schadens_flag:
            conn.execute(
                "UPDATE payments SET akonto = ?, schadensrechnung = ? WHERE payment_id = ?",
                (akonto_flag, schadens_flag, row["payment_id"]),
            )


def query_db(query, args=(), one=False, db_path=None):
    """Execute a query and return results as list of Row objects."""
    conn = get_db(db_path)
    cur = conn.execute(query, args)
    rv = cur.fetchall()
    conn.close()
    return (rv[0] if rv else None) if one else rv
