"""Zentrale Datenbankverbindung für Rechnungsverwaltung."""

import os
import sys
import sqlite3
import json
import shutil


def _safe_copy(src_path, dst_path):
    """Copy file if source exists and destination does not exist yet."""
    if os.path.exists(src_path) and not os.path.exists(dst_path):
        shutil.copy2(src_path, dst_path)


# Determine data storage location
if getattr(sys, 'frozen', False):
    # Running as bundled executable (e.g., via PyInstaller)
    home_dir = os.path.expanduser("~")
    legacy_docs_dir = os.path.join(home_dir, "Documents", "Rechnungsverwaltung_Daten")
    # On macOS, Application Support is the safer writable app data path.
    if sys.platform == "darwin":
        user_data_dir = os.path.join(home_dir, "Library", "Application Support", "Rechnungsverwaltung_Daten")
    else:
        user_data_dir = legacy_docs_dir

    os.makedirs(user_data_dir, exist_ok=True)
    DB_PATH = os.path.join(user_data_dir, "rechnungsverwaltung.db")
    # PyInstaller extracts bundled files to sys._MEIPASS
    SCHEMA_PATH = os.path.join(sys._MEIPASS, "schema", "schema.sql")
    BUNDLED_PARAM_PATH = os.path.join(sys._MEIPASS, "parameters.json")
    PARAM_PATH = os.path.join(user_data_dir, "parameters.json")

    # One-time migration from legacy Documents path (used by older builds).
    legacy_db_path = os.path.join(legacy_docs_dir, "rechnungsverwaltung.db")
    legacy_param_path = os.path.join(legacy_docs_dir, "parameters.json")
    if user_data_dir != legacy_docs_dir:
        try:
            _safe_copy(legacy_db_path, DB_PATH)
            _safe_copy(legacy_param_path, PARAM_PATH)
        except Exception:
            pass

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
    path = os.path.abspath(db_path or DB_PATH)
    data_dir = os.path.dirname(path)
    if data_dir:
        os.makedirs(data_dir, exist_ok=True)
    conn = sqlite3.connect(path)
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
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS invoice_reminders (
          reminder_entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
          invoice_id INTEGER NOT NULL,
          reminder_status TEXT NOT NULL,
          reminder_date TEXT NOT NULL,
          manual_entry INTEGER DEFAULT 0,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP,
          updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
          FOREIGN KEY(invoice_id) REFERENCES invoices(invoice_id)
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_invoice_reminders_invoice_stage
          ON invoice_reminders(invoice_id, reminder_status);

        CREATE INDEX IF NOT EXISTS idx_invoice_reminders_invoice
          ON invoice_reminders(invoice_id);
        """
    )
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
