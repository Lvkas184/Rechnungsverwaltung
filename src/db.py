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
    _ensure_column(
        conn,
        "invoices",
        "document_type",
        "document_type TEXT NOT NULL DEFAULT 'rechnung'",
    )
    _ensure_column(
        conn,
        "invoices",
        "credit_target_invoice_id",
        "credit_target_invoice_id INTEGER",
    )
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
        CREATE INDEX IF NOT EXISTS idx_invoices_document_type
          ON invoices(document_type);

        CREATE INDEX IF NOT EXISTS idx_invoices_credit_target
          ON invoices(credit_target_invoice_id);

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

        CREATE TABLE IF NOT EXISTS manual_change_log (
          change_id INTEGER PRIMARY KEY AUTOINCREMENT,
          source_audit_id INTEGER UNIQUE,
          entry_origin TEXT NOT NULL DEFAULT 'auto',
          is_resolved INTEGER DEFAULT 0,
          resolved_at TEXT,
          change_scope TEXT NOT NULL,
          invoice_id INTEGER,
          payment_id INTEGER,
          action_code TEXT NOT NULL,
          action_label TEXT NOT NULL,
          before_value TEXT,
          after_value TEXT,
          note TEXT,
          changed_by TEXT,
          changed_at TEXT DEFAULT CURRENT_TIMESTAMP,
          FOREIGN KEY(invoice_id) REFERENCES invoices(invoice_id),
          FOREIGN KEY(payment_id) REFERENCES payments(payment_id),
          FOREIGN KEY(source_audit_id) REFERENCES audit_log(audit_id)
        );

        CREATE INDEX IF NOT EXISTS idx_manual_change_log_changed_at
          ON manual_change_log(changed_at);

        CREATE INDEX IF NOT EXISTS idx_manual_change_log_invoice
          ON manual_change_log(invoice_id);

        CREATE INDEX IF NOT EXISTS idx_manual_change_log_payment
          ON manual_change_log(payment_id);
        """
    )
    _ensure_column(conn, "manual_change_log", "source_audit_id", "source_audit_id INTEGER")
    _ensure_column(conn, "manual_change_log", "entry_origin", "entry_origin TEXT NOT NULL DEFAULT 'auto'")
    _ensure_column(conn, "manual_change_log", "is_resolved", "is_resolved INTEGER DEFAULT 0")
    _ensure_column(conn, "manual_change_log", "resolved_at", "resolved_at TEXT")
    conn.execute(
        """
        UPDATE manual_change_log
        SET entry_origin = 'auto'
        WHERE entry_origin IS NULL OR TRIM(entry_origin) = ''
        """
    )
    conn.execute(
        """
        UPDATE manual_change_log
        SET entry_origin = CASE
            WHEN LOWER(TRIM(entry_origin)) IN ('manual', 'manuell') THEN 'manual'
            ELSE 'auto'
        END
        """
    )
    conn.execute(
        """
        UPDATE manual_change_log
        SET is_resolved = CASE
            WHEN COALESCE(is_resolved, 0) <> 0 THEN 1
            ELSE 0
        END
        """
    )
    _backfill_invoice_document_type(conn)
    _backfill_special_payment_flags(conn)
    _backfill_manual_change_log_from_audit(conn)


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


def _backfill_invoice_document_type(conn):
    """Ensure invoices have a normalized document_type value."""
    rows = conn.execute(
        """
        SELECT invoice_id,
               COALESCE(TRIM(document_type), '') AS document_type,
               COALESCE(TRIM(status), '') AS status
        FROM invoices
        """
    ).fetchall()
    for row in rows:
        current_doc_type = str(row["document_type"] or "").strip().lower()
        status_label = str(row["status"] or "").strip().lower()
        target_doc_type = "gutschrift" if status_label == "gutschrift" else "rechnung"
        if current_doc_type in ("rechnung", "gutschrift"):
            target_doc_type = current_doc_type
        if target_doc_type != current_doc_type:
            conn.execute(
                "UPDATE invoices SET document_type = ? WHERE invoice_id = ?",
                (target_doc_type, row["invoice_id"]),
            )


def _backfill_manual_change_log_from_audit(conn):
    """Seed manual change log from existing manual audit entries (idempotent)."""
    conn.executescript(
        """
        INSERT INTO manual_change_log(
            source_audit_id,
            change_scope,
            invoice_id,
            payment_id,
            action_code,
            action_label,
            changed_by,
            changed_at
        )
        SELECT
            a.audit_id,
            'payment',
            a.invoice_id,
            a.payment_id,
            COALESCE(NULLIF(TRIM(a.rule_used), ''), 'manual_audit'),
            CASE
                WHEN COALESCE(a.rule_used, '') = 'manual_single' THEN 'Zahlung manuell zugeordnet'
                WHEN COALESCE(a.rule_used, '') = 'manual_split' THEN 'Zahlung manuell aufgeteilt'
                WHEN COALESCE(a.rule_used, '') = 'manual_split_child' THEN 'Split-Teilzahlung manuell erstellt'
                WHEN COALESCE(a.rule_used, '') = 'manual_unassigned' THEN 'Zahlungszuordnung manuell entfernt'
                ELSE 'Manuelle Zahlungsänderung (Audit)'
            END,
            COALESCE(NULLIF(TRIM(a.user), ''), 'manual'),
            COALESCE(NULLIF(TRIM(a.ts), ''), CURRENT_TIMESTAMP)
        FROM audit_log a
        WHERE COALESCE(a.automated, 1) = 0
          AND COALESCE(a.rule_used, '') NOT IN (
                'manual_single',
                'manual_split',
                'manual_split_child',
                'manual_unassigned'
          )
          AND NOT EXISTS (
                SELECT 1
                FROM manual_change_log m
                WHERE m.source_audit_id = a.audit_id
          );
        """
    )


def query_db(query, args=(), one=False, db_path=None):
    """Execute a query and return results as list of Row objects."""
    conn = get_db(db_path)
    cur = conn.execute(query, args)
    rv = cur.fetchall()
    conn.close()
    return (rv[0] if rv else None) if one else rv
