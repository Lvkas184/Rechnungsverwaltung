"""Zentrale Datenbankverbindung für Rechnungsverwaltung."""

import os
import sys
import sqlite3

# Determine data storage location
if getattr(sys, 'frozen', False):
    # Running as bundled executable (e.g., via PyInstaller)
    # Store database in the user's Documents folder so it persists across updates
    USER_DOCS = os.path.join(os.path.expanduser("~"), "Documents", "Rechnungsverwaltung_Daten")
    os.makedirs(USER_DOCS, exist_ok=True)
    DB_PATH = os.path.join(USER_DOCS, "rechnungsverwaltung.db")
    # PyInstaller extracts bundled files to sys._MEIPASS
    SCHEMA_PATH = os.path.join(sys._MEIPASS, "schema", "schema.sql")
    PARAM_PATH = os.path.join(sys._MEIPASS, "parameters.json")
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
    conn.commit()
    conn.close()


def query_db(query, args=(), one=False, db_path=None):
    """Execute a query and return results as list of Row objects."""
    conn = get_db(db_path)
    cur = conn.execute(query, args)
    rv = cur.fetchall()
    conn.close()
    return (rv[0] if rv else None) if one else rv
