"""Flask Web-App für Rechnungsverwaltung."""

import os
import sqlite3
from datetime import datetime

from flask import Flask, flash, redirect, render_template, request, url_for

from src.csv_import import (
    import_datev_rechnungen,
    import_sparkasse_csv,
    import_voba_kraichgau_csv,
    import_voba_pur_csv,
)
from src.db import DB_PATH, get_db, init_db
from src.mahnung import run_mahnung
from src.matching import apply_matching
from src.status import update_all

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "rechnungsverwaltung-secret-key-change-me")

# Ensure uploads directory exists
UPLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)


# ---------------------------------------------------------------------------
# Startup: ensure DB tables exist
# ---------------------------------------------------------------------------

@app.before_request
def _ensure_db():
    if not hasattr(app, "_db_initialized"):
        init_db()
        app._db_initialized = True


# ---------------------------------------------------------------------------
# Template helpers
# ---------------------------------------------------------------------------

@app.template_filter("format_eur")
def format_eur(value):
    """Format a number as Euro currency."""
    if value is None:
        return "—"
    try:
        return f"{float(value):,.2f} €".replace(",", "X").replace(".", ",").replace("X", ".")
    except (ValueError, TypeError):
        return str(value)


@app.template_filter("format_date")
def format_date(value):
    """Format ISO date to German format."""
    if not value:
        return "—"
    try:
        d = datetime.fromisoformat(str(value))
        return d.strftime("%d.%m.%Y")
    except (ValueError, TypeError):
        return str(value)


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

@app.route("/")
def dashboard():
    conn = get_db()
    stats = {
        "total_invoices": conn.execute("SELECT COUNT(*) FROM invoices").fetchone()[0],
        "open_invoices": conn.execute("SELECT COUNT(*) FROM invoices WHERE COALESCE(status, 'Offen') = 'Offen'").fetchone()[0],
        "partial_invoices": conn.execute("SELECT COUNT(*) FROM invoices WHERE status = 'Teiloffen/Unterzahlung'").fetchone()[0],
        "paid_invoices": conn.execute("SELECT COUNT(*) FROM invoices WHERE status = 'Bezahlt'").fetchone()[0],
        "overpaid_invoices": conn.execute("SELECT COUNT(*) FROM invoices WHERE status = 'Überzahlung'").fetchone()[0],
        "total_payments": conn.execute("SELECT COUNT(*) FROM payments").fetchone()[0],
        "matched_payments": conn.execute("SELECT COUNT(*) FROM payments WHERE matched = 1").fetchone()[0],
        "unmatched_payments": conn.execute("SELECT COUNT(*) FROM payments WHERE COALESCE(matched, 0) = 0").fetchone()[0],
        "mahnung_1": conn.execute("SELECT COUNT(*) FROM invoices WHERE reminder_status = '1. Mahnung'").fetchone()[0],
        "mahnung_2": conn.execute("SELECT COUNT(*) FROM invoices WHERE reminder_status = '2. Mahnung'").fetchone()[0],
        "open_sum": conn.execute("SELECT COALESCE(SUM(amount_gross - COALESCE(paid_sum_eur, 0)), 0) FROM invoices WHERE COALESCE(status, 'Offen') != 'Bezahlt'").fetchone()[0],
    }
    # Recent audit entries
    recent_audit = conn.execute(
        "SELECT * FROM audit_log ORDER BY audit_id DESC LIMIT 10"
    ).fetchall()
    conn.close()
    return render_template("dashboard.html", stats=stats, recent_audit=recent_audit)


# ---------------------------------------------------------------------------
# Rechnungen (Invoices)
# ---------------------------------------------------------------------------

@app.route("/rechnungen")
def rechnungen():
    conn = get_db()
    status_filter = request.args.get("status", "")
    search = request.args.get("q", "").strip()
    page = max(1, int(request.args.get("page", 1)))
    sort_col = request.args.get("sort", "invoice_id")
    order = request.args.get("order", "desc").lower()
    per_page = 50

    # Validate sorting
    valid_cols = ["invoice_id", "name", "amount_gross", "paid_sum_eur", "deviation_eur", "status", "reminder_status"]
    if sort_col not in valid_cols:
        sort_col = "invoice_id"
    if order not in ["asc", "desc"]:
        order = "desc"

    query = "SELECT * FROM invoices WHERE 1=1"
    params = []
    if status_filter:
        query += " AND status = ?"
        params.append(status_filter)
    if search:
        query += " AND (CAST(invoice_id AS TEXT) LIKE ? OR name LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])
    query += f" ORDER BY {sort_col} {order.upper()} LIMIT ? OFFSET ?"
    params.extend([per_page, (page - 1) * per_page])

    invoices = conn.execute(query, params).fetchall()

    # Count total for pagination
    count_query = "SELECT COUNT(*) FROM invoices WHERE 1=1"
    count_params = []
    if status_filter:
        count_query += " AND status = ?"
        count_params.append(status_filter)
    if search:
        count_query += " AND (CAST(invoice_id AS TEXT) LIKE ? OR name LIKE ?)"
        count_params.extend([f"%{search}%", f"%{search}%"])
    total = conn.execute(count_query, count_params).fetchone()[0]
    conn.close()

    total_pages = max(1, (total + per_page - 1) // per_page)
    return render_template("rechnungen.html", invoices=invoices, page=page,
                           total_pages=total_pages, total=total,
                           status_filter=status_filter, search=search,
                           sort_col=sort_col, order=order)


@app.route("/rechnungen/<int:invoice_id>")
def rechnung_detail(invoice_id):
    conn = get_db()
    inv = conn.execute("SELECT * FROM invoices WHERE invoice_id = ?", (invoice_id,)).fetchone()
    if not inv:
        conn.close()
        flash("Rechnung nicht gefunden.", "error")
        return redirect(url_for("rechnungen"))
    payments = conn.execute(
        "SELECT * FROM payments WHERE invoice_id = ? ORDER BY booking_date DESC", (invoice_id,)
    ).fetchall()
    audit = conn.execute(
        "SELECT * FROM audit_log WHERE invoice_id = ? ORDER BY audit_id DESC", (invoice_id,)
    ).fetchall()
    conn.close()
    return render_template("rechnung_detail.html", inv=inv, payments=payments, audit=audit)


# ---------------------------------------------------------------------------
# Zahlungen (Payments)
# ---------------------------------------------------------------------------

@app.route("/zahlungen")
def zahlungen():
    conn = get_db()
    filter_type = request.args.get("filter", "")
    search = request.args.get("q", "").strip()
    page = max(1, int(request.args.get("page", 1)))
    sort_col = request.args.get("sort", "payment_id")
    order = request.args.get("order", "desc").lower()
    per_page = 50

    valid_cols = ["payment_id", "source", "booking_date", "amount_eur", "beneficiary_name", "reference_text", "match_score", "invoice_id", "matched"]
    if sort_col not in valid_cols:
        sort_col = "payment_id"
    if order not in ["asc", "desc"]:
        order = "desc"

    query = "SELECT * FROM payments WHERE 1=1"
    params = []
    if filter_type == "matched":
        query += " AND matched = 1"
    elif filter_type == "unmatched":
        query += " AND COALESCE(matched, 0) = 0"
    elif filter_type == "review":
        query += " AND COALESCE(matched, 0) = 0 AND match_score IS NOT NULL AND match_score > 0"
    if search:
        query += " AND (reference_text LIKE ? OR beneficiary_name LIKE ? OR CAST(invoice_id AS TEXT) LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])
    query += f" ORDER BY {sort_col} {order.upper()} LIMIT ? OFFSET ?"
    params.extend([per_page, (page - 1) * per_page])

    payments = conn.execute(query, params).fetchall()

    count_query = "SELECT COUNT(*) FROM payments WHERE 1=1"
    count_params = []
    if filter_type == "matched":
        count_query += " AND matched = 1"
    elif filter_type == "unmatched":
        count_query += " AND COALESCE(matched, 0) = 0"
    elif filter_type == "review":
        count_query += " AND COALESCE(matched, 0) = 0 AND match_score IS NOT NULL AND match_score > 0"
    if search:
        count_query += " AND (reference_text LIKE ? OR beneficiary_name LIKE ? OR CAST(invoice_id AS TEXT) LIKE ?)"
        count_params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])
    total = conn.execute(count_query, count_params).fetchone()[0]
    conn.close()

    total_pages = max(1, (total + per_page - 1) // per_page)
    return render_template("zahlungen.html", payments=payments, page=page,
                           total_pages=total_pages, total=total,
                           filter_type=filter_type, search=search,
                           sort_col=sort_col, order=order)


@app.route("/zahlungen/<int:payment_id>")
def zahlung_detail(payment_id):
    conn = get_db()
    pay = conn.execute("SELECT * FROM payments WHERE payment_id = ?", (payment_id,)).fetchone()
    if not pay:
        conn.close()
        flash("Zahlung nicht gefunden.", "error")
        return redirect(url_for("zahlungen"))
    
    # If matched, fetch the corresponding invoice details
    inv = None
    if pay["invoice_id"]:
        inv = conn.execute("SELECT * FROM invoices WHERE invoice_id = ?", (pay["invoice_id"],)).fetchone()

    audit = conn.execute(
        "SELECT * FROM audit_log WHERE payment_id = ? ORDER BY audit_id DESC", (payment_id,)
    ).fetchall()
    conn.close()
    return render_template("zahlung_detail.html", pay=pay, inv=inv, audit=audit)



# ---------------------------------------------------------------------------
# CSV Upload
# ---------------------------------------------------------------------------

@app.route("/upload")
def upload():
    return render_template("upload.html")


@app.route("/upload/rechnungen", methods=["POST"])
def upload_rechnungen():
    f = request.files.get("file")
    if not f or not f.filename:
        flash("Keine Datei ausgewählt.", "error")
        return redirect(url_for("upload"))
    result = import_datev_rechnungen(f.read())
    if result["error"]:
        flash(f"Fehler: {result['error']}", "error")
    else:
        flash(f"✅ {result['imported']} Rechnungen importiert, {result['skipped']} übersprungen.", "success")
    return redirect(url_for("upload"))


@app.route("/upload/bank/<bank>", methods=["POST"])
def upload_bank(bank):
    f = request.files.get("file")
    if not f or not f.filename:
        flash("Keine Datei ausgewählt.", "error")
        return redirect(url_for("upload"))

    importers = {
        "sparkasse": import_sparkasse_csv,
        "voba_kraichgau": import_voba_kraichgau_csv,
        "voba_pur": import_voba_pur_csv,
    }
    importer = importers.get(bank)
    if not importer:
        flash(f"Unbekannte Bank: {bank}", "error")
        return redirect(url_for("upload"))

    result = importer(f.read())
    if result["error"]:
        flash(f"Fehler: {result['error']}", "error")
    else:
        bank_name = {"sparkasse": "Sparkasse", "voba_kraichgau": "VoBa Kraichgau", "voba_pur": "VoBa Pur"}[bank]
        flash(f"✅ {result['imported']} Buchungen ({bank_name}) importiert, {result['skipped']} übersprungen.", "success")
    return redirect(url_for("upload"))


@app.route("/migration", methods=["GET"])
def migration():
    """Versteckte Seite für den einmaligen Alt-Daten-Import."""
    return render_template("migration.html")


@app.route("/migration/upload", methods=["POST"])
def migration_upload():
    if "file" not in request.files:
        flash("Keine Datei ausgewählt.", "error")
        return redirect(url_for("migration"))

    file = request.files["file"]
    if file.filename == "":
        flash("Keine Datei ausgewählt.", "error")
        return redirect(url_for("migration"))

    content = file.read()
    if not content:
        flash("Die Datei ist leer.", "error")
        return redirect(url_for("migration"))

    from src.csv_import import import_legacy_csv

    try:
        res = import_legacy_csv(content)
        if res.get("error"):
            flash(f"Fehler beim Import: {res['error']}", "error")
        else:
            flash(f"Alt-Daten-Import erfolgreich: {res['imported']} Zahlungen importiert, {res['skipped']} übersprungen.", "success")
            
            # WICHTIG: Die Rechnungsstati neu berechnen!
            try:
                msg, ok = update_all()
                if ok:
                    flash(f"Status-Update: {msg}", "success")
            except Exception as e:
                pass
                
    except Exception as e:
        flash(f"Unerwarteter Fehler: {str(e)}", "error")

    return redirect(url_for("migration"))


@app.route("/migration/upload_invoices", methods=["POST"])
def migration_upload_invoices():
    if "file" not in request.files:
        flash("Keine Datei ausgewählt.", "error")
        return redirect(url_for("migration"))

    file = request.files["file"]
    if file.filename == "":
        flash("Keine Datei ausgewählt.", "error")
        return redirect(url_for("migration"))

    content = file.read()
    if not content:
        flash("Die Datei ist leer.", "error")
        return redirect(url_for("migration"))

    try:
        from src.csv_import import import_legacy_invoices_csv
        res = import_legacy_invoices_csv(content)
        if res.get("error"):
            flash(f"Fehler beim Import: {res['error']}", "error")
        else:
            flash(f"Rechnungs-Import erfolgreich: {res['imported']} Rechnungen importiert, {res['skipped']} übersprungen.", "success")
            
            # WICHTIG: Die Rechnungsstati neu berechnen!
            try:
                msg, ok = update_all()
                if ok:
                    flash(f"Status-Update: {msg}", "success")
            except Exception as e:
                pass
                
    except Exception as e:
        flash(f"Unerwarteter Fehler: {str(e)}", "error")

    return redirect(url_for("migration"))


@app.route("/shutdown", methods=["POST"])
def shutdown():
    """Schaltet den lokalen Flask-Server ab."""
    func = request.environ.get("werkzeug.server.shutdown")
    if func is None:
        # Fallback if unsupported
        os._exit(0)
    func()
    return "Programm wurde beendet. Sie können dieses Fenster nun schließen."


# ---------------------------------------------------------------------------
# Aktionen: Matching / Status / Mahnung
# ---------------------------------------------------------------------------

@app.route("/aktionen/matching", methods=["POST"])
def run_matching_action():
    try:
        apply_matching()
        flash("✅ Matching erfolgreich durchgeführt.", "success")
    except Exception as e:
        flash(f"Fehler beim Matching: {e}", "error")
    return redirect(url_for("dashboard"))


@app.route("/aktionen/status", methods=["POST"])
def run_status_action():
    try:
        update_all()
        flash("✅ Status aller Rechnungen aktualisiert.", "success")
    except Exception as e:
        flash(f"Fehler bei Status-Update: {e}", "error")
    return redirect(url_for("dashboard"))


@app.route("/aktionen/mahnung", methods=["POST"])
def run_mahnung_action():
    try:
        run_mahnung()
        flash("✅ Mahnlauf durchgeführt.", "success")
    except Exception as e:
        flash(f"Fehler beim Mahnlauf: {e}", "error")
    return redirect(url_for("dashboard"))


# ---------------------------------------------------------------------------
# Manuelle Zuordnungen
# ---------------------------------------------------------------------------

@app.route("/zuordnungen")
def zuordnungen():
    conn = get_db()
    maps = conn.execute("SELECT * FROM manual_map ORDER BY mapped_invoice_id DESC").fetchall()
    conn.close()
    return render_template("zuordnungen.html", maps=maps)


@app.route("/zuordnungen/add", methods=["POST"])
def add_zuordnung():
    sig = request.form.get("signature", "").strip()
    inv_id = request.form.get("invoice_id", "").strip()
    if not sig or not inv_id:
        flash("Signatur und Rechnungsnummer sind Pflichtfelder.", "error")
        return redirect(url_for("zuordnungen"))
    try:
        inv_id = int(inv_id)
    except ValueError:
        flash("Rechnungsnummer muss eine Zahl sein.", "error")
        return redirect(url_for("zuordnungen"))
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO manual_map(signature, mapped_invoice_id, updated_at) VALUES (?, ?, ?)",
        (sig, inv_id, datetime.utcnow().isoformat()),
    )
    conn.commit()
    conn.close()
    flash(f"✅ Zuordnung gespeichert: '{sig[:40]}...' → {inv_id}", "success")
    return redirect(url_for("zuordnungen"))


@app.route("/zuordnungen/delete", methods=["POST"])
def delete_zuordnung():
    sig = request.form.get("signature", "")
    conn = get_db()
    conn.execute("DELETE FROM manual_map WHERE signature = ?", (sig,))
    conn.commit()
    conn.close()
    flash("Zuordnung gelöscht.", "success")
    return redirect(url_for("zuordnungen"))


# ---------------------------------------------------------------------------
# Audit-Log
# ---------------------------------------------------------------------------

@app.route("/audit")
def audit():
    conn = get_db()
    page = max(1, int(request.args.get("page", 1)))
    per_page = 100
    logs = conn.execute(
        "SELECT * FROM audit_log ORDER BY audit_id DESC LIMIT ? OFFSET ?",
        (per_page, (page - 1) * per_page),
    ).fetchall()
    total = conn.execute("SELECT COUNT(*) FROM audit_log").fetchone()[0]
    conn.close()
    total_pages = max(1, (total + per_page - 1) // per_page)
    return render_template("audit.html", logs=logs, page=page, total_pages=total_pages, total=total)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True, port=5000)
