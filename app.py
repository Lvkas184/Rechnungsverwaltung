"""Flask Web-App für Rechnungsverwaltung."""

import json
import os
import re
import sqlite3
from datetime import datetime

from flask import Flask, flash, redirect, render_template, request, session, url_for

from src.csv_import import (
    import_datev_rechnungen,
    import_sparkasse_csv,
    import_voba_kraichgau_csv,
    import_voba_pur_csv,
)
from src.db import DB_PATH, PARAM_PATH, get_db, init_db
from src.import_history import fetch_import_batches, rollback_import_batch
from src.mahnung import run_mahnung
from src.matching import apply_matching
from src.reminders import clear_invoice_reminders, fetch_invoice_reminder_history, save_invoice_reminder
from src.status import update_all

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "rechnungsverwaltung-secret-key-change-me")

# Ensure uploads directory exists
UPLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

SPLIT_LINE_RE = re.compile(r"^\s*(\d{4,12})\s*[:=,;]\s*([-+]?\d+(?:[.,]\d{1,2})?)\s*$")
DEFAULT_PARAMS = {
    "Toleranz": 0.001,
    "due_days_1": 30,
    "due_days_2": 60,
    "due_days_3": 90,
    "split_threshold": 0.01,
    "match_score_auto": 0.85,
    "match_score_review": 0.6,
    "mahngebuehr_1_eur": 0.0,
    "mahngebuehr_2_eur": 0.0,
    "mahngebuehr_3_eur": 0.0,
    "mahngebuehr_eur": 0.0,
    "date_origin": "1899-12-30",
}
MANUAL_INVOICE_STATUSES = {
    "Offen",
    "Bezahlt",
    "Bezahlt mit Mahngebühr",
    "Teiloffen/Unterzahlung",
    "Überzahlung",
    "Akonto",
    "Schadensrechnungen",
}
MANUAL_PAYMENT_STATUSES = {
    "Offen",
    "Zugeordnet",
    "Akonto",
    "Schadensrechnungen",
}


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


def _parse_eur(value):
    """Parse user amount input like '381,69' or '381.69' to float."""
    s = str(value or "").strip()
    if not s:
        raise ValueError("Betrag fehlt.")
    s = s.replace("€", "").replace(" ", "")
    if "," in s:
        s = s.replace(".", "").replace(",", ".")
    amount = float(s)
    return round(amount, 2)


def _load_app_params():
    params = dict(DEFAULT_PARAMS)
    try:
        with open(PARAM_PATH, encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                params.update(data)
    except Exception:
        pass
    return params


def _save_app_params(params):
    with open(PARAM_PATH, "w", encoding="utf-8") as f:
        json.dump(params, f, ensure_ascii=False, indent=2)


def _payment_effective_status_sql(alias="payments"):
    """SQL CASE expression for effective payment status (manual override aware)."""
    return f"""
        CASE
            WHEN COALESCE({alias}.status_manual, 0) = 1
                 AND COALESCE({alias}.status_override, '') <> ''
            THEN {alias}.status_override
            WHEN COALESCE({alias}.matched, 0) = 1 THEN 'Zugeordnet'
            WHEN COALESCE({alias}.akonto, 0) = 1 THEN 'Akonto'
            WHEN COALESCE({alias}.schadensrechnung, 0) = 1 THEN 'Schadensrechnungen'
            ELSE 'Offen'
        END
    """


def payment_effective_status(payment):
    """Return effective status label for a payment row/dict."""
    status_manual = int((payment["status_manual"] if "status_manual" in payment.keys() else 0) or 0)
    if status_manual == 1:
        override = str((payment["status_override"] if "status_override" in payment.keys() else "") or "").strip()
        if override in MANUAL_PAYMENT_STATUSES:
            return override

    if int((payment["matched"] if "matched" in payment.keys() else 0) or 0) == 1:
        return "Zugeordnet"
    if int((payment["akonto"] if "akonto" in payment.keys() else 0) or 0) == 1:
        return "Akonto"
    if int((payment["schadensrechnung"] if "schadensrechnung" in payment.keys() else 0) or 0) == 1:
        return "Schadensrechnungen"
    return "Offen"


@app.context_processor
def _inject_template_helpers():
    return {
        "payment_effective_status": payment_effective_status,
    }


def _parse_manual_split_allocations(raw_text):
    """Parse multiline allocation text to [(invoice_id, amount), ...]."""
    grouped = {}
    lines = str(raw_text or "").splitlines()
    for idx, line in enumerate(lines, start=1):
        line = line.strip()
        if not line:
            continue
        m = SPLIT_LINE_RE.match(line)
        if not m:
            raise ValueError(f"Zeile {idx}: Format muss 'Rechnungsnummer=Betrag' sein.")
        inv_id = int(m.group(1))
        amount = _parse_eur(m.group(2))
        if abs(amount) < 0.005:
            raise ValueError(f"Zeile {idx}: Betrag darf nicht 0 sein.")
        grouped[inv_id] = round(grouped.get(inv_id, 0.0) + amount, 2)

    allocations = [(inv_id, amount) for inv_id, amount in grouped.items() if abs(amount) >= 0.005]
    if len(allocations) < 2:
        raise ValueError("Bitte mindestens zwei Rechnungszeilen angeben.")
    return allocations


def _resolve_editable_payment(conn, payment_id):
    """Return parent payment row if child row is addressed."""
    pay = conn.execute("SELECT * FROM payments WHERE payment_id = ?", (payment_id,)).fetchone()
    if not pay:
        return None
    if pay["parent_payment_id"]:
        parent = conn.execute("SELECT * FROM payments WHERE payment_id = ?", (pay["parent_payment_id"],)).fetchone()
        if parent:
            return parent
    return pay


def _delete_child_payments(conn, parent_payment_id):
    child_ids = [
        row["payment_id"]
        for row in conn.execute(
            "SELECT payment_id FROM payments WHERE parent_payment_id = ?",
            (parent_payment_id,),
        ).fetchall()
    ]
    if not child_ids:
        return 0
    marks = ",".join(["?"] * len(child_ids))
    conn.execute(f"DELETE FROM audit_log WHERE payment_id IN ({marks})", child_ids)
    conn.execute(f"DELETE FROM payments WHERE payment_id IN ({marks})", child_ids)
    return len(child_ids)


def _reset_payment_assignment(conn, parent_payment_id):
    """Clear invoice/split assignment for a parent payment."""
    removed_children = _delete_child_payments(conn, parent_payment_id)
    conn.execute(
        """UPDATE payments
           SET invoice_id = NULL,
               matched = 0,
               match_score = NULL,
               match_rule = NULL
           WHERE payment_id = ?""",
        (parent_payment_id,),
    )
    return removed_children


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

@app.route("/")
def dashboard():
    conn = get_db()
    payment_status_expr = _payment_effective_status_sql("p")
    stats = {
        "total_invoices": conn.execute("SELECT COUNT(*) FROM invoices").fetchone()[0],
        "open_invoices": conn.execute("SELECT COUNT(*) FROM invoices WHERE COALESCE(status, 'Offen') = 'Offen'").fetchone()[0],
        "partial_invoices": conn.execute("SELECT COUNT(*) FROM invoices WHERE status = 'Teiloffen/Unterzahlung'").fetchone()[0],
        "paid_invoices": conn.execute("SELECT COUNT(*) FROM invoices WHERE status IN ('Bezahlt', 'Bezahlt mit Mahngebühr')").fetchone()[0],
        "overpaid_invoices": conn.execute("SELECT COUNT(*) FROM invoices WHERE status = 'Überzahlung'").fetchone()[0],
        "total_payments": conn.execute("SELECT COUNT(*) FROM payments").fetchone()[0],
        "matched_payments": conn.execute(
            f"""
            SELECT COUNT(*)
            FROM payments p
            WHERE p.parent_payment_id IS NULL
              AND ({payment_status_expr}) IN ('Zugeordnet', 'Akonto', 'Schadensrechnungen')
            """
        ).fetchone()[0],
        "unmatched_payments": conn.execute(
            f"""
            SELECT COUNT(*)
            FROM payments p
            WHERE p.parent_payment_id IS NULL
              AND ({payment_status_expr}) = 'Offen'
            """
        ).fetchone()[0],
        "mahnung_1": conn.execute("SELECT COUNT(*) FROM invoices WHERE reminder_status = '1. Mahnung'").fetchone()[0],
        "mahnung_2": conn.execute("SELECT COUNT(*) FROM invoices WHERE reminder_status = '2. Mahnung'").fetchone()[0],
        "mahnung_3": conn.execute("SELECT COUNT(*) FROM invoices WHERE reminder_status = '3. Mahnung'").fetchone()[0],
        "open_sum": conn.execute("SELECT COALESCE(SUM(amount_gross - COALESCE(paid_sum_eur, 0)), 0) FROM invoices WHERE COALESCE(status, 'Offen') NOT IN ('Bezahlt', 'Bezahlt mit Mahngebühr')").fetchone()[0],
    }
    # Recent audit entries
    recent_audit = conn.execute(
        "SELECT * FROM audit_log ORDER BY audit_id DESC LIMIT 10"
    ).fetchall()
    conn.close()
    return render_template("dashboard.html", stats=stats, recent_audit=recent_audit)


# ---------------------------------------------------------------------------
# Einstellungen (Settings)
# ---------------------------------------------------------------------------

@app.route("/einstellungen", methods=["GET", "POST"])
def einstellungen():
    params = _load_app_params()
    if request.method == "POST":
        try:
            mahngebuehr_1 = _parse_eur((request.form.get("mahngebuehr_1_eur", "") or "").strip() or "0")
            mahngebuehr_2 = _parse_eur((request.form.get("mahngebuehr_2_eur", "") or "").strip() or "0")
            mahngebuehr_3 = _parse_eur((request.form.get("mahngebuehr_3_eur", "") or "").strip() or "0")
        except ValueError:
            flash("Mahngebühren müssen gültige Eurobeträge sein (z.B. 7,50).", "error")
            return redirect(url_for("einstellungen"))

        if mahngebuehr_1 < 0 or mahngebuehr_2 < 0 or mahngebuehr_3 < 0:
            flash("Mahngebühren dürfen nicht negativ sein.", "error")
            return redirect(url_for("einstellungen"))

        params["mahngebuehr_1_eur"] = round(mahngebuehr_1, 2)
        params["mahngebuehr_2_eur"] = round(mahngebuehr_2, 2)
        params["mahngebuehr_3_eur"] = round(mahngebuehr_3, 2)
        # Legacy-Parameter für Abwärtskompatibilität
        params["mahngebuehr_eur"] = round(mahngebuehr_1, 2)
        try:
            _save_app_params(params)
            update_all()
            flash("✅ Einstellungen gespeichert. Status wurde neu berechnet.", "success")
        except Exception as e:
            flash(f"Fehler beim Speichern der Einstellungen: {e}", "error")
        return redirect(url_for("einstellungen"))

    return render_template("einstellungen.html", params=params)


# ---------------------------------------------------------------------------
# Rechnungen (Invoices)
# ---------------------------------------------------------------------------

@app.route("/rechnungen")
def rechnungen():
    conn = get_db()
    # Persist sorting preference across navigation/tab changes.
    saved_sort_col = session.get("rechnungen_sort_col", "invoice_id")
    saved_order = session.get("rechnungen_order", "asc")

    status_filter = request.args.get("status", "")
    search = request.args.get("q", "").strip()
    page = max(1, int(request.args.get("page", 1)))
    sort_col = request.args.get("sort", saved_sort_col)
    order = request.args.get("order", saved_order).lower()
    per_page = int(request.args.get("per_page", 50))
    if per_page not in [20, 50, 100, 200, 500]:
        per_page = 50

    # Validate sorting
    valid_cols = [
        "invoice_id",
        "name",
        "remark",
        "amount_gross",
        "paid_sum_eur",
        "deviation_eur",
        "status",
        "reminder_status",
        "reminder_date",
    ]
    if sort_col not in valid_cols:
        sort_col = "invoice_id"
    if order not in ["asc", "desc"]:
        order = "asc"

    session["rechnungen_sort_col"] = sort_col
    session["rechnungen_order"] = order

    query = "SELECT * FROM invoices WHERE 1=1"
    params = []
    if status_filter:
        query += " AND status = ?"
        params.append(status_filter)
    if search:
        query += " AND (CAST(invoice_id AS TEXT) LIKE ? OR name LIKE ? OR COALESCE(remark, '') LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])
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
        count_query += " AND (CAST(invoice_id AS TEXT) LIKE ? OR name LIKE ? OR COALESCE(remark, '') LIKE ?)"
        count_params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])
    total = conn.execute(count_query, count_params).fetchone()[0]
    conn.close()

    total_pages = max(1, (total + per_page - 1) // per_page)
    return render_template("rechnungen.html", invoices=invoices, page=page,
                           total_pages=total_pages, total=total,
                           status_filter=status_filter, search=search,
                           sort_col=sort_col, order=order, per_page=per_page)


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
    reminder_history = fetch_invoice_reminder_history(conn, invoice_id, invoice_row=inv)
    conn.close()
    return render_template("rechnung_detail.html", inv=inv, payments=payments, audit=audit, reminder_history=reminder_history)


@app.route("/rechnungen/<int:invoice_id>/bemerkung", methods=["POST"])
def rechnung_update_bemerkung(invoice_id):
    raw_remark = request.form.get("remark", "")
    remark = str(raw_remark or "").strip()
    if len(remark) > 2000:
        flash("Bemerkung ist zu lang (max. 2000 Zeichen).", "error")
        return redirect(url_for("rechnung_detail", invoice_id=invoice_id))

    conn = get_db()
    try:
        row = conn.execute(
            "SELECT invoice_id FROM invoices WHERE invoice_id = ?",
            (invoice_id,),
        ).fetchone()
        if not row:
            flash("Rechnung nicht gefunden.", "error")
            return redirect(url_for("rechnungen"))

        conn.execute(
            """
            UPDATE invoices
            SET remark = ?,
                updated_at = ?
            WHERE invoice_id = ?
            """,
            (remark if remark else None, datetime.utcnow().isoformat(), invoice_id),
        )
        conn.commit()
        flash("✅ Bemerkung gespeichert.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Fehler beim Speichern der Bemerkung: {e}", "error")
    finally:
        conn.close()

    return redirect(url_for("rechnung_detail", invoice_id=invoice_id))


@app.route("/rechnungen/<int:invoice_id>/status", methods=["POST"])
def rechnung_update_status(invoice_id):
    status = (request.form.get("status") or "").strip()
    if status not in MANUAL_INVOICE_STATUSES:
        flash("Ungültiger Rechnungsstatus.", "error")
        return redirect(url_for("rechnung_detail", invoice_id=invoice_id))

    conn = get_db()
    try:
        row = conn.execute(
            "SELECT invoice_id FROM invoices WHERE invoice_id = ?",
            (invoice_id,),
        ).fetchone()
        if not row:
            flash("Rechnung nicht gefunden.", "error")
            return redirect(url_for("rechnungen"))

        conn.execute(
            """
            UPDATE invoices
            SET status = ?,
                status_manual = 1,
                updated_at = ?
            WHERE invoice_id = ?
            """,
            (status, datetime.utcnow().isoformat(), invoice_id),
        )
        conn.commit()
        flash("✅ Rechnungsstatus manuell gespeichert.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Fehler beim Speichern des Rechnungsstatus: {e}", "error")
    finally:
        conn.close()

    return redirect(url_for("rechnung_detail", invoice_id=invoice_id))


@app.route("/rechnungen/<int:invoice_id>/status/auto", methods=["POST"])
def rechnung_reset_status_auto(invoice_id):
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT invoice_id FROM invoices WHERE invoice_id = ?",
            (invoice_id,),
        ).fetchone()
        if not row:
            flash("Rechnung nicht gefunden.", "error")
            return redirect(url_for("rechnungen"))
        conn.execute(
            """
            UPDATE invoices
            SET status_manual = 0,
                updated_at = ?
            WHERE invoice_id = ?
            """,
            (datetime.utcnow().isoformat(), invoice_id),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        flash(f"Fehler beim Aktivieren der Status-Automatik: {e}", "error")
        return redirect(url_for("rechnung_detail", invoice_id=invoice_id))
    finally:
        conn.close()

    try:
        update_all()
    except Exception:
        pass
    flash("✅ Status-Automatik für diese Rechnung wieder aktiviert.", "success")
    return redirect(url_for("rechnung_detail", invoice_id=invoice_id))


@app.route("/rechnungen/<int:invoice_id>/mahnung", methods=["POST"])
def rechnung_update_mahnung(invoice_id):
    allowed_statuses = {"", "1. Mahnung", "2. Mahnung", "3. Mahnung"}
    reminder_status = (request.form.get("reminder_status") or "").strip()
    reminder_date_raw = (request.form.get("reminder_date") or "").strip()

    if reminder_status not in allowed_statuses:
        flash("Ungültiger Mahnungsstatus.", "error")
        return redirect(url_for("rechnung_detail", invoice_id=invoice_id))

    reminder_date = None
    if reminder_date_raw:
        try:
            reminder_date = datetime.fromisoformat(reminder_date_raw).date().isoformat()
        except ValueError:
            flash("Mahnungsdatum muss ein gültiges Datum sein.", "error")
            return redirect(url_for("rechnung_detail", invoice_id=invoice_id))

    if not reminder_status and reminder_date:
        flash("Mahnungsdatum kann nur zusammen mit einem Mahnungsstatus gesetzt werden.", "error")
        return redirect(url_for("rechnung_detail", invoice_id=invoice_id))

    if reminder_status and not reminder_date:
        reminder_date = datetime.utcnow().date().isoformat()

    conn = get_db()
    try:
        row = conn.execute(
            "SELECT invoice_id FROM invoices WHERE invoice_id = ?",
            (invoice_id,),
        ).fetchone()
        if not row:
            flash("Rechnung nicht gefunden.", "error")
            return redirect(url_for("rechnungen"))

        if reminder_status:
            save_invoice_reminder(
                conn,
                invoice_id,
                reminder_status,
                reminder_date,
                manual_entry=1,
                manual_override=1,
            )
        else:
            clear_invoice_reminders(conn, invoice_id, manual_override=1)
        conn.commit()
        if reminder_status:
            flash("✅ Mahnung gespeichert und im Verlauf ergänzt.", "success")
        else:
            flash("✅ Mahnverlauf für diese Rechnung gelöscht.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Fehler beim Speichern des Mahnungsstatus: {e}", "error")
    finally:
        conn.close()

    return redirect(url_for("rechnung_detail", invoice_id=invoice_id))


@app.route("/rechnungen/<int:invoice_id>/mahnung/auto", methods=["POST"])
def rechnung_reset_mahnung_auto(invoice_id):
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT invoice_id FROM invoices WHERE invoice_id = ?",
            (invoice_id,),
        ).fetchone()
        if not row:
            flash("Rechnung nicht gefunden.", "error")
            return redirect(url_for("rechnungen"))

        conn.execute(
            """
            UPDATE invoices
            SET reminder_manual = 0,
                updated_at = ?
            WHERE invoice_id = ?
            """,
            (datetime.utcnow().isoformat(), invoice_id),
        )
        conn.commit()
        flash("✅ Automatischen Mahnlauf für diese Rechnung wieder aktiviert.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Fehler beim Aktivieren der Automatik: {e}", "error")
    finally:
        conn.close()

    return redirect(url_for("rechnung_detail", invoice_id=invoice_id))


# ---------------------------------------------------------------------------
# Zahlungen (Payments)
# ---------------------------------------------------------------------------

@app.route("/zahlungen")
def zahlungen():
    conn = get_db()
    filter_type = request.args.get("filter", "")
    show_type = request.args.get("show", "income")  # income (default), all, akonto, schadens
    bank_filter = request.args.get("bank", "").strip()
    search = request.args.get("q", "").strip()
    page = max(1, int(request.args.get("page", 1)))
    sort_col = request.args.get("sort", "payment_id")
    order = request.args.get("order", "desc").lower()
    per_page = int(request.args.get("per_page", 50))
    if per_page not in [20, 50, 100, 200, 500]:
        per_page = 50
    if show_type not in ["income", "all", "akonto", "schadens"]:
        show_type = "income"
    effective_status_expr = _payment_effective_status_sql("payments")

    valid_cols = ["payment_id", "source", "booking_date", "amount_eur", "beneficiary_name", "reference_text", "match_score", "invoice_id", "matched", "remark"]
    sort_expr = {
        "payment_id": "payments.payment_id",
        "source": "payments.source",
        "booking_date": "payments.booking_date",
        "amount_eur": "payments.amount_eur",
        "beneficiary_name": "payments.beneficiary_name",
        "reference_text": "payments.reference_text",
        "match_score": "payments.match_score",
        "invoice_id": "payments.invoice_id",
        "matched": effective_status_expr,
        "remark": "payments.remark",
    }
    if sort_col not in valid_cols:
        sort_col = "payment_id"
    if order not in ["asc", "desc"]:
        order = "desc"

    bank_options = [
        row["source"]
        for row in conn.execute(
            """
            SELECT DISTINCT source
            FROM payments
            WHERE source IS NOT NULL
              AND TRIM(source) <> ''
            ORDER BY source COLLATE NOCASE
            """
        ).fetchall()
    ]

    query = """
        SELECT payments.*, invoices.remark AS invoice_remark
        FROM payments
        LEFT JOIN invoices ON invoices.invoice_id = payments.invoice_id
        WHERE 1=1
          AND payments.parent_payment_id IS NULL
    """
    params = []
    
    # Show filter: only income, all payments, or Akonto (Abschlagsrechnungen 9xxxxx)
    if show_type == "income":
        query += " AND payments.amount_eur > 0"
    elif show_type == "akonto":
        query += """
            AND (
                ({status_expr}) = 'Akonto'
                OR
                COALESCE(payments.akonto, 0) = 1
                OR (payments.invoice_id BETWEEN 900000 AND 999999)
                OR EXISTS (
                    SELECT 1
                    FROM payments child
                    WHERE child.parent_payment_id = payments.payment_id
                      AND (
                        COALESCE(child.akonto, 0) = 1
                        OR child.invoice_id BETWEEN 900000 AND 999999
                      )
                )
            )
        """.format(status_expr=effective_status_expr)
    elif show_type == "schadens":
        query += """
            AND (
                ({status_expr}) = 'Schadensrechnungen'
                OR
                COALESCE(payments.schadensrechnung, 0) = 1
                OR (payments.invoice_id BETWEEN 800000 AND 899999)
                OR EXISTS (
                    SELECT 1
                    FROM payments child
                    WHERE child.parent_payment_id = payments.payment_id
                      AND (
                        COALESCE(child.schadensrechnung, 0) = 1
                        OR child.invoice_id BETWEEN 800000 AND 899999
                      )
                )
            )
        """.format(status_expr=effective_status_expr)
    
    if filter_type == "matched":
        query += f" AND ({effective_status_expr}) = 'Zugeordnet'"
    elif filter_type == "unmatched":
        query += f" AND ({effective_status_expr}) = 'Offen'"
    elif filter_type == "review":
        query += f"""
            AND ({effective_status_expr}) = 'Offen'
            AND payments.match_score IS NOT NULL
            AND payments.match_score > 0
        """
    if bank_filter:
        query += " AND payments.source = ?"
        params.append(bank_filter)
    if search:
        query += """
            AND (
                payments.reference_text LIKE ?
                OR payments.beneficiary_name LIKE ?
                OR CAST(payments.invoice_id AS TEXT) LIKE ?
                OR COALESCE(payments.remark, '') LIKE ?
                OR COALESCE(invoices.remark, '') LIKE ?
            )
        """
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%", f"%{search}%", f"%{search}%"])
    query += f" ORDER BY {sort_expr[sort_col]} {order.upper()} LIMIT ? OFFSET ?"
    params.extend([per_page, (page - 1) * per_page])

    payments = conn.execute(query, params).fetchall()

    count_query = """
        SELECT COUNT(*)
        FROM payments
        LEFT JOIN invoices ON invoices.invoice_id = payments.invoice_id
        WHERE 1=1
          AND payments.parent_payment_id IS NULL
    """
    count_params = []
    
    if show_type == "income":
        count_query += " AND payments.amount_eur > 0"
    elif show_type == "akonto":
        count_query += """
            AND (
                ({status_expr}) = 'Akonto'
                OR
                COALESCE(payments.akonto, 0) = 1
                OR (payments.invoice_id BETWEEN 900000 AND 999999)
                OR EXISTS (
                    SELECT 1
                    FROM payments child
                    WHERE child.parent_payment_id = payments.payment_id
                      AND (
                        COALESCE(child.akonto, 0) = 1
                        OR child.invoice_id BETWEEN 900000 AND 999999
                      )
                )
            )
        """.format(status_expr=effective_status_expr)
    elif show_type == "schadens":
        count_query += """
            AND (
                ({status_expr}) = 'Schadensrechnungen'
                OR
                COALESCE(payments.schadensrechnung, 0) = 1
                OR (payments.invoice_id BETWEEN 800000 AND 899999)
                OR EXISTS (
                    SELECT 1
                    FROM payments child
                    WHERE child.parent_payment_id = payments.payment_id
                      AND (
                        COALESCE(child.schadensrechnung, 0) = 1
                        OR child.invoice_id BETWEEN 800000 AND 899999
                      )
                )
            )
        """.format(status_expr=effective_status_expr)
        
    if filter_type == "matched":
        count_query += f" AND ({effective_status_expr}) = 'Zugeordnet'"
    elif filter_type == "unmatched":
        count_query += f" AND ({effective_status_expr}) = 'Offen'"
    elif filter_type == "review":
        count_query += f"""
            AND ({effective_status_expr}) = 'Offen'
            AND payments.match_score IS NOT NULL
            AND payments.match_score > 0
        """
    if bank_filter:
        count_query += " AND payments.source = ?"
        count_params.append(bank_filter)
    if search:
        count_query += """
            AND (
                payments.reference_text LIKE ?
                OR payments.beneficiary_name LIKE ?
                OR CAST(payments.invoice_id AS TEXT) LIKE ?
                OR COALESCE(payments.remark, '') LIKE ?
                OR COALESCE(invoices.remark, '') LIKE ?
            )
        """
        count_params.extend([f"%{search}%", f"%{search}%", f"%{search}%", f"%{search}%", f"%{search}%"])
    total = conn.execute(count_query, count_params).fetchone()[0]
    conn.close()

    total_pages = max(1, (total + per_page - 1) // per_page)
    return render_template("zahlungen.html", payments=payments, page=page,
                           total_pages=total_pages, total=total,
                           filter_type=filter_type, show_type=show_type, bank_filter=bank_filter,
                           bank_options=bank_options, search=search,
                           sort_col=sort_col, order=order, per_page=per_page)


@app.route("/zahlungen/<int:payment_id>")
def zahlung_detail(payment_id):
    conn = get_db()
    pay = conn.execute("SELECT * FROM payments WHERE payment_id = ?", (payment_id,)).fetchone()
    if not pay:
        conn.close()
        flash("Zahlung nicht gefunden.", "error")
        return redirect(url_for("zahlungen"))
    
    # If matched to a single invoice, fetch the corresponding invoice details
    inv = None
    if pay["invoice_id"]:
        inv = conn.execute("SELECT * FROM invoices WHERE invoice_id = ?", (pay["invoice_id"],)).fetchone()

    # Child allocations for split parent payments
    split_invoices = conn.execute(
        """
        SELECT p.payment_id, p.parent_payment_id, p.invoice_id, p.amount_eur, p.match_score,
               i.name AS invoice_name, i.amount_gross AS invoice_amount_gross,
               i.status AS invoice_status, i.reminder_status AS invoice_reminder_status,
               i.remark AS invoice_remark
        FROM payments p
        LEFT JOIN invoices i ON i.invoice_id = p.invoice_id
        WHERE p.parent_payment_id = ?
        ORDER BY p.payment_id ASC
        """,
        (payment_id,),
    ).fetchall()

    parent_payment = None
    if pay["parent_payment_id"]:
        parent_payment = conn.execute(
            "SELECT * FROM payments WHERE payment_id = ?",
            (pay["parent_payment_id"],),
        ).fetchone()

    audit = conn.execute(
        """
        SELECT * FROM audit_log
        WHERE payment_id = ?
           OR payment_id IN (SELECT payment_id FROM payments WHERE parent_payment_id = ?)
        ORDER BY audit_id DESC
        """,
        (payment_id, payment_id),
    ).fetchall()
    conn.close()
    return render_template(
        "zahlung_detail.html",
        pay=pay,
        inv=inv,
        split_invoices=split_invoices,
        parent_payment=parent_payment,
        audit=audit,
    )


@app.route("/zahlungen/<int:payment_id>/bemerkung", methods=["POST"])
def zahlung_update_bemerkung(payment_id):
    raw_remark = request.form.get("remark", "")
    remark = str(raw_remark or "").strip()
    if len(remark) > 2000:
        flash("Bemerkung ist zu lang (max. 2000 Zeichen).", "error")
        return redirect(url_for("zahlung_detail", payment_id=payment_id))

    conn = get_db()
    target_id = payment_id
    try:
        pay = _resolve_editable_payment(conn, payment_id)
        if not pay:
            flash("Zahlung nicht gefunden.", "error")
            return redirect(url_for("zahlungen"))
        target_id = pay["payment_id"]

        value = remark if remark else None
        conn.execute(
            "UPDATE payments SET remark = ? WHERE payment_id = ?",
            (value, target_id),
        )
        conn.execute(
            "UPDATE payments SET remark = ? WHERE parent_payment_id = ?",
            (value, target_id),
        )
        conn.commit()
        flash("✅ Bemerkung zur Zahlung gespeichert.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Fehler beim Speichern der Zahlungs-Bemerkung: {e}", "error")
    finally:
        conn.close()

    return redirect(url_for("zahlung_detail", payment_id=target_id))


@app.route("/zahlungen/<int:payment_id>/status", methods=["POST"])
def zahlung_update_status(payment_id):
    manual_status = (request.form.get("status") or "").strip()
    if manual_status not in MANUAL_PAYMENT_STATUSES:
        flash("Ungültiger Zahlungsstatus.", "error")
        return redirect(url_for("zahlung_detail", payment_id=payment_id))

    conn = get_db()
    target_id = payment_id
    try:
        pay = _resolve_editable_payment(conn, payment_id)
        if not pay:
            flash("Zahlung nicht gefunden.", "error")
            return redirect(url_for("zahlungen"))
        target_id = pay["payment_id"]

        conn.execute(
            """
            UPDATE payments
            SET status_override = ?,
                status_manual = 1
            WHERE payment_id = ?
            """,
            (manual_status, target_id),
        )
        conn.execute(
            """
            UPDATE payments
            SET status_override = ?,
                status_manual = 1
            WHERE parent_payment_id = ?
            """,
            (manual_status, target_id),
        )
        conn.commit()
        flash("✅ Zahlungsstatus manuell gespeichert.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Fehler beim Speichern des Zahlungsstatus: {e}", "error")
    finally:
        conn.close()

    return redirect(url_for("zahlung_detail", payment_id=target_id))


@app.route("/zahlungen/<int:payment_id>/status/auto", methods=["POST"])
def zahlung_reset_status_auto(payment_id):
    conn = get_db()
    target_id = payment_id
    try:
        pay = _resolve_editable_payment(conn, payment_id)
        if not pay:
            flash("Zahlung nicht gefunden.", "error")
            return redirect(url_for("zahlungen"))
        target_id = pay["payment_id"]

        conn.execute(
            """
            UPDATE payments
            SET status_override = NULL,
                status_manual = 0
            WHERE payment_id = ?
            """,
            (target_id,),
        )
        conn.execute(
            """
            UPDATE payments
            SET status_override = NULL,
                status_manual = 0
            WHERE parent_payment_id = ?
            """,
            (target_id,),
        )
        conn.commit()
        flash("✅ Zahlungsstatus wieder auf Automatik gesetzt.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Fehler beim Zurücksetzen des Zahlungsstatus: {e}", "error")
    finally:
        conn.close()

    return redirect(url_for("zahlung_detail", payment_id=target_id))


@app.route("/zahlungen/<int:payment_id>/manual/assign", methods=["POST"])
def zahlung_manual_assign(payment_id):
    inv_raw = request.form.get("invoice_id", "").strip()
    if not inv_raw:
        flash("Bitte eine Rechnungsnummer angeben.", "error")
        return redirect(url_for("zahlung_detail", payment_id=payment_id))
    try:
        invoice_id = int(inv_raw)
    except ValueError:
        flash("Rechnungsnummer muss eine Zahl sein.", "error")
        return redirect(url_for("zahlung_detail", payment_id=payment_id))

    conn = get_db()
    target_id = payment_id
    try:
        pay = _resolve_editable_payment(conn, payment_id)
        if not pay:
            flash("Zahlung nicht gefunden.", "error")
            return redirect(url_for("zahlungen"))
        target_id = pay["payment_id"]

        inv = conn.execute("SELECT invoice_id FROM invoices WHERE invoice_id = ?", (invoice_id,)).fetchone()
        if not inv:
            flash(f"Rechnung #{invoice_id} existiert nicht.", "error")
            return redirect(url_for("zahlung_detail", payment_id=target_id))

        _reset_payment_assignment(conn, target_id)
        conn.execute(
            """UPDATE payments
               SET invoice_id = ?, matched = 1, match_score = 1.0, match_rule = 'manual_single', created_by = 'manual'
               WHERE payment_id = ?""",
            (invoice_id, target_id),
        )
        conn.execute(
            "INSERT INTO audit_log(payment_id, invoice_id, match_score, rule_used, automated, user) VALUES (?,?,?,?,0,'manual')",
            (target_id, invoice_id, 1.0, "manual_single"),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        flash(f"Fehler bei manueller Zuordnung: {e}", "error")
        return redirect(url_for("zahlung_detail", payment_id=target_id))
    finally:
        conn.close()

    try:
        update_all()
    except Exception:
        pass

    flash(f"✅ Zahlung #{target_id} manuell auf Rechnung #{invoice_id} gesetzt.", "success")
    return redirect(url_for("zahlung_detail", payment_id=target_id))


@app.route("/zahlungen/<int:payment_id>/manual/split", methods=["POST"])
def zahlung_manual_split(payment_id):
    raw_alloc = request.form.get("allocations", "")
    conn = get_db()
    target_id = payment_id
    try:
        pay = _resolve_editable_payment(conn, payment_id)
        if not pay:
            flash("Zahlung nicht gefunden.", "error")
            return redirect(url_for("zahlungen"))
        target_id = pay["payment_id"]

        allocations = _parse_manual_split_allocations(raw_alloc)
        parent_amount = round(float(pay["amount_eur"] or 0), 2)
        alloc_sum = round(sum(amount for _, amount in allocations), 2)
        if abs(alloc_sum - parent_amount) > 0.01:
            flash(
                f"Summe der Aufteilung ({alloc_sum:,.2f} €) muss dem Zahlungsbetrag ({parent_amount:,.2f} €) entsprechen."
                .replace(",", "X").replace(".", ",").replace("X", "."),
                "error",
            )
            return redirect(url_for("zahlung_detail", payment_id=target_id))

        for inv_id, _ in allocations:
            inv = conn.execute("SELECT invoice_id FROM invoices WHERE invoice_id = ?", (inv_id,)).fetchone()
            if not inv:
                flash(f"Rechnung #{inv_id} existiert nicht.", "error")
                return redirect(url_for("zahlung_detail", payment_id=target_id))

        _reset_payment_assignment(conn, target_id)
        for inv_id, amount in allocations:
            conn.execute(
                """INSERT INTO payments(
                     invoice_id, parent_payment_id, source, booking_date, value_date, amount_eur,
                     reference_text, iban, beneficiary_name, remark, matched, match_score, match_rule, created_by
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1.0, 'manual_split_child', 'manual')""",
                (
                    inv_id,
                    target_id,
                    pay["source"],
                    pay["booking_date"],
                    pay["value_date"],
                    amount,
                    pay["reference_text"],
                    pay["iban"],
                    pay["beneficiary_name"],
                    pay["remark"],
                ),
            )
            child_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.execute(
                "INSERT INTO audit_log(payment_id, invoice_id, match_score, rule_used, automated, user) VALUES (?,?,?,?,0,'manual')",
                (child_id, inv_id, 1.0, "manual_split_child"),
            )

        conn.execute(
            """UPDATE payments
               SET invoice_id = NULL, matched = 1, match_score = 1.0, match_rule = 'manual_split', created_by = 'manual'
               WHERE payment_id = ?""",
            (target_id,),
        )
        conn.execute(
            "INSERT INTO audit_log(payment_id, invoice_id, match_score, rule_used, automated, user) VALUES (?,?,?,?,0,'manual')",
            (target_id, None, 1.0, "manual_split"),
        )
        conn.commit()
    except ValueError as e:
        conn.rollback()
        flash(str(e), "error")
        return redirect(url_for("zahlung_detail", payment_id=target_id))
    except Exception as e:
        conn.rollback()
        flash(f"Fehler bei manueller Aufteilung: {e}", "error")
        return redirect(url_for("zahlung_detail", payment_id=target_id))
    finally:
        conn.close()

    try:
        update_all()
    except Exception:
        pass

    flash(f"✅ Zahlung #{target_id} manuell aufgeteilt.", "success")
    return redirect(url_for("zahlung_detail", payment_id=target_id))


@app.route("/zahlungen/<int:payment_id>/manual/clear", methods=["POST"])
def zahlung_manual_clear(payment_id):
    conn = get_db()
    target_id = payment_id
    try:
        pay = _resolve_editable_payment(conn, payment_id)
        if not pay:
            flash("Zahlung nicht gefunden.", "error")
            return redirect(url_for("zahlungen"))
        target_id = pay["payment_id"]

        removed_children = _reset_payment_assignment(conn, target_id)
        conn.execute(
            """UPDATE payments
               SET matched = 0, match_score = NULL, match_rule = 'manual_unassigned', created_by = 'manual'
               WHERE payment_id = ?""",
            (target_id,),
        )
        conn.execute(
            "INSERT INTO audit_log(payment_id, invoice_id, match_score, rule_used, automated, user) VALUES (?,?,?,?,0,'manual')",
            (target_id, None, 0.0, "manual_unassigned"),
        )
        conn.commit()
    except Exception as e:
        conn.rollback()
        flash(f"Fehler beim Lösen der Zuordnung: {e}", "error")
        return redirect(url_for("zahlung_detail", payment_id=target_id))
    finally:
        conn.close()

    try:
        update_all()
    except Exception:
        pass

    msg = f"✅ Zuordnung für Zahlung #{target_id} entfernt."
    if removed_children:
        msg += f" ({removed_children} Split-Zeilen entfernt)"
    flash(msg, "success")
    return redirect(url_for("zahlung_detail", payment_id=target_id))



# ---------------------------------------------------------------------------
# CSV Upload
# ---------------------------------------------------------------------------

@app.route("/upload")
def upload():
    import_batches = fetch_import_batches(limit=30)
    return render_template("upload.html", import_batches=import_batches)


@app.route("/upload/rechnungen", methods=["POST"])
def upload_rechnungen():
    f = request.files.get("file")
    if not f or not f.filename:
        flash("Keine Datei ausgewählt.", "error")
        return redirect(url_for("upload"))
    result = import_datev_rechnungen(f.read(), filename=f.filename, created_by="upload")
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

    result = importer(f.read(), filename=f.filename, created_by="upload")
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
        res = import_legacy_csv(content, filename=file.filename, created_by="migration")
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
        res = import_legacy_invoices_csv(content, filename=file.filename, created_by="migration")
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


@app.route("/imports/<int:import_batch_id>/rollback", methods=["POST"])
def import_rollback(import_batch_id):
    result = rollback_import_batch(import_batch_id)
    if not result.get("ok"):
        flash(result.get("error", "Import konnte nicht rückgängig gemacht werden."), "error")
        return redirect(url_for("upload"))

    try:
        update_all()
    except Exception:
        pass

    flash(result.get("message", "Import wurde rückgängig gemacht."), "success")
    return redirect(url_for("upload"))


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
