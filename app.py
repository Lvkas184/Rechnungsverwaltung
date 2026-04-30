"""Flask Web-App für Rechnungsverwaltung."""

import json
import os
import re
import sqlite3
from datetime import datetime

from flask import Flask, flash, jsonify, redirect, render_template, request, session, url_for

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
    "custom_invoice_statuses": [],
    "custom_payment_statuses": [],
    "custom_invoice_status_colors": {},
    "custom_payment_status_colors": {},
    "date_origin": "1899-12-30",
}
MANUAL_INVOICE_STATUS_ORDER = [
    "Offen",
    "In Klärung",
    "Bezahlt",
    "Bezahlt mit Mahngebühr",
    "Teiloffen/Unterzahlung",
    "Überzahlung",
    "ausgebucht",
    "Gutschrift",
    "Akonto",
    "Schadensrechnungen",
]
MANUAL_PAYMENT_STATUS_ORDER = [
    "Offen",
    "Zugeordnet",
    "Akonto",
    "Schadensrechnungen",
]
INVOICE_DOC_TYPES = {"rechnung", "gutschrift"}
MANUAL_INVOICE_STATUSES = set(MANUAL_INVOICE_STATUS_ORDER)
MANUAL_PAYMENT_STATUSES = set(MANUAL_PAYMENT_STATUS_ORDER)
SPLIT_NAME_STOPWORDS = {
    "gmbh",
    "co",
    "und",
    "kg",
    "ag",
    "mbh",
    "verwaltung",
    "familie",
    "stadt",
    "stadtwerke",
    "herr",
    "frau",
    "dr",
    "med",
    "von",
    "der",
    "die",
    "das",
    "e",
    "v",
    "c",
    "o",
}
STEUERBUERO_EXCLUDED_ACTION_CODES = {
    "payment_manual_assign_single",
    "payment_manual_assign_split",
    "payment_manual_unassign",
    "manual_single",
    "manual_split",
    "manual_split_child",
    "manual_unassigned",
}

STATUS_COLOR_PRESETS = {
    "grau": {"bg": "#e5e7eb", "text": "#4b5563", "border": "#d1d5db"},
    "gruen": {"bg": "#dcfce7", "text": "#15803d", "border": "#bbf7d0"},
    "rot": {"bg": "#fee2e2", "text": "#c2410c", "border": "#fecaca"},
    "orange": {"bg": "#ffedd5", "text": "#c2410c", "border": "#fed7aa"},
    "blau": {"bg": "#dbeafe", "text": "#1d4ed8", "border": "#bfdbfe"},
    "lila": {"bg": "#ede9fe", "text": "#7c3aed", "border": "#ddd6fe"},
    "gelb": {"bg": "#fef9c3", "text": "#a16207", "border": "#fde68a"},
    "tuerkis": {"bg": "#ccfbf1", "text": "#0f766e", "border": "#99f6e4"},
}

STATUS_COLOR_ALIASES = {
    "grau": "grau",
    "gray": "grau",
    "grey": "grau",
    "gruen": "gruen",
    "grun": "gruen",
    "green": "gruen",
    "rot": "rot",
    "red": "rot",
    "orange": "orange",
    "blau": "blau",
    "blue": "blau",
    "lila": "lila",
    "violett": "lila",
    "violet": "lila",
    "purple": "lila",
    "gelb": "gelb",
    "yellow": "gelb",
    "tuerkis": "tuerkis",
    "turkis": "tuerkis",
    "teal": "tuerkis",
    "turquoise": "tuerkis",
    "cyan": "tuerkis",
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


@app.template_filter("format_datetime")
def format_datetime(value):
    """Format ISO datetime to German date + time."""
    if not value:
        return "—"
    try:
        d = datetime.fromisoformat(str(value))
        return d.strftime("%d.%m.%Y %H:%M")
    except (ValueError, TypeError):
        return str(value)


@app.template_filter("status_class")
def status_class(value):
    """Map status labels to stable ASCII CSS class names."""
    s = str(value or "").strip().lower()
    replacements = {
        "ä": "ae",
        "ö": "oe",
        "ü": "ue",
        "ß": "ss",
        "/": "-",
        " ": "-",
    }
    for old, new in replacements.items():
        s = s.replace(old, new)
    while "--" in s:
        s = s.replace("--", "-")
    return s or "offen"


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


def _status_sort_unique(values):
    unique = []
    seen = set()
    for raw in values:
        value = str(raw or "").strip()
        if not value:
            continue
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        unique.append(value)
    return unique


def _normalize_ascii_key(value):
    s = str(value or "").strip().lower()
    replacements = {
        "ä": "ae",
        "ö": "oe",
        "ü": "ue",
        "ß": "ss",
    }
    for old, new in replacements.items():
        s = s.replace(old, new)
    return s


def _normalize_hex_color(value):
    raw = str(value or "").strip()
    if not raw:
        return ""
    if re.fullmatch(r"#[0-9a-fA-F]{6}", raw):
        return raw.lower()
    if re.fullmatch(r"#[0-9a-fA-F]{3}", raw):
        r, g, b = raw[1], raw[2], raw[3]
        return f"#{r}{r}{g}{g}{b}{b}".lower()
    return ""


def _hex_to_rgb(hex_color):
    normalized = _normalize_hex_color(hex_color)
    if not normalized:
        return None
    return (
        int(normalized[1:3], 16),
        int(normalized[3:5], 16),
        int(normalized[5:7], 16),
    )


def _parse_custom_statuses_input(raw_value):
    if raw_value is None:
        return []

    if isinstance(raw_value, (list, tuple, set)):
        raw_items = list(raw_value)
    else:
        raw_items = str(raw_value).splitlines()

    statuses = []
    for item in raw_items:
        for chunk in re.split(r"[;,]", str(item or "")):
            value = chunk.strip()
            if not value:
                continue
            if len(value) > 60:
                raise ValueError("Jeder Zusatz-Status darf maximal 60 Zeichen haben.")
            statuses.append(value)

    statuses = _status_sort_unique(statuses)
    if len(statuses) > 50:
        raise ValueError("Maximal 50 Zusatz-Status pro Bereich erlaubt.")
    return statuses


def _parse_custom_status_colors_input(raw_value, allowed_statuses, strict=True):
    if raw_value is None:
        return {}

    allowed_lookup = {str(s).strip().casefold(): str(s).strip() for s in (allowed_statuses or []) if str(s).strip()}
    result = {}

    raw_pairs = []
    if isinstance(raw_value, dict):
        raw_pairs = list(raw_value.items())
    else:
        if isinstance(raw_value, (list, tuple, set)):
            raw_lines = [str(item or "") for item in raw_value]
        else:
            raw_lines = str(raw_value).splitlines()
        for idx, raw_line in enumerate(raw_lines, start=1):
            line = str(raw_line or "").strip()
            if not line:
                continue
            parts = re.split(r"\s*[:=]\s*", line, maxsplit=1)
            if len(parts) != 2:
                if strict:
                    raise ValueError(
                        f"Zeile {idx} bei Status-Farben ist ungültig. Format: Status=Farbe."
                    )
                continue
            raw_pairs.append((parts[0], parts[1]))

    if len(raw_pairs) > 100:
        raise ValueError("Maximal 100 Status-Farbzuordnungen pro Bereich erlaubt.")

    for raw_status, raw_color in raw_pairs:
        status_label = str(raw_status or "").strip()
        color_name = str(raw_color or "").strip()
        if not status_label:
            if strict:
                raise ValueError("Statusname in der Farbzuordnung darf nicht leer sein.")
            continue
        if not color_name:
            if strict:
                raise ValueError(f"Farbwert für Status '{status_label}' darf nicht leer sein.")
            continue

        status_key = status_label.casefold()
        canonical_status = allowed_lookup.get(status_key)
        if not canonical_status:
            if strict:
                raise ValueError(
                    f"Status '{status_label}' ist unbekannt. Bitte zuerst als Status anlegen."
                )
            continue

        hex_color = _normalize_hex_color(color_name)
        if hex_color:
            result[canonical_status] = hex_color
            continue

        normalized_color = _normalize_ascii_key(color_name)
        normalized_color = re.sub(r"[\s_-]+", "", normalized_color)
        canonical_color = STATUS_COLOR_ALIASES.get(normalized_color)
        if not canonical_color:
            if strict:
                allowed_colors = ", ".join(sorted(STATUS_COLOR_PRESETS.keys()))
                raise ValueError(
                    f"Farbe '{color_name}' ist ungültig. Erlaubt: {allowed_colors} oder z.B. #1d4ed8."
                )
            continue

        result[canonical_status] = canonical_color

    return result


def _status_color_map_to_text(statuses, color_map):
    if not isinstance(color_map, dict):
        return ""
    lines = []
    known = {str(s).strip().casefold() for s in (statuses or [])}
    seen = set()
    for status in statuses or []:
        status_label = str(status or "").strip()
        if not status_label:
            continue
        color = color_map.get(status_label)
        if color:
            lines.append(f"{status_label}={color}")
            seen.add(status_label.casefold())
    # Fallback for unknown entries that may still exist in config.
    for status_label in sorted(color_map.keys(), key=lambda x: str(x).casefold()):
        key = str(status_label or "").strip().casefold()
        if not key or key in seen:
            continue
        if known and key not in known:
            continue
        lines.append(f"{status_label}={color_map[status_label]}")
    return "\n".join(lines)


def _status_badge_inline_style(status, kind, status_cfg):
    value = str(status or "").strip()
    if not value:
        return ""
    mapping = (
        status_cfg.get("invoice_status_colors", {})
        if kind == "invoice"
        else status_cfg.get("payment_status_colors", {})
    )
    if not isinstance(mapping, dict):
        return ""

    color_key = None
    target_key = value.casefold()
    for status_name, configured_color in mapping.items():
        if str(status_name or "").strip().casefold() == target_key:
            color_key = configured_color
            break

    if not color_key:
        return ""
    palette = STATUS_COLOR_PRESETS.get(str(color_key))
    if palette:
        return (
            f"background: {palette['bg']}; "
            f"color: {palette['text']}; "
            f"border-color: {palette['border']};"
        )

    rgb = _hex_to_rgb(color_key)
    if not rgb:
        return ""
    r, g, b = rgb
    return (
        f"background: rgba({r}, {g}, {b}, 0.18); "
        f"color: rgb({r}, {g}, {b}); "
        f"border-color: rgba({r}, {g}, {b}, 0.36);"
    )


def _status_options_from_params(params=None):
    source = params if isinstance(params, dict) else _load_app_params()
    try:
        extra_invoice = _parse_custom_statuses_input(source.get("custom_invoice_statuses", []))
    except ValueError:
        extra_invoice = []
    try:
        extra_payment = _parse_custom_statuses_input(source.get("custom_payment_statuses", []))
    except ValueError:
        extra_payment = []

    invoice_statuses = _status_sort_unique(MANUAL_INVOICE_STATUS_ORDER + extra_invoice)
    payment_statuses = _status_sort_unique(MANUAL_PAYMENT_STATUS_ORDER + extra_payment)
    invoice_status_colors = _parse_custom_status_colors_input(
        source.get("custom_invoice_status_colors", {}),
        invoice_statuses,
        strict=False,
    )
    payment_status_colors = _parse_custom_status_colors_input(
        source.get("custom_payment_status_colors", {}),
        payment_statuses,
        strict=False,
    )
    return {
        "invoice_statuses": invoice_statuses,
        "payment_statuses": payment_statuses,
        "invoice_status_colors": invoice_status_colors,
        "payment_status_colors": payment_status_colors,
    }


def _status_options_with_current(statuses, current_status):
    options = list(statuses or [])
    current = str(current_status or "").strip()
    if not current:
        return options
    if current.casefold() not in {item.casefold() for item in options}:
        options.append(current)
    return options


def _redirect_to_next(default_endpoint, **default_values):
    next_url = (request.form.get("next") or request.args.get("next") or "").strip()
    if next_url.startswith("/") and not next_url.startswith("//"):
        return redirect(next_url)
    return redirect(url_for(default_endpoint, **default_values))


def _serialize_change_value(value):
    if value is None:
        return None
    if isinstance(value, (dict, list, tuple, set)):
        try:
            text = json.dumps(value, ensure_ascii=False)
            return text if text else None
        except Exception:
            return str(value)
    text = str(value).strip()
    return text if text else None


def _format_eur_for_log(value):
    if value is None:
        return None
    try:
        amount = float(value)
    except (TypeError, ValueError):
        return _serialize_change_value(value)
    return f"{amount:,.2f} €".replace(",", "X").replace(".", ",").replace("X", ".")


def _log_manual_change(
    conn,
    *,
    action_code,
    action_label,
    change_scope,
    entry_origin="auto",
    invoice_id=None,
    payment_id=None,
    before_value=None,
    after_value=None,
    note=None,
    changed_by="manual",
):
    conn.execute(
        """
        INSERT INTO manual_change_log(
            entry_origin, change_scope, invoice_id, payment_id,
            action_code, action_label,
            before_value, after_value, note, changed_by, changed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "manual" if str(entry_origin or "").strip().lower() == "manual" else "auto",
            str(change_scope or "").strip() or "unknown",
            invoice_id,
            payment_id,
            str(action_code or "").strip() or "manual_change",
            str(action_label or "").strip() or "Manuelle Änderung",
            _serialize_change_value(before_value),
            _serialize_change_value(after_value),
            _serialize_change_value(note),
            _serialize_change_value(changed_by) or "manual",
            datetime.utcnow().isoformat(),
        ),
    )


def _payment_assignment_snapshot(conn, payment_id):
    pay = conn.execute(
        "SELECT payment_id, invoice_id, matched, match_rule FROM payments WHERE payment_id = ?",
        (payment_id,),
    ).fetchone()
    if not pay:
        return "—"

    children = conn.execute(
        """
        SELECT invoice_id, amount_eur
        FROM payments
        WHERE parent_payment_id = ?
        ORDER BY payment_id ASC
        """,
        (payment_id,),
    ).fetchall()
    if children:
        parts = []
        for row in children:
            inv_id = row["invoice_id"] if row["invoice_id"] is not None else "?"
            parts.append(f"{inv_id}={_format_eur_for_log(row['amount_eur'])}")
        return "Split: " + "; ".join(parts)

    if pay["invoice_id"]:
        return f"Rechnung #{pay['invoice_id']}"
    if int(pay["matched"] or 0) == 0:
        return "Keine Zuordnung"
    rule = str(pay["match_rule"] or "").strip()
    if rule:
        return f"Ohne Rechnung ({rule})"
    return "Ohne Rechnung"


def _month_label(month_key):
    months = {
        1: "Januar",
        2: "Februar",
        3: "März",
        4: "April",
        5: "Mai",
        6: "Juni",
        7: "Juli",
        8: "August",
        9: "September",
        10: "Oktober",
        11: "November",
        12: "Dezember",
    }
    try:
        year, month = str(month_key).split("-", 1)
        month_no = int(month)
        year_no = int(year)
        if month_no < 1 or month_no > 12:
            raise ValueError()
        return f"{months.get(month_no, month_no)} {year_no}"
    except Exception:
        return str(month_key or "")


def _normalize_steuerbuero_origin(value, default="auto"):
    raw = str(value or "").strip().lower()
    if raw in {"manual", "manuell"}:
        return "manual"
    if raw in {"auto", "automatic", "automatisch", "generated", "system"}:
        return "auto"
    return default


def _parse_steuerbuero_changed_at(raw_value, fallback_iso=None):
    value = str(raw_value or "").strip()
    if not value:
        return fallback_iso or datetime.utcnow().isoformat()
    value = value.replace(" ", "T")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return fallback_iso or datetime.utcnow().isoformat()
    return parsed.isoformat()


def _parse_optional_int(raw_value):
    value = str(raw_value or "").strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _normalize_invoice_doc_type(value, default="rechnung"):
    normalized = str(value or "").strip().lower()
    if normalized in INVOICE_DOC_TYPES:
        return normalized
    return default


def _invoice_doc_type_from_row(row, default="rechnung"):
    if row is None:
        return default
    try:
        if "document_type" in row.keys():
            return _normalize_invoice_doc_type(row["document_type"], default=default)
    except Exception:
        pass
    if isinstance(row, dict):
        return _normalize_invoice_doc_type(row.get("document_type"), default=default)
    return default


def _payment_effective_status_sql(alias="payments"):
    """SQL CASE expression for effective payment status (manual override aware)."""
    return f"""
        CASE
            WHEN COALESCE({alias}.status_manual, 0) = 1
                 AND COALESCE({alias}.status_override, '') <> ''
            THEN {alias}.status_override
            WHEN COALESCE({alias}.amount_eur, 0) < 0 THEN 'Zugeordnet'
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
        if override:
            return override

    if float((payment["amount_eur"] if "amount_eur" in payment.keys() else 0) or 0) < 0:
        return "Zugeordnet"
    if int((payment["matched"] if "matched" in payment.keys() else 0) or 0) == 1:
        return "Zugeordnet"
    if int((payment["akonto"] if "akonto" in payment.keys() else 0) or 0) == 1:
        return "Akonto"
    if int((payment["schadensrechnung"] if "schadensrechnung" in payment.keys() else 0) or 0) == 1:
        return "Schadensrechnungen"
    return "Offen"


@app.context_processor
def _inject_template_helpers():
    status_cfg = _status_options_from_params()

    def status_inline_style(status, kind="invoice"):
        return _status_badge_inline_style(status, kind, status_cfg)

    return {
        "payment_effective_status": payment_effective_status,
        "invoice_status_options": status_cfg["invoice_statuses"],
        "payment_status_options": status_cfg["payment_statuses"],
        "invoice_status_filter_options": status_cfg["invoice_statuses"],
        "status_inline_style": status_inline_style,
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


def _split_name_tokens(value):
    normalized = _normalize_ascii_key(value)
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    tokens = []
    for token in normalized.split():
        if len(token) < 3:
            continue
        if token in SPLIT_NAME_STOPWORDS:
            continue
        tokens.append(token)
    return tokens


def _is_structured_invoice_reference_12(raw):
    """12-stellige Referenz: YY + 4 Stellen + '1' + 5 Stellen => Rechnung = erste 6."""
    if not raw or not re.fullmatch(r"\d{12}", str(raw)):
        return False
    if str(raw)[6] != "1":
        return False
    yy = int(str(raw)[:2])
    current_yy = datetime.now().year % 100
    return 20 <= yy <= min(99, current_yy + 2)


def _extract_reference_numbers(value):
    numbers = set()
    for match in re.finditer(r"\b(\d{4,12})\b", str(value or "")):
        try:
            raw = match.group(1)
            numbers.add(int(raw))
            if _is_structured_invoice_reference_12(raw):
                numbers.add(int(raw[:6]))
        except Exception:
            continue
    return numbers


def _parse_positive_int(raw_value, default_value, minimum=1, maximum=80):
    try:
        value = int(raw_value)
    except Exception:
        value = default_value
    return max(minimum, min(maximum, value))


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
    invoice_base_filter = "COALESCE(document_type, 'rechnung') = 'rechnung'"
    stats = {
        "total_invoices": conn.execute(
            f"SELECT COUNT(*) FROM invoices WHERE {invoice_base_filter}"
        ).fetchone()[0],
        "open_invoices": conn.execute(
            """
            SELECT COUNT(*)
            FROM invoices
            WHERE COALESCE(document_type, 'rechnung') = 'rechnung'
              AND LOWER(TRIM(COALESCE(status, 'Offen'))) IN ('offen', 'in klärung')
              AND LOWER(TRIM(COALESCE(status, 'Offen'))) NOT IN ('ausgebucht', 'skonto')
            """
        ).fetchone()[0],
        "partial_invoices": conn.execute(
            """
            SELECT COUNT(*)
            FROM invoices
            WHERE COALESCE(document_type, 'rechnung') = 'rechnung'
              AND status = 'Teiloffen/Unterzahlung'
            """
        ).fetchone()[0],
        "paid_invoices": conn.execute(
            """
            SELECT COUNT(*)
            FROM invoices
            WHERE COALESCE(document_type, 'rechnung') = 'rechnung'
              AND status IN ('Bezahlt', 'Bezahlt mit Mahngebühr', 'Gutschrift')
            """
        ).fetchone()[0],
        "overpaid_invoices": conn.execute(
            """
            SELECT COUNT(*)
            FROM invoices
            WHERE COALESCE(document_type, 'rechnung') = 'rechnung'
              AND status = 'Überzahlung'
            """
        ).fetchone()[0],
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
        "mahnung_1": conn.execute(
            """
            SELECT COUNT(*)
            FROM invoices
            WHERE COALESCE(document_type, 'rechnung') = 'rechnung'
              AND reminder_status = '1. Mahnung'
            """
        ).fetchone()[0],
        "mahnung_2": conn.execute(
            """
            SELECT COUNT(*)
            FROM invoices
            WHERE COALESCE(document_type, 'rechnung') = 'rechnung'
              AND reminder_status = '2. Mahnung'
            """
        ).fetchone()[0],
        "mahnung_3": conn.execute(
            """
            SELECT COUNT(*)
            FROM invoices
            WHERE COALESCE(document_type, 'rechnung') = 'rechnung'
              AND reminder_status = '3. Mahnung'
            """
        ).fetchone()[0],
        "open_sum": conn.execute(
            """
            SELECT COALESCE(SUM(amount_gross - COALESCE(paid_sum_eur, 0)), 0)
            FROM invoices
            WHERE LOWER(TRIM(COALESCE(status, 'Offen'))) IN ('offen', 'in klärung')
              AND COALESCE(document_type, 'rechnung') = 'rechnung'
              AND LOWER(TRIM(COALESCE(status, 'Offen'))) NOT IN (
                'bezahlt',
                'bezahlt mit mahngebühr',
                'ausgebucht',
                'skonto'
            )
            """
        ).fetchone()[0],
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
    status_cfg = _status_options_from_params(params)
    custom_invoice_statuses_list = [
        s for s in status_cfg["invoice_statuses"] if s not in MANUAL_INVOICE_STATUSES
    ]
    custom_payment_statuses_list = [
        s for s in status_cfg["payment_statuses"] if s not in MANUAL_PAYMENT_STATUSES
    ]
    custom_invoice_statuses_text = "\n".join(custom_invoice_statuses_list)
    custom_payment_statuses_text = "\n".join(custom_payment_statuses_list)
    custom_invoice_status_colors_text = _status_color_map_to_text(
        status_cfg["invoice_statuses"], status_cfg.get("invoice_status_colors", {})
    )
    custom_payment_status_colors_text = _status_color_map_to_text(
        status_cfg["payment_statuses"], status_cfg.get("payment_status_colors", {})
    )
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

        try:
            custom_invoice_statuses = _parse_custom_statuses_input(
                request.form.get("custom_invoice_statuses", "")
            )
            custom_payment_statuses = _parse_custom_statuses_input(
                request.form.get("custom_payment_statuses", "")
            )
            allowed_invoice_statuses = _status_sort_unique(
                MANUAL_INVOICE_STATUS_ORDER + custom_invoice_statuses
            )
            allowed_payment_statuses = _status_sort_unique(
                MANUAL_PAYMENT_STATUS_ORDER + custom_payment_statuses
            )
            custom_invoice_status_colors = _parse_custom_status_colors_input(
                request.form.get("custom_invoice_status_colors", ""),
                allowed_invoice_statuses,
                strict=True,
            )
            custom_payment_status_colors = _parse_custom_status_colors_input(
                request.form.get("custom_payment_status_colors", ""),
                allowed_payment_statuses,
                strict=True,
            )
        except ValueError as e:
            flash(str(e), "error")
            return redirect(url_for("einstellungen"))

        params["mahngebuehr_1_eur"] = round(mahngebuehr_1, 2)
        params["mahngebuehr_2_eur"] = round(mahngebuehr_2, 2)
        params["mahngebuehr_3_eur"] = round(mahngebuehr_3, 2)
        # Legacy-Parameter für Abwärtskompatibilität
        params["mahngebuehr_eur"] = round(mahngebuehr_1, 2)
        params["custom_invoice_statuses"] = custom_invoice_statuses
        params["custom_payment_statuses"] = custom_payment_statuses
        params["custom_invoice_status_colors"] = custom_invoice_status_colors
        params["custom_payment_status_colors"] = custom_payment_status_colors
        try:
            _save_app_params(params)
            update_all()
            flash("✅ Einstellungen gespeichert. Status wurde neu berechnet.", "success")
        except Exception as e:
            flash(f"Fehler beim Speichern der Einstellungen: {e}", "error")
        return redirect(url_for("einstellungen"))

    return render_template(
        "einstellungen.html",
        params=params,
        status_cfg=status_cfg,
        custom_invoice_statuses_list=custom_invoice_statuses_list,
        custom_payment_statuses_list=custom_payment_statuses_list,
        custom_invoice_statuses_text=custom_invoice_statuses_text,
        custom_payment_statuses_text=custom_payment_statuses_text,
        custom_invoice_status_colors_text=custom_invoice_status_colors_text,
        custom_payment_status_colors_text=custom_payment_status_colors_text,
    )


# ---------------------------------------------------------------------------
# Rechnungen (Invoices)
# ---------------------------------------------------------------------------

@app.route("/rechnungen")
def rechnungen():
    conn = get_db()
    status_cfg = _status_options_from_params()
    doc_type = _normalize_invoice_doc_type(request.args.get("doc_type"), default="rechnung")
    sort_key_prefix = f"rechnungen_{doc_type}"
    # Persist sorting preference across navigation/tab changes (per doc-type tab).
    saved_sort_col = session.get(f"{sort_key_prefix}_sort_col", "invoice_id")
    saved_order = session.get(f"{sort_key_prefix}_order", "asc")

    status_filter = request.args.get("status", "")
    search = request.args.get("q", "").strip()
    page = max(1, int(request.args.get("page", 1)))
    sort_col = request.args.get("sort", saved_sort_col)
    order = request.args.get("order", saved_order).lower()
    per_page = int(request.args.get("per_page", 500))
    if per_page not in [20, 50, 100, 200, 500]:
        per_page = 500

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

    session[f"{sort_key_prefix}_sort_col"] = sort_col
    session[f"{sort_key_prefix}_order"] = order

    query = "SELECT * FROM invoices WHERE COALESCE(document_type, 'rechnung') = ?"
    params = [doc_type]
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
    count_query = "SELECT COUNT(*) FROM invoices WHERE COALESCE(document_type, 'rechnung') = ?"
    count_params = [doc_type]
    if status_filter:
        count_query += " AND status = ?"
        count_params.append(status_filter)
    if search:
        count_query += " AND (CAST(invoice_id AS TEXT) LIKE ? OR name LIKE ? OR COALESCE(remark, '') LIKE ?)"
        count_params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])
    total = conn.execute(count_query, count_params).fetchone()[0]

    counts = {"rechnung": 0, "gutschrift": 0}
    for row in conn.execute(
        """
        SELECT COALESCE(document_type, 'rechnung') AS doc_type, COUNT(*) AS cnt
        FROM invoices
        GROUP BY COALESCE(document_type, 'rechnung')
        """
    ).fetchall():
        counts[_normalize_invoice_doc_type(row["doc_type"], default="rechnung")] = int(row["cnt"] or 0)
    conn.close()

    total_pages = max(1, (total + per_page - 1) // per_page)
    invoice_status_filter_options = _status_options_with_current(
        status_cfg["invoice_statuses"], status_filter
    )
    return render_template(
        "rechnungen.html",
        invoices=invoices,
        page=page,
        total_pages=total_pages,
        total=total,
        status_filter=status_filter,
        search=search,
        sort_col=sort_col,
        order=order,
        per_page=per_page,
        doc_type=doc_type,
        rechnung_count=counts["rechnung"],
        gutschrift_count=counts["gutschrift"],
        invoice_status_filter_options=invoice_status_filter_options,
    )


@app.route("/rechnungen/<int:invoice_id>")
def rechnung_detail(invoice_id):
    status_cfg = _status_options_from_params()
    conn = get_db()
    inv = conn.execute("SELECT * FROM invoices WHERE invoice_id = ?", (invoice_id,)).fetchone()
    if not inv:
        conn.close()
        flash("Rechnung nicht gefunden.", "error")
        return redirect(url_for("rechnungen"))
    inv_doc_type = _invoice_doc_type_from_row(inv, default="rechnung")
    list_doc_type = _normalize_invoice_doc_type(
        request.args.get("doc_type"),
        default=inv_doc_type,
    )
    payments = conn.execute(
        "SELECT * FROM payments WHERE invoice_id = ? ORDER BY booking_date DESC", (invoice_id,)
    ).fetchall()
    linked_credit_notes = conn.execute(
        """
        SELECT invoice_id, name, amount_gross, issue_date, status, remark
        FROM invoices
        WHERE COALESCE(document_type, 'rechnung') = 'gutschrift'
          AND credit_target_invoice_id = ?
        ORDER BY COALESCE(issue_date, updated_at, created_at) DESC, invoice_id DESC
        """,
        (invoice_id,),
    ).fetchall()
    credit_target_invoice = None
    if inv_doc_type == "gutschrift" and inv["credit_target_invoice_id"]:
        credit_target_invoice = conn.execute(
            "SELECT * FROM invoices WHERE invoice_id = ?",
            (inv["credit_target_invoice_id"],),
        ).fetchone()
    audit = conn.execute(
        "SELECT * FROM audit_log WHERE invoice_id = ? ORDER BY audit_id DESC", (invoice_id,)
    ).fetchall()
    reminder_history = fetch_invoice_reminder_history(conn, invoice_id, invoice_row=inv)
    conn.close()
    invoice_status_options = _status_options_with_current(
        status_cfg["invoice_statuses"], inv["status"] if inv else ""
    )
    return render_template(
        "rechnung_detail.html",
        inv=inv,
        inv_doc_type=inv_doc_type,
        list_doc_type=list_doc_type,
        payments=payments,
        linked_credit_notes=linked_credit_notes,
        credit_target_invoice=credit_target_invoice,
        audit=audit,
        reminder_history=reminder_history,
        invoice_status_options=invoice_status_options,
    )


@app.route("/rechnungen/<int:invoice_id>/bemerkung", methods=["POST"])
def rechnung_update_bemerkung(invoice_id):
    raw_remark = request.form.get("remark", "")
    remark = str(raw_remark or "").strip()
    if len(remark) > 2000:
        flash("Bemerkung ist zu lang (max. 2000 Zeichen).", "error")
        return _redirect_to_next("rechnung_detail", invoice_id=invoice_id)

    conn = get_db()
    try:
        row = conn.execute(
            "SELECT invoice_id, remark FROM invoices WHERE invoice_id = ?",
            (invoice_id,),
        ).fetchone()
        if not row:
            flash("Rechnung nicht gefunden.", "error")
            return _redirect_to_next("rechnungen")

        before_remark = str(row["remark"] or "").strip()
        after_remark = remark

        conn.execute(
            """
            UPDATE invoices
            SET remark = ?,
                updated_at = ?
            WHERE invoice_id = ?
            """,
            (remark if remark else None, datetime.utcnow().isoformat(), invoice_id),
        )
        if before_remark != after_remark:
            _log_manual_change(
                conn,
                action_code="invoice_remark_update",
                action_label="Rechnungs-Bemerkung geändert",
                change_scope="invoice",
                invoice_id=invoice_id,
                before_value=before_remark or "leer",
                after_value=after_remark or "leer",
            )
        conn.commit()
        flash("✅ Bemerkung gespeichert.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Fehler beim Speichern der Bemerkung: {e}", "error")
    finally:
        conn.close()

    return _redirect_to_next("rechnung_detail", invoice_id=invoice_id)


@app.route("/rechnungen/<int:invoice_id>/betrag", methods=["POST"])
def rechnung_update_betrag(invoice_id):
    raw_amount = (request.form.get("amount_gross") or "").strip()
    try:
        amount = _parse_eur(raw_amount)
    except ValueError:
        flash("Betrag ist ungültig (z.B. 1144,78).", "error")
        return _redirect_to_next("rechnung_detail", invoice_id=invoice_id)

    if amount < 0:
        flash("Rechnungsbetrag darf nicht negativ sein.", "error")
        return _redirect_to_next("rechnung_detail", invoice_id=invoice_id)

    conn = get_db()
    try:
        row = conn.execute(
            "SELECT invoice_id, amount_gross FROM invoices WHERE invoice_id = ?",
            (invoice_id,),
        ).fetchone()
        if not row:
            flash("Rechnung nicht gefunden.", "error")
            return _redirect_to_next("rechnungen")

        before_amount = float(row["amount_gross"] or 0.0)

        conn.execute(
            """
            UPDATE invoices
            SET amount_gross = ?,
                updated_at = ?
            WHERE invoice_id = ?
            """,
            (amount, datetime.utcnow().isoformat(), invoice_id),
        )
        if round(before_amount, 2) != round(amount, 2):
            _log_manual_change(
                conn,
                action_code="invoice_amount_update",
                action_label="Rechnungsbetrag manuell geändert",
                change_scope="invoice",
                invoice_id=invoice_id,
                before_value=_format_eur_for_log(before_amount),
                after_value=_format_eur_for_log(amount),
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        flash(f"Fehler beim Speichern des Rechnungsbetrags: {e}", "error")
        return _redirect_to_next("rechnung_detail", invoice_id=invoice_id)
    finally:
        conn.close()

    try:
        update_all()
        flash("✅ Rechnungsbetrag gespeichert und Status neu berechnet.", "success")
    except Exception as e:
        flash(f"Rechnungsbetrag gespeichert, aber Status-Neuberechnung fehlgeschlagen: {e}", "error")

    return _redirect_to_next("rechnung_detail", invoice_id=invoice_id)


@app.route("/rechnungen/<int:invoice_id>/typ", methods=["POST"])
def rechnung_update_typ(invoice_id):
    target_doc_type = _normalize_invoice_doc_type(request.form.get("document_type"), default="")
    if target_doc_type not in INVOICE_DOC_TYPES:
        flash("Ungültiger Dokumenttyp.", "error")
        return _redirect_to_next("rechnung_detail", invoice_id=invoice_id)

    conn = get_db()
    try:
        inv = conn.execute(
            "SELECT * FROM invoices WHERE invoice_id = ?",
            (invoice_id,),
        ).fetchone()
        if not inv:
            flash("Rechnung nicht gefunden.", "error")
            return _redirect_to_next("rechnungen")

        current_doc_type = _invoice_doc_type_from_row(inv, default="rechnung")
        if current_doc_type == target_doc_type:
            flash("Dokumenttyp ist bereits gesetzt.", "info")
            return _redirect_to_next("rechnung_detail", invoice_id=invoice_id, doc_type=target_doc_type)

        now = datetime.utcnow().isoformat()
        if target_doc_type == "gutschrift":
            conn.execute(
                """
                UPDATE invoices
                SET document_type = 'gutschrift',
                    status = 'Gutschrift',
                    status_manual = 0,
                    credit_target_invoice_id = NULL,
                    reminder_status = NULL,
                    reminder_date = NULL,
                    reminder_manual = 0,
                    updated_at = ?
                WHERE invoice_id = ?
                """,
                (now, invoice_id),
            )
            conn.execute("DELETE FROM invoice_reminders WHERE invoice_id = ?", (invoice_id,))
        else:
            conn.execute(
                """
                UPDATE invoices
                SET document_type = 'rechnung',
                    credit_target_invoice_id = NULL,
                    status_manual = 0,
                    updated_at = ?
                WHERE invoice_id = ?
                """,
                (now, invoice_id),
            )

        _log_manual_change(
            conn,
            action_code="invoice_document_type_update",
            action_label="Dokumenttyp geändert",
            change_scope="invoice",
            invoice_id=invoice_id,
            before_value=current_doc_type,
            after_value=target_doc_type,
        )

        conn.commit()
    except Exception as e:
        conn.rollback()
        flash(f"Fehler beim Umstellen des Dokumenttyps: {e}", "error")
        return _redirect_to_next("rechnung_detail", invoice_id=invoice_id)
    finally:
        conn.close()

    try:
        update_all()
    except Exception:
        pass

    if target_doc_type == "gutschrift":
        flash("✅ Dokument als Gutschrift markiert.", "success")
    else:
        flash("✅ Dokument wieder als Rechnung markiert.", "success")
    return _redirect_to_next("rechnung_detail", invoice_id=invoice_id, doc_type=target_doc_type)


@app.route("/rechnungen/<int:invoice_id>/gutschrift-zuordnung", methods=["POST"])
def rechnung_update_gutschrift_zuordnung(invoice_id):
    target_raw = (request.form.get("target_invoice_id") or "").strip()
    changed = False

    conn = get_db()
    try:
        source = conn.execute(
            """
            SELECT invoice_id, document_type, credit_target_invoice_id
            FROM invoices
            WHERE invoice_id = ?
            """,
            (invoice_id,),
        ).fetchone()
        if not source:
            flash("Gutschrift nicht gefunden.", "error")
            return _redirect_to_next("rechnungen", doc_type="gutschrift")

        source_doc_type = _invoice_doc_type_from_row(source, default="rechnung")
        if source_doc_type != "gutschrift":
            flash("Zuordnung ist nur für Dokumenttyp 'Gutschrift' erlaubt.", "error")
            return _redirect_to_next("rechnung_detail", invoice_id=invoice_id)

        now = datetime.utcnow().isoformat()
        before_target = source["credit_target_invoice_id"]
        if not target_raw:
            conn.execute(
                """
                UPDATE invoices
                SET credit_target_invoice_id = NULL,
                    updated_at = ?
                WHERE invoice_id = ?
                """,
                (now, invoice_id),
            )
            if before_target is not None:
                _log_manual_change(
                    conn,
                    action_code="credit_note_assignment_update",
                    action_label="Gutschrift-Zuordnung geändert",
                    change_scope="invoice",
                    invoice_id=invoice_id,
                    before_value=f"Rechnung #{before_target}",
                    after_value="keine Zuordnung",
                )
            conn.commit()
            changed = True
            flash("✅ Gutschrift-Zuordnung entfernt.", "success")

        if target_raw:
            try:
                target_invoice_id = int(target_raw)
            except ValueError:
                flash("Ziel-Rechnungsnummer muss eine Zahl sein.", "error")
                return _redirect_to_next("rechnung_detail", invoice_id=invoice_id, doc_type="gutschrift")

            if target_invoice_id == invoice_id:
                flash("Selbstzuordnung ist nicht erlaubt.", "error")
                return _redirect_to_next("rechnung_detail", invoice_id=invoice_id, doc_type="gutschrift")

            target = conn.execute(
                """
                SELECT invoice_id, document_type
                FROM invoices
                WHERE invoice_id = ?
                """,
                (target_invoice_id,),
            ).fetchone()
            if not target:
                flash(f"Ziel-Rechnung #{target_invoice_id} existiert nicht.", "error")
                return _redirect_to_next("rechnung_detail", invoice_id=invoice_id, doc_type="gutschrift")

            target_doc_type = _invoice_doc_type_from_row(target, default="rechnung")
            if target_doc_type != "rechnung":
                flash("Gutschriften dürfen nur normalen Rechnungen zugeordnet werden.", "error")
                return _redirect_to_next("rechnung_detail", invoice_id=invoice_id, doc_type="gutschrift")

            conn.execute(
                """
                UPDATE invoices
                SET credit_target_invoice_id = ?,
                    updated_at = ?
                WHERE invoice_id = ?
                """,
                (target_invoice_id, now, invoice_id),
            )
            if before_target != target_invoice_id:
                _log_manual_change(
                    conn,
                    action_code="credit_note_assignment_update",
                    action_label="Gutschrift-Zuordnung geändert",
                    change_scope="invoice",
                    invoice_id=invoice_id,
                    before_value=(f"Rechnung #{before_target}" if before_target else "keine Zuordnung"),
                    after_value=f"Rechnung #{target_invoice_id}",
                )
            conn.commit()
            changed = True
            flash(f"✅ Gutschrift auf Rechnung #{target_invoice_id} zugeordnet.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Fehler beim Speichern der Gutschrift-Zuordnung: {e}", "error")
    finally:
        conn.close()

    if changed:
        try:
            update_all()
        except Exception:
            pass

    return _redirect_to_next("rechnung_detail", invoice_id=invoice_id, doc_type="gutschrift")


@app.route("/rechnungen/<int:invoice_id>/status", methods=["POST"])
def rechnung_update_status(invoice_id):
    status = (request.form.get("status") or "").strip()
    allowed_statuses = set(_status_options_from_params()["invoice_statuses"])
    if status not in allowed_statuses:
        flash("Ungültiger Rechnungsstatus.", "error")
        return _redirect_to_next("rechnung_detail", invoice_id=invoice_id)

    conn = get_db()
    try:
        row = conn.execute(
            "SELECT invoice_id, status, status_manual FROM invoices WHERE invoice_id = ?",
            (invoice_id,),
        ).fetchone()
        if not row:
            flash("Rechnung nicht gefunden.", "error")
            return redirect(url_for("rechnungen"))

        before_status = str(row["status"] or "").strip() or "—"

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
        _log_manual_change(
            conn,
            action_code="invoice_status_manual_set",
            action_label="Rechnungsstatus manuell gesetzt",
            change_scope="invoice",
            invoice_id=invoice_id,
            before_value=before_status,
            after_value=status,
        )
        conn.commit()
        flash("✅ Rechnungsstatus manuell gespeichert.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Fehler beim Speichern des Rechnungsstatus: {e}", "error")
    finally:
        conn.close()

    return _redirect_to_next("rechnung_detail", invoice_id=invoice_id)


@app.route("/rechnungen/<int:invoice_id>/status/auto", methods=["POST"])
def rechnung_reset_status_auto(invoice_id):
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT invoice_id, status, status_manual FROM invoices WHERE invoice_id = ?",
            (invoice_id,),
        ).fetchone()
        if not row:
            flash("Rechnung nicht gefunden.", "error")
            return redirect(url_for("rechnungen"))
        before_manual = int(row["status_manual"] or 0)
        before_status = str(row["status"] or "").strip() or "—"
        conn.execute(
            """
            UPDATE invoices
            SET status_manual = 0,
                updated_at = ?
            WHERE invoice_id = ?
            """,
            (datetime.utcnow().isoformat(), invoice_id),
        )
        if before_manual == 1:
            _log_manual_change(
                conn,
                action_code="invoice_status_manual_reset",
                action_label="Rechnungsstatus auf Automatik zurückgesetzt",
                change_scope="invoice",
                invoice_id=invoice_id,
                before_value=before_status,
                after_value="Automatik aktiv",
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        flash(f"Fehler beim Aktivieren der Status-Automatik: {e}", "error")
        return _redirect_to_next("rechnung_detail", invoice_id=invoice_id)
    finally:
        conn.close()

    try:
        update_all()
    except Exception:
        pass
    flash("✅ Status-Automatik für diese Rechnung wieder aktiviert.", "success")
    return _redirect_to_next("rechnung_detail", invoice_id=invoice_id)


@app.route("/rechnungen/<int:invoice_id>/mahnung", methods=["POST"])
def rechnung_update_mahnung(invoice_id):
    allowed_statuses = {"", "1. Mahnung", "2. Mahnung", "3. Mahnung"}
    reminder_status = (request.form.get("reminder_status") or "").strip()
    reminder_date_raw = (request.form.get("reminder_date") or "").strip()

    if reminder_status not in allowed_statuses:
        flash("Ungültiger Mahnungsstatus.", "error")
        return _redirect_to_next("rechnung_detail", invoice_id=invoice_id)

    reminder_date = None
    if reminder_date_raw:
        try:
            reminder_date = datetime.fromisoformat(reminder_date_raw).date().isoformat()
        except ValueError:
            flash("Mahnungsdatum muss ein gültiges Datum sein.", "error")
            return _redirect_to_next("rechnung_detail", invoice_id=invoice_id)

    # Bei "Keine Mahnung" wird ein ggf. übergebenes Datum ignoriert.
    if not reminder_status:
        reminder_date = None

    if reminder_status and not reminder_date:
        reminder_date = datetime.utcnow().date().isoformat()

    conn = get_db()
    try:
        row = conn.execute(
            "SELECT invoice_id, reminder_status, reminder_date, reminder_manual FROM invoices WHERE invoice_id = ?",
            (invoice_id,),
        ).fetchone()
        if not row:
            flash("Rechnung nicht gefunden.", "error")
            return _redirect_to_next("rechnungen")

        before_state = {
            "status": row["reminder_status"] or "",
            "datum": row["reminder_date"] or "",
            "manuell": int(row["reminder_manual"] or 0),
        }

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

        after_state = {
            "status": reminder_status or "",
            "datum": reminder_date or "",
            "manuell": 1,
        }
        if before_state != after_state:
            _log_manual_change(
                conn,
                action_code="invoice_reminder_manual_update",
                action_label="Mahnstatus manuell geändert",
                change_scope="invoice",
                invoice_id=invoice_id,
                before_value=before_state,
                after_value=after_state,
            )
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

    return _redirect_to_next("rechnung_detail", invoice_id=invoice_id)


@app.route("/rechnungen/<int:invoice_id>/mahnung/auto", methods=["POST"])
def rechnung_reset_mahnung_auto(invoice_id):
    conn = get_db()
    try:
        row = conn.execute(
            "SELECT invoice_id, reminder_manual FROM invoices WHERE invoice_id = ?",
            (invoice_id,),
        ).fetchone()
        if not row:
            flash("Rechnung nicht gefunden.", "error")
            return _redirect_to_next("rechnungen")

        before_manual = int(row["reminder_manual"] or 0)

        conn.execute(
            """
            UPDATE invoices
            SET reminder_manual = 0,
                updated_at = ?
            WHERE invoice_id = ?
            """,
            (datetime.utcnow().isoformat(), invoice_id),
        )
        if before_manual == 1:
            _log_manual_change(
                conn,
                action_code="invoice_reminder_manual_reset",
                action_label="Mahn-Automatik wieder aktiviert",
                change_scope="invoice",
                invoice_id=invoice_id,
                before_value="manuell",
                after_value="automatisch",
            )
        conn.commit()
        flash("✅ Automatischen Mahnlauf für diese Rechnung wieder aktiviert.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Fehler beim Aktivieren der Automatik: {e}", "error")
    finally:
        conn.close()

    return _redirect_to_next("rechnung_detail", invoice_id=invoice_id)


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
    per_page = int(request.args.get("per_page", 500))
    if per_page not in [20, 50, 100, 200, 500]:
        per_page = 500
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
        SELECT payments.*,
               invoices.remark AS invoice_remark,
               COALESCE(invoices.document_type, 'rechnung') AS invoice_document_type
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
    status_cfg = _status_options_from_params()
    conn = get_db()
    pay = conn.execute("SELECT * FROM payments WHERE payment_id = ?", (payment_id,)).fetchone()
    if not pay:
        conn.close()
        flash("Zahlung nicht gefunden.", "error")
        return redirect(url_for("zahlungen"))
    
    # If matched to a single invoice, fetch the corresponding invoice details
    inv = None
    inv_doc_type = "rechnung"
    if pay["invoice_id"]:
        inv = conn.execute("SELECT * FROM invoices WHERE invoice_id = ?", (pay["invoice_id"],)).fetchone()
        inv_doc_type = _invoice_doc_type_from_row(inv, default="rechnung")

    # Child allocations for split parent payments
    split_invoices = conn.execute(
        """
        SELECT p.payment_id, p.parent_payment_id, p.invoice_id, p.amount_eur, p.match_score,
               i.name AS invoice_name, i.amount_gross AS invoice_amount_gross,
               i.status AS invoice_status, i.reminder_status AS invoice_reminder_status,
               COALESCE(i.document_type, 'rechnung') AS invoice_document_type,
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
    payment_status_options = _status_options_with_current(
        status_cfg["payment_statuses"],
        payment_effective_status(pay),
    )
    return render_template(
        "zahlung_detail.html",
        pay=pay,
        inv=inv,
        inv_doc_type=inv_doc_type,
        split_invoices=split_invoices,
        parent_payment=parent_payment,
        audit=audit,
        payment_status_options=payment_status_options,
    )


@app.route("/zahlungen/<int:payment_id>/manual/split/candidates")
def zahlung_manual_split_candidates(payment_id):
    query = (request.args.get("q") or "").strip()
    query_key = _normalize_ascii_key(query)
    limit = _parse_positive_int(request.args.get("limit"), 25, minimum=5, maximum=80)

    conn = get_db()
    try:
        pay = _resolve_editable_payment(conn, payment_id)
        if not pay:
            return jsonify({"ok": False, "error": "Zahlung nicht gefunden.", "candidates": []}), 404

        payment_amount_abs = round(abs(float(pay["amount_eur"] or 0.0)), 2)
        ref_invoice_numbers = _extract_reference_numbers(pay["reference_text"])
        payer_tokens = _split_name_tokens(pay["beneficiary_name"])

        like_term = f"%{query}%"
        rows = conn.execute(
            """
            SELECT invoice_id, name, amount_gross, paid_sum_eur, deviation_eur, status
            FROM invoices
            WHERE COALESCE(document_type, 'rechnung') = 'rechnung'
              AND (
                  ? = ''
                  OR CAST(invoice_id AS TEXT) LIKE ?
                  OR LOWER(COALESCE(name, '')) LIKE LOWER(?)
              )
            ORDER BY invoice_id DESC
            LIMIT 1200
            """,
            (query, like_term, like_term),
        ).fetchall()

        candidates = []
        for row in rows:
            invoice_id = int(row["invoice_id"])
            name = str(row["name"] or "").strip()
            status = str(row["status"] or "").strip() or "Offen"
            amount_gross = round(float(row["amount_gross"] or 0.0), 2)
            paid_sum = round(float(row["paid_sum_eur"] or 0.0), 2)
            open_amount = round(max(0.0, amount_gross - paid_sum), 2)

            score = 0
            reasons = []

            if invoice_id in ref_invoice_numbers:
                score += 120
                reasons.append("Rechnungsnummer im Verwendungszweck")

            normalized_name = _normalize_ascii_key(name)
            token_hits = [token for token in payer_tokens if token in normalized_name]
            if token_hits:
                score += min(45, len(set(token_hits)) * 15)
                reasons.append("Name ähnlich")

            if payment_amount_abs > 0:
                if abs(open_amount - payment_amount_abs) <= 0.01:
                    score += 80
                    reasons.append("Betrag passt exakt")
                elif 0 < open_amount < payment_amount_abs:
                    score += 28
                    reasons.append("Teilbetrag plausibel")
                elif 0 < open_amount <= (payment_amount_abs * 1.1):
                    score += 18
                    reasons.append("Betrag passt fast")

            status_key = status.strip().lower()
            if status_key in {"offen", "in klärung", "teiloffen/unterzahlung"}:
                score += 8
            elif status_key in {"bezahlt", "bezahlt mit mahngebuehr", "gutschrift", "ausgebucht", "skonto"}:
                score -= 18

            if query_key:
                if query_key.isdigit() and query_key in str(invoice_id):
                    score += 40
                elif query_key and query_key in normalized_name:
                    score += 30
                else:
                    query_tokens = [token for token in re.split(r"[^a-z0-9]+", query_key) if len(token) >= 3]
                    matches = [token for token in query_tokens if token in normalized_name]
                    if matches:
                        score += min(24, len(set(matches)) * 8)

            if not query and score <= 0:
                continue

            suggested_amount = 0.0
            if payment_amount_abs > 0 and open_amount > 0:
                suggested_amount = round(min(open_amount, payment_amount_abs), 2)

            candidates.append(
                {
                    "invoice_id": invoice_id,
                    "name": name or "—",
                    "status": status,
                    "amount_gross": amount_gross,
                    "paid_sum_eur": paid_sum,
                    "open_amount": open_amount,
                    "suggested_amount": suggested_amount,
                    "score": int(score),
                    "reasons": reasons[:3],
                }
            )

        candidates.sort(
            key=lambda item: (
                -item["score"],
                abs(item["open_amount"] - payment_amount_abs),
                -item["invoice_id"],
            )
        )
        candidates = candidates[:limit]

        return jsonify(
            {
                "ok": True,
                "payment_id": int(pay["payment_id"]),
                "payment_amount": payment_amount_abs,
                "query": query,
                "reference_invoice_ids": sorted([n for n in ref_invoice_numbers if n < 1000000000])[:30],
                "candidates": candidates,
            }
        )
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc), "candidates": []}), 500
    finally:
        conn.close()


@app.route("/zahlungen/<int:payment_id>/bemerkung", methods=["POST"])
def zahlung_update_bemerkung(payment_id):
    raw_remark = request.form.get("remark", "")
    remark = str(raw_remark or "").strip()
    if len(remark) > 2000:
        flash("Bemerkung ist zu lang (max. 2000 Zeichen).", "error")
        return _redirect_to_next("zahlung_detail", payment_id=payment_id)

    conn = get_db()
    target_id = payment_id
    try:
        pay = _resolve_editable_payment(conn, payment_id)
        if not pay:
            flash("Zahlung nicht gefunden.", "error")
            return _redirect_to_next("zahlungen")
        target_id = pay["payment_id"]
        before_remark = str(pay["remark"] or "").strip()

        value = remark if remark else None
        conn.execute(
            "UPDATE payments SET remark = ? WHERE payment_id = ?",
            (value, target_id),
        )
        conn.execute(
            "UPDATE payments SET remark = ? WHERE parent_payment_id = ?",
            (value, target_id),
        )
        if before_remark != remark:
            _log_manual_change(
                conn,
                action_code="payment_remark_update",
                action_label="Zahlungs-Bemerkung geändert",
                change_scope="payment",
                payment_id=target_id,
                before_value=before_remark or "leer",
                after_value=remark or "leer",
            )
        conn.commit()
        flash("✅ Bemerkung zur Zahlung gespeichert.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Fehler beim Speichern der Zahlungs-Bemerkung: {e}", "error")
    finally:
        conn.close()

    return _redirect_to_next("zahlung_detail", payment_id=target_id)


@app.route("/zahlungen/<int:payment_id>/status", methods=["POST"])
def zahlung_update_status(payment_id):
    manual_status = (request.form.get("status") or "").strip()
    allowed_statuses = set(_status_options_from_params()["payment_statuses"])
    if manual_status not in allowed_statuses:
        flash("Ungültiger Zahlungsstatus.", "error")
        return _redirect_to_next("zahlung_detail", payment_id=payment_id)

    conn = get_db()
    target_id = payment_id
    try:
        pay = _resolve_editable_payment(conn, payment_id)
        if not pay:
            flash("Zahlung nicht gefunden.", "error")
            return redirect(url_for("zahlungen"))
        target_id = pay["payment_id"]
        before_status = payment_effective_status(pay)

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
        if before_status != manual_status:
            _log_manual_change(
                conn,
                action_code="payment_status_manual_set",
                action_label="Zahlungsstatus manuell gesetzt",
                change_scope="payment",
                payment_id=target_id,
                before_value=before_status,
                after_value=manual_status,
            )
        conn.commit()
        flash("✅ Zahlungsstatus manuell gespeichert.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Fehler beim Speichern des Zahlungsstatus: {e}", "error")
    finally:
        conn.close()

    return _redirect_to_next("zahlung_detail", payment_id=target_id)


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
        before_manual = int(pay["status_manual"] or 0)
        before_status = payment_effective_status(pay)

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
        if before_manual == 1:
            _log_manual_change(
                conn,
                action_code="payment_status_manual_reset",
                action_label="Zahlungsstatus auf Automatik zurückgesetzt",
                change_scope="payment",
                payment_id=target_id,
                before_value=before_status,
                after_value="Automatik aktiv",
            )
        conn.commit()
        flash("✅ Zahlungsstatus wieder auf Automatik gesetzt.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Fehler beim Zurücksetzen des Zahlungsstatus: {e}", "error")
    finally:
        conn.close()

    return _redirect_to_next("zahlung_detail", payment_id=target_id)


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
        inv = conn.execute(
            "SELECT invoice_id, COALESCE(document_type, 'rechnung') AS document_type FROM invoices WHERE invoice_id = ?",
            (invoice_id,),
        ).fetchone()
        if not inv:
            flash(f"Rechnung #{invoice_id} existiert nicht.", "error")
            return redirect(url_for("zahlung_detail", payment_id=target_id))
        if _invoice_doc_type_from_row(inv, default="rechnung") != "rechnung":
            flash("Manuelle Zahlungszuordnung ist nur auf Dokumenttyp 'Rechnung' erlaubt.", "error")
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
            inv = conn.execute(
                "SELECT invoice_id, COALESCE(document_type, 'rechnung') AS document_type FROM invoices WHERE invoice_id = ?",
                (inv_id,),
            ).fetchone()
            if not inv:
                flash(f"Rechnung #{inv_id} existiert nicht.", "error")
                return redirect(url_for("zahlung_detail", payment_id=target_id))
            if _invoice_doc_type_from_row(inv, default="rechnung") != "rechnung":
                flash(f"Rechnung #{inv_id} ist eine Gutschrift und kann nicht als Zahlungsziel verwendet werden.", "error")
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
    try:
        import_batches = fetch_import_batches(limit=30)
    except Exception as exc:
        import_batches = []
        flash(f"Import-Historie konnte nicht geladen werden: {exc}", "error")
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
# Steuerbüro-Änderungen
# ---------------------------------------------------------------------------

@app.route("/steuerbuero")
def steuerbuero():
    selected_month = (request.args.get("month") or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}", selected_month):
        selected_month = datetime.utcnow().strftime("%Y-%m")
    status_filter = str(request.args.get("status") or "open").strip().lower()
    if status_filter not in {"open", "resolved", "all"}:
        status_filter = "open"

    excluded_codes = sorted(STEUERBUERO_EXCLUDED_ACTION_CODES)
    excluded_placeholders = ", ".join("?" for _ in excluded_codes)
    status_clause = ""
    if status_filter == "open":
        status_clause = " AND COALESCE(log.is_resolved, 0) = 0"
    elif status_filter == "resolved":
        status_clause = " AND COALESCE(log.is_resolved, 0) = 1"

    conn = get_db()
    month_rows = conn.execute(
        f"""
        SELECT
            log.*,
            COALESCE(i.document_type, 'rechnung') AS invoice_document_type
        FROM manual_change_log log
        LEFT JOIN invoices i ON i.invoice_id = log.invoice_id
        WHERE substr(log.changed_at, 1, 7) = ?
          AND COALESCE(log.action_code, '') NOT IN ({excluded_placeholders})
          {status_clause}
        ORDER BY log.changed_at DESC, log.change_id DESC
        """,
        [selected_month] + excluded_codes,
    ).fetchall()

    month_counts = conn.execute(
        f"""
        SELECT
            substr(changed_at, 1, 7) AS month_key,
            COUNT(*) AS cnt
        FROM manual_change_log
        WHERE COALESCE(action_code, '') NOT IN ({excluded_placeholders})
        GROUP BY substr(changed_at, 1, 7)
        ORDER BY month_key DESC
        LIMIT 48
        """,
        excluded_codes,
    ).fetchall()

    summary = conn.execute(
        f"""
        SELECT
            SUM(CASE WHEN COALESCE(is_resolved, 0) = 0 THEN 1 ELSE 0 END) AS open_count,
            SUM(CASE WHEN COALESCE(is_resolved, 0) = 1 THEN 1 ELSE 0 END) AS resolved_count
        FROM manual_change_log
        WHERE substr(changed_at, 1, 7) = ?
          AND COALESCE(action_code, '') NOT IN ({excluded_placeholders})
        """,
        [selected_month] + excluded_codes,
    ).fetchone()
    conn.close()

    month_options = [
        {
            "key": row["month_key"],
            "label": _month_label(row["month_key"]),
            "count": int(row["cnt"] or 0),
        }
        for row in month_counts
        if row["month_key"]
    ]

    if selected_month not in {entry["key"] for entry in month_options}:
        month_options.insert(
            0,
            {
                "key": selected_month,
                "label": _month_label(selected_month),
                "count": len(month_rows),
            },
        )

    return render_template(
        "steuerbuero.html",
        changes=month_rows,
        selected_month=selected_month,
        selected_month_label=_month_label(selected_month),
        month_options=month_options,
        status_filter=status_filter,
        open_count=int((summary["open_count"] if summary else 0) or 0),
        resolved_count=int((summary["resolved_count"] if summary else 0) or 0),
        default_changed_at=datetime.now().replace(second=0, microsecond=0).isoformat(timespec="minutes"),
        total=len(month_rows),
    )


@app.route("/steuerbuero/add", methods=["POST"])
def steuerbuero_add_entry():
    selected_month = (request.form.get("month") or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}", selected_month):
        selected_month = datetime.utcnow().strftime("%Y-%m")
    status_filter = str(request.form.get("status") or "open").strip().lower()
    if status_filter not in {"open", "resolved", "all"}:
        status_filter = "open"

    action_label = str(request.form.get("action_label") or "").strip()
    if not action_label:
        flash("Bitte eine Bezeichnung für die Änderung angeben.", "error")
        return redirect(url_for("steuerbuero", month=selected_month, status=status_filter))

    entry_origin = "manual"
    change_scope = str(request.form.get("change_scope") or "").strip() or "note"
    invoice_id = _parse_optional_int(request.form.get("invoice_id"))
    payment_id = _parse_optional_int(request.form.get("payment_id"))
    before_value = _serialize_change_value(request.form.get("before_value"))
    after_value = _serialize_change_value(request.form.get("after_value"))
    note = _serialize_change_value(request.form.get("note"))
    changed_at = _parse_steuerbuero_changed_at(request.form.get("changed_at"))
    resolved = 1 if request.form.get("is_resolved") else 0
    resolved_at = datetime.utcnow().isoformat() if resolved else None
    action_code = "manual_custom_entry"

    conn = get_db()
    try:
        conn.execute(
            """
            INSERT INTO manual_change_log(
                entry_origin, is_resolved, resolved_at,
                change_scope, invoice_id, payment_id,
                action_code, action_label,
                before_value, after_value, note, changed_by, changed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry_origin,
                resolved,
                resolved_at,
                change_scope,
                invoice_id,
                payment_id,
                action_code,
                action_label,
                before_value,
                after_value,
                note,
                "manual",
                changed_at,
            ),
        )
        conn.commit()
        flash("✅ Steuerbüro-Eintrag hinzugefügt.", "success")
    except Exception as exc:
        conn.rollback()
        flash(f"Fehler beim Hinzufügen des Eintrags: {exc}", "error")
    finally:
        conn.close()

    return redirect(url_for("steuerbuero", month=selected_month, status=status_filter))


@app.route("/steuerbuero/update", methods=["POST"])
def steuerbuero_update_entry():
    selected_month = (request.form.get("month") or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}", selected_month):
        selected_month = datetime.utcnow().strftime("%Y-%m")
    status_filter = str(request.form.get("status") or "open").strip().lower()
    if status_filter not in {"open", "resolved", "all"}:
        status_filter = "open"

    change_id_raw = (request.form.get("change_id") or "").strip()
    try:
        change_id = int(change_id_raw)
    except ValueError:
        flash("Ungültige Änderungs-ID.", "error")
        return redirect(url_for("steuerbuero", month=selected_month, status=status_filter))

    action_label = str(request.form.get("action_label") or "").strip()
    if not action_label:
        flash("Bitte eine Bezeichnung für die Änderung angeben.", "error")
        return redirect(url_for("steuerbuero", month=selected_month, status=status_filter))

    conn = get_db()
    try:
        existing = conn.execute(
            """
            SELECT change_id, action_code, changed_at, resolved_at
            FROM manual_change_log
            WHERE change_id = ?
            """,
            (change_id,),
        ).fetchone()
        if not existing:
            flash("Eintrag wurde nicht gefunden.", "error")
            return redirect(url_for("steuerbuero", month=selected_month, status=status_filter))

        change_scope = str(request.form.get("change_scope") or "").strip() or "note"
        invoice_id = _parse_optional_int(request.form.get("invoice_id"))
        payment_id = _parse_optional_int(request.form.get("payment_id"))
        before_value = _serialize_change_value(request.form.get("before_value"))
        after_value = _serialize_change_value(request.form.get("after_value"))
        note = _serialize_change_value(request.form.get("note"))
        changed_at = _parse_steuerbuero_changed_at(request.form.get("changed_at"), fallback_iso=existing["changed_at"])
        resolved = 1 if request.form.get("is_resolved") else 0
        resolved_at = existing["resolved_at"] if resolved else None
        if resolved and not resolved_at:
            resolved_at = datetime.utcnow().isoformat()

        conn.execute(
            """
            UPDATE manual_change_log
            SET is_resolved = ?,
                resolved_at = ?,
                change_scope = ?,
                invoice_id = ?,
                payment_id = ?,
                action_label = ?,
                before_value = ?,
                after_value = ?,
                note = ?,
                changed_by = ?,
                changed_at = ?
            WHERE change_id = ?
            """,
            (
                resolved,
                resolved_at,
                change_scope,
                invoice_id,
                payment_id,
                action_label,
                before_value,
                after_value,
                note,
                "manual_edit",
                changed_at,
                change_id,
            ),
        )
        conn.commit()
        flash(f"✅ Eintrag #{change_id} aktualisiert.", "success")
    except Exception as exc:
        conn.rollback()
        flash(f"Fehler beim Aktualisieren des Eintrags: {exc}", "error")
    finally:
        conn.close()

    return redirect(url_for("steuerbuero", month=selected_month, status=status_filter))


@app.route("/steuerbuero/toggle-resolved", methods=["POST"])
def steuerbuero_toggle_resolved():
    selected_month = (request.form.get("month") or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}", selected_month):
        selected_month = datetime.utcnow().strftime("%Y-%m")
    status_filter = str(request.form.get("status") or "open").strip().lower()
    if status_filter not in {"open", "resolved", "all"}:
        status_filter = "open"

    change_id_raw = (request.form.get("change_id") or "").strip()
    target_raw = (request.form.get("resolved") or "").strip()
    try:
        change_id = int(change_id_raw)
        target_state = 1 if target_raw == "1" else 0
    except ValueError:
        flash("Ungültige Änderungs-ID.", "error")
        return redirect(url_for("steuerbuero", month=selected_month, status=status_filter))

    conn = get_db()
    try:
        row = conn.execute(
            "SELECT change_id FROM manual_change_log WHERE change_id = ?",
            (change_id,),
        ).fetchone()
        if not row:
            flash("Eintrag wurde nicht gefunden oder bereits gelöscht.", "error")
            return redirect(url_for("steuerbuero", month=selected_month, status=status_filter))

        conn.execute(
            """
            UPDATE manual_change_log
            SET is_resolved = ?,
                resolved_at = ?,
                changed_by = ?
            WHERE change_id = ?
            """,
            (
                target_state,
                datetime.utcnow().isoformat() if target_state else None,
                "manual_check",
                change_id,
            ),
        )
        conn.commit()
        flash(
            f"✅ Eintrag #{change_id} {'abgehakt' if target_state else 'wieder als offen markiert'}.",
            "success",
        )
    except Exception as exc:
        conn.rollback()
        flash(f"Fehler beim Aktualisieren des Status: {exc}", "error")
    finally:
        conn.close()

    return redirect(url_for("steuerbuero", month=selected_month, status=status_filter))


@app.route("/steuerbuero/delete", methods=["POST"])
def steuerbuero_delete_entry():
    change_id_raw = (request.form.get("change_id") or "").strip()
    selected_month = (request.form.get("month") or "").strip()
    if not re.fullmatch(r"\d{4}-\d{2}", selected_month):
        selected_month = datetime.utcnow().strftime("%Y-%m")
    status_filter = str(request.form.get("status") or "open").strip().lower()
    if status_filter not in {"open", "resolved", "all"}:
        status_filter = "open"

    try:
        change_id = int(change_id_raw)
    except ValueError:
        flash("Ungültige Änderungs-ID.", "error")
        return redirect(url_for("steuerbuero", month=selected_month, status=status_filter))

    conn = get_db()
    try:
        row = conn.execute(
            "SELECT change_id FROM manual_change_log WHERE change_id = ?",
            (change_id,),
        ).fetchone()
        if not row:
            flash("Eintrag wurde nicht gefunden oder bereits gelöscht.", "error")
            return redirect(url_for("steuerbuero", month=selected_month, status=status_filter))

        conn.execute("DELETE FROM manual_change_log WHERE change_id = ?", (change_id,))
        conn.commit()
        flash(f"✅ Eintrag #{change_id} gelöscht.", "success")
    except Exception as exc:
        conn.rollback()
        flash(f"Fehler beim Löschen des Eintrags: {exc}", "error")
    finally:
        conn.close()

    return redirect(url_for("steuerbuero", month=selected_month, status=status_filter))


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
