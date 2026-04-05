"""Unit tests for status computation."""

import sys
from pathlib import Path
import sqlite3

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import src.status as status_module
import src.db as db_module
from src.status import compute_status_row
import app as app_module


def test_offen():
    status, dev = compute_status_row({"paid_sum_eur": 0.0, "amount_gross": 100.0}, 0.001)
    assert status == "Offen"
    assert dev == -100.0


def test_bezahlt_exact():
    status, _ = compute_status_row({"paid_sum_eur": 100.0, "amount_gross": 100.0}, 0.001)
    assert status == "Bezahlt"


def test_bezahlt_within_tolerance():
    status, _ = compute_status_row({"paid_sum_eur": 100.0004, "amount_gross": 100.0}, 0.001)
    assert status == "Bezahlt"


def test_teiloffen():
    status, _ = compute_status_row({"paid_sum_eur": 50.0, "amount_gross": 100.0}, 0.001)
    assert status == "Teiloffen/Unterzahlung"


def test_ueberzahlung():
    status, _ = compute_status_row({"paid_sum_eur": 110.0, "amount_gross": 100.0}, 0.001)
    assert status == "Überzahlung"


def test_bezahlt_mit_mahngebuehr_when_deviation_matches_fee():
    status, dev = compute_status_row(
        {"paid_sum_eur": 107.5, "amount_gross": 100.0},
        0.001,
        7.5,
    )
    assert status == "Bezahlt mit Mahngebühr"
    assert dev == 7.5


def test_bezahlt_mit_mahngebuehr_uses_fee_for_2nd_reminder():
    status, dev = compute_status_row(
        {"invoice_id": 260001, "reminder_status": "2. Mahnung", "paid_sum_eur": 112.5, "amount_gross": 100.0},
        0.001,
        7.5,
        12.5,
        20.0,
    )
    assert status == "Bezahlt mit Mahngebühr"
    assert dev == 12.5


def test_ueberzahlung_when_fee_does_not_match_reminder_stage():
    status, dev = compute_status_row(
        {"invoice_id": 260001, "reminder_status": "3. Mahnung", "paid_sum_eur": 112.5, "amount_gross": 100.0},
        0.001,
        7.5,
        12.5,
        20.0,
    )
    assert status == "Überzahlung"
    assert dev == 12.5


def test_bezahlt_mit_mahngebuehr_without_reminder_stage_matches_any_configured_fee():
    status, dev = compute_status_row(
        {"invoice_id": 260001, "paid_sum_eur": 109.0, "amount_gross": 100.0},
        0.001,
        6.0,
        9.0,
        12.0,
    )
    assert status == "Bezahlt mit Mahngebühr"
    assert dev == 9.0


def test_ueberzahlung_without_reminder_stage_when_deviation_matches_no_configured_fee():
    status, dev = compute_status_row(
        {"invoice_id": 260001, "paid_sum_eur": 111.0, "amount_gross": 100.0},
        0.001,
        6.0,
        9.0,
        12.0,
    )
    assert status == "Überzahlung"
    assert dev == 11.0


def test_akonto_for_9xxxxx_invoice():
    status, dev = compute_status_row(
        {"invoice_id": 923399, "paid_sum_eur": 0.0, "amount_gross": 100.0},
        0.001,
    )
    assert status == "Akonto"
    assert dev == -100.0


def test_schadensrechnung_for_8xxxxx_invoice():
    status, dev = compute_status_row(
        {"invoice_id": 823399, "paid_sum_eur": 50.0, "amount_gross": 100.0},
        0.001,
    )
    assert status == "Schadensrechnungen"
    assert dev == -50.0


def test_manual_invoice_statuses_include_in_klaerung():
    assert "In Klärung" in app_module.MANUAL_INVOICE_STATUSES


def test_manual_invoice_statuses_include_ausgebucht_and_gutschrift():
    assert "ausgebucht" in app_module.MANUAL_INVOICE_STATUSES
    assert "Gutschrift" in app_module.MANUAL_INVOICE_STATUSES


def test_status_options_from_params_extend_invoice_and_payment_statuses():
    cfg = app_module._status_options_from_params(
        {
            "custom_invoice_statuses": ["Widerspruch", "widerspruch", "Freigabe intern"],
            "custom_payment_statuses": ["Zur Prüfung", "zuR prüfung", "Rückfrage Kunde"],
        }
    )
    assert "Widerspruch" in cfg["invoice_statuses"]
    assert "Freigabe intern" in cfg["invoice_statuses"]
    assert "Zur Prüfung" in cfg["payment_statuses"]
    assert "Rückfrage Kunde" in cfg["payment_statuses"]
    assert cfg["invoice_statuses"].count("Widerspruch") == 1
    assert cfg["payment_statuses"].count("Zur Prüfung") == 1


def test_parse_custom_status_colors_input_accepts_aliases_and_last_wins():
    mapping = app_module._parse_custom_status_colors_input(
        "Widerspruch=grün\nOffen=rot\nwiderspruch=blue",
        ["Offen", "Widerspruch"],
    )
    assert mapping == {"Widerspruch": "blau", "Offen": "rot"}


def test_parse_custom_status_colors_input_rejects_unknown_color():
    with pytest.raises(ValueError):
        app_module._parse_custom_status_colors_input(
            "Widerspruch=pink",
            ["Widerspruch"],
        )


def test_status_options_from_params_loads_clean_color_maps():
    cfg = app_module._status_options_from_params(
        {
            "custom_invoice_statuses": ["Widerspruch"],
            "custom_payment_statuses": ["Zur Prüfung"],
            "custom_invoice_status_colors": {
                "Offen": "rot",
                "Widerspruch": "orange",
                "Nicht vorhanden": "blau",
            },
            "custom_payment_status_colors": {
                "Zur Prüfung": "lila",
                "Offen": "gray",
            },
        }
    )
    assert cfg["invoice_status_colors"]["Offen"] == "rot"
    assert cfg["invoice_status_colors"]["Widerspruch"] == "orange"
    assert "Nicht vorhanden" not in cfg["invoice_status_colors"]
    assert cfg["payment_status_colors"]["Zur Prüfung"] == "lila"
    assert cfg["payment_status_colors"]["Offen"] == "grau"


def test_status_badge_inline_style_uses_configured_palette():
    style = app_module._status_badge_inline_style(
        "Widerspruch",
        "invoice",
        {"invoice_status_colors": {"Widerspruch": "orange"}},
    )
    assert "background: #ffedd5" in style
    assert "color: #c2410c" in style
    assert "border-color: #fed7aa" in style


def test_payment_effective_status_accepts_manual_custom_override():
    status = app_module.payment_effective_status(
        {
            "status_manual": 1,
            "status_override": "Zur Prüfung",
            "amount_eur": 100.0,
            "matched": 0,
            "akonto": 0,
            "schadensrechnung": 0,
        }
    )
    assert status == "Zur Prüfung"


def test_update_all_keeps_manual_invoice_status(tmp_path, monkeypatch):
    db_path = tmp_path / "status_manual.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE invoices(
          invoice_id INTEGER PRIMARY KEY,
          amount_gross REAL,
          paid_sum_eur REAL DEFAULT 0,
          payment_count INTEGER DEFAULT 0,
          last_payment_date TEXT,
          status TEXT,
          status_manual INTEGER DEFAULT 0,
          deviation_eur REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE payments(
          payment_id INTEGER PRIMARY KEY,
          invoice_id INTEGER,
          amount_eur REAL,
          matched INTEGER DEFAULT 0,
          value_date TEXT,
          booking_date TEXT,
          created_at TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO invoices(invoice_id, amount_gross, paid_sum_eur, status, status_manual, deviation_eur) VALUES (260001, 100.0, 0.0, 'Überzahlung', 1, -100.0)"
    )
    conn.execute(
        "INSERT INTO payments(payment_id, invoice_id, amount_eur, matched, booking_date, created_at) VALUES (1, 260001, 100.0, 1, '2026-03-01', '2026-03-01T00:00:00')"
    )
    conn.commit()
    conn.close()

    monkeypatch.setattr(status_module, "load_params", lambda: {"Toleranz": 0.001, "mahngebuehr_eur": 0.0})

    def _get_db():
        c = sqlite3.connect(db_path)
        c.row_factory = sqlite3.Row
        return c

    monkeypatch.setattr(db_module, "get_db", lambda db_path=None: _get_db())
    msg, ok = status_module.update_all()
    assert ok
    assert "aktualisiert" in msg

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT status, deviation_eur, paid_sum_eur FROM invoices WHERE invoice_id = 260001"
    ).fetchone()
    conn.close()
    assert row["status"] == "Überzahlung"
    assert row["paid_sum_eur"] == 100.0
    assert row["deviation_eur"] == 0.0
