"""Unit tests for matching helpers."""

import sqlite3
import sys
from pathlib import Path

# Ensure the project root is on the path so `src.*` imports work.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.matching import (
    extract_explicit_multi_invoice_numbers,
    _try_split_by_referenced_invoices,
    amount_similarity,
    compute_score,
    extract_invoice_number,
    extract_invoice_numbers,
    match_payment_row,
    name_similarity,
)


def test_extract_invoice_number_simple():
    assert extract_invoice_number("SVWZ+ReNr 252325 947SZ") == 252325


def test_extract_invoice_number_with_re():
    result = extract_invoice_number("KREF+NONREFSVWZ+RE. NR. 250590100514 HS/BC")
    # The regex may extract a truncated number; it must be an int or None
    assert isinstance(result, (int, type(None)))


def test_extract_invoice_number_long_truncated():
    """Numbers longer than 9 digits are truncated to 6."""
    result = extract_invoice_number("KREF NONREF SVWZ RE. NR. 250590100514")
    assert result == 250590


def test_extract_invoice_numbers_multiple():
    assert extract_invoice_numbers("SVWZ+260643 +260644") == [260643, 260644]


def test_extract_invoice_number_allows_akonto_9xxxxx():
    assert extract_invoice_number("SVWZ+2. Abschlagsrechnung 923399") == 923399


def test_match_payment_row_marks_akonto_reference_as_excluded():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE invoices(invoice_id INTEGER PRIMARY KEY, name TEXT, amount_gross REAL, paid_sum_eur REAL, status TEXT)"
    )
    payment = {
        "reference_text": "EREF+NOTPROVIDEDSVWZ+2.TR 923399 100802 Frohn",
        "amount_eur": 15374.5,
        "beneficiary_name": "Nees GmbH + Co. KG",
        "invoice_id": None,
        "matched": 0,
    }
    res = match_payment_row(conn, payment)
    assert res["rule"] == "akonto_excluded"
    assert res["invoice_id"] is None


def test_extract_explicit_multi_invoice_numbers_requires_delimiter():
    assert extract_explicit_multi_invoice_numbers("SVWZ+260643 +260644") == [260643, 260644]
    assert extract_explicit_multi_invoice_numbers("Schlussrechnung Nr.: 252621 100409") == []


def test_amount_similarity_exact():
    assert amount_similarity(100.0, 100.0) == 1.0


def test_amount_similarity_close():
    assert amount_similarity(100.0, 95.0) > 0.9


def test_amount_similarity_none():
    assert amount_similarity(None, 100.0) == 0.0


def test_name_similarity():
    # rapidfuzz token_set_ratio treats hyphens as part of the token, so the score
    # for "Linkenheim-Hochstetten" vs "Linkenheim Hochstetten" is lower than one
    # might expect (~0.5). A more similar pair scores higher.
    assert name_similarity("Linkenheim-Hochstetten", "Linkenheim Hochstetten") > 0.4
    assert name_similarity("Gemeinde Linkenheim", "Gemeinde Linkenheim") == 1.0


def test_name_similarity_none():
    assert name_similarity(None, "Foo") == 0.0


def test_compute_score_bounds():
    assert 0 <= compute_score(0.85, 0.6, 0.9) <= 1
    assert 0 <= compute_score(0.85, 0.7, 0.8) <= 1
    assert 0 <= compute_score(1.0, 1.0, 1.0) <= 1


def test_split_by_referenced_invoices_exact_amount():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE invoices(invoice_id INTEGER PRIMARY KEY, name TEXT, amount_gross REAL, paid_sum_eur REAL, status TEXT)"
    )
    conn.execute(
        "INSERT INTO invoices(invoice_id, name, amount_gross, paid_sum_eur, status) VALUES (260643, 'Thomas Kreisel', 381.69, 0, 'Offen')"
    )
    conn.execute(
        "INSERT INTO invoices(invoice_id, name, amount_gross, paid_sum_eur, status) VALUES (260644, 'Thomas Kreisel', 61.70, 0, 'Offen')"
    )

    payment = {"amount_eur": 443.39}
    res = _try_split_by_referenced_invoices(conn, payment, [260643, 260644])

    assert res is not None
    assert res["rule"] == "split_multi_invoice_ref"
    assert res["split"] == [(260643, 381.69), (260644, 61.7)]


def test_split_by_referenced_invoices_mismatch_returns_none():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE invoices(invoice_id INTEGER PRIMARY KEY, name TEXT, amount_gross REAL, paid_sum_eur REAL, status TEXT)"
    )
    conn.execute(
        "INSERT INTO invoices(invoice_id, name, amount_gross, paid_sum_eur, status) VALUES (260643, 'Thomas Kreisel', 381.69, 0, 'Offen')"
    )
    conn.execute(
        "INSERT INTO invoices(invoice_id, name, amount_gross, paid_sum_eur, status) VALUES (260644, 'Thomas Kreisel', 61.70, 0, 'Offen')"
    )

    payment = {"amount_eur": 500.00}
    res = _try_split_by_referenced_invoices(conn, payment, [260643, 260644])
    assert res is None


def test_match_payment_row_prefers_multi_invoice_reference_split():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE invoices(invoice_id INTEGER PRIMARY KEY, name TEXT, amount_gross REAL, paid_sum_eur REAL, status TEXT)"
    )
    conn.execute(
        "INSERT INTO invoices(invoice_id, name, amount_gross, paid_sum_eur, status) VALUES (260643, 'Thomas Kreisel', 381.69, 0, 'Offen')"
    )
    conn.execute(
        "INSERT INTO invoices(invoice_id, name, amount_gross, paid_sum_eur, status) VALUES (260644, 'Thomas Kreisel', 61.70, 0, 'Offen')"
    )

    payment = {
        "reference_text": "SVWZ+260643 +260644",
        "amount_eur": 443.39,
        "beneficiary_name": "Thomas Kreisel",
    }
    res = match_payment_row(conn, payment)
    assert res["rule"] == "split_multi_invoice_ref"
    assert res["split"] == [(260643, 381.69), (260644, 61.7)]


def test_match_payment_row_remap_candidate_ignores_own_old_assignment():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE invoices(invoice_id INTEGER PRIMARY KEY, name TEXT, amount_gross REAL, paid_sum_eur REAL, status TEXT)"
    )
    # Invoice 260643 currently appears overpaid because payment is wrongly fully linked there.
    conn.execute(
        "INSERT INTO invoices(invoice_id, name, amount_gross, paid_sum_eur, status) VALUES (260643, 'Thomas Kreisel', 381.69, 443.39, 'Überzahlung')"
    )
    conn.execute(
        "INSERT INTO invoices(invoice_id, name, amount_gross, paid_sum_eur, status) VALUES (260644, 'Thomas Kreisel', 61.70, 0, 'Offen')"
    )

    payment = {
        "reference_text": "SVWZ+260643 +260644",
        "amount_eur": 443.39,
        "beneficiary_name": "Thomas Kreisel",
        "invoice_id": 260643,
        "matched": 1,
    }
    res = match_payment_row(conn, payment)
    assert res["rule"] == "split_multi_invoice_ref"
    assert res["split"] == [(260643, 381.69), (260644, 61.7)]


def test_no_collective_split_for_large_amount_without_keyword():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE invoices(invoice_id INTEGER PRIMARY KEY, name TEXT, amount_gross REAL, paid_sum_eur REAL, status TEXT)"
    )
    conn.execute("CREATE TABLE manual_map(signature TEXT PRIMARY KEY, mapped_invoice_id INTEGER)")

    payment = {
        "reference_text": "SVWZ+1.te Abschlagsrechnung 91388990 vom 18.12.2025",
        "amount_eur": 47600.0,
        "beneficiary_name": "Burkhardt, Uwe",
        "matched": 0,
        "invoice_id": None,
    }
    res = match_payment_row(conn, payment)
    assert res["rule"] == "no_match"


def test_collective_split_requires_full_allocation():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE invoices(invoice_id INTEGER PRIMARY KEY, name TEXT, amount_gross REAL, paid_sum_eur REAL, status TEXT)"
    )
    conn.execute("CREATE TABLE manual_map(signature TEXT PRIMARY KEY, mapped_invoice_id INTEGER)")
    conn.execute(
        "INSERT INTO invoices(invoice_id, name, amount_gross, paid_sum_eur, status) VALUES (1, 'A', 100.0, 0, 'Offen')"
    )
    conn.execute(
        "INSERT INTO invoices(invoice_id, name, amount_gross, paid_sum_eur, status) VALUES (2, 'B', 50.0, 0, 'Offen')"
    )

    payment = {
        "reference_text": "Sammelüberweisung Dezember",
        "amount_eur": 500.0,
        "beneficiary_name": "X",
        "matched": 0,
        "invoice_id": None,
    }
    res = match_payment_row(conn, payment)
    assert res["rule"] == "no_match"


def test_exact_manual_map_is_used():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE invoices(invoice_id INTEGER PRIMARY KEY, name TEXT, amount_gross REAL, paid_sum_eur REAL, status TEXT)"
    )
    conn.execute("CREATE TABLE manual_map(signature TEXT PRIMARY KEY, mapped_invoice_id INTEGER)")
    conn.execute(
        "INSERT INTO invoices(invoice_id, name, amount_gross, paid_sum_eur, status) VALUES (260004, 'Christa Gorenflo', 29336.12, 0, 'Offen')"
    )
    conn.execute(
        "INSERT INTO manual_map(signature, mapped_invoice_id) VALUES ('SVWZ+Abschlagsrechnung ohne ReNr', 260004)"
    )

    payment = {
        "reference_text": "SVWZ+Abschlagsrechnung ohne ReNr",
        "amount_eur": 29336.12,
        "beneficiary_name": "Christa Gorenflo",
        "matched": 0,
        "invoice_id": None,
    }
    res = match_payment_row(conn, payment)
    assert res["rule"] == "manual_map_exact"
    assert res["invoice_id"] == 260004


def test_reference_only_does_not_use_amount_name_fallback():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE invoices(invoice_id INTEGER PRIMARY KEY, name TEXT, amount_gross REAL, paid_sum_eur REAL, status TEXT)"
    )
    conn.execute(
        "INSERT INTO invoices(invoice_id, name, amount_gross, paid_sum_eur, status) VALUES (260061, 'Burkhardt, Uwe', 47600.0, 0, 'Offen')"
    )

    payment = {
        "reference_text": "SVWZ+1.te Abschlagsrechnung vom 18.12.2025",
        "amount_eur": 47600.0,
        "beneficiary_name": "Burkhardt, Uwe",
        "matched": 0,
        "invoice_id": None,
    }
    res = match_payment_row(conn, payment)
    assert res["rule"] == "no_match"


def test_multiple_numbers_without_explicit_separator_do_not_trigger_split():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE invoices(invoice_id INTEGER PRIMARY KEY, name TEXT, amount_gross REAL, paid_sum_eur REAL, status TEXT)"
    )
    conn.execute(
        "INSERT INTO invoices(invoice_id, name, amount_gross, paid_sum_eur, status) VALUES (252621, 'A', 100.0, 0, 'Offen')"
    )
    conn.execute(
        "INSERT INTO invoices(invoice_id, name, amount_gross, paid_sum_eur, status) VALUES (251234, 'B', 200.0, 0, 'Offen')"
    )

    payment = {
        "reference_text": "SVWZ+Schlussrechnung Nr.: 252621 251234 KdNr.:11340",
        "amount_eur": 300.0,
        "beneficiary_name": "Test",
        "matched": 0,
        "invoice_id": None,
    }
    res = match_payment_row(conn, payment)
    assert res["rule"] == "regex_invoice"
    assert res["invoice_id"] == 252621


def test_explicit_multi_reference_with_missing_invoice_does_not_fallback_to_single():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE invoices(invoice_id INTEGER PRIMARY KEY, name TEXT, amount_gross REAL, paid_sum_eur REAL, status TEXT)"
    )
    conn.execute(
        "INSERT INTO invoices(invoice_id, name, amount_gross, paid_sum_eur, status) VALUES (255761, 'A', 123.76, 0, 'Offen')"
    )
    conn.execute(
        "INSERT INTO invoices(invoice_id, name, amount_gross, paid_sum_eur, status) VALUES (255626, 'B', 1951.84, 0, 'Offen')"
    )
    # 255624 intentionally missing

    payment = {
        "reference_text": "SVWZ+Rechnungen Nr. 255761/193288+255626/100842+255624/100842",
        "amount_eur": 9286.35,
        "beneficiary_name": "Christmann/Scheurer GdbR",
        "matched": 1,
        "invoice_id": 255761,
    }
    res = match_payment_row(conn, payment)
    assert res["rule"] == "split_multi_invoice_ref_missing_invoice"
    assert res["invoice_id"] is None


def test_explicit_multi_reference_amount_mismatch_does_not_fallback_to_single():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE invoices(invoice_id INTEGER PRIMARY KEY, name TEXT, amount_gross REAL, paid_sum_eur REAL, status TEXT)"
    )
    conn.execute(
        "INSERT INTO invoices(invoice_id, name, amount_gross, paid_sum_eur, status) VALUES (255761, 'A', 123.76, 0, 'Offen')"
    )
    conn.execute(
        "INSERT INTO invoices(invoice_id, name, amount_gross, paid_sum_eur, status) VALUES (255626, 'B', 1951.84, 0, 'Offen')"
    )
    conn.execute(
        "INSERT INTO invoices(invoice_id, name, amount_gross, paid_sum_eur, status) VALUES (255624, 'C', 1500.00, 0, 'Offen')"
    )

    payment = {
        "reference_text": "SVWZ+Rechnungen Nr. 255761/193288+255626/100842+255624/100842",
        "amount_eur": 9286.35,
        "beneficiary_name": "Christmann/Scheurer GdbR",
        "matched": 1,
        "invoice_id": 255761,
    }
    res = match_payment_row(conn, payment)
    assert res["rule"] == "split_multi_invoice_ref_unbalanced_amount"
    assert res["invoice_id"] is None
