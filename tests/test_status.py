"""Unit tests for status computation."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.status import compute_status_row


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
