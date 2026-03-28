import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from status import compute_status_row


def test_status_offen():
    status, dev = compute_status_row({"paid_sum_eur": 0, "amount_gross": 100}, 0.001)
    assert status == "Offen"
    assert dev == -100


def test_status_bezahlt_with_tolerance():
    status, _ = compute_status_row({"paid_sum_eur": 100.0004, "amount_gross": 100}, 0.001)
    assert status == "Bezahlt"


def test_status_ueberzahlung():
    status, _ = compute_status_row({"paid_sum_eur": 120, "amount_gross": 100}, 0.001)
    assert status == "Überzahlung"


def test_status_teiloffen():
    status, _ = compute_status_row({"paid_sum_eur": 50, "amount_gross": 100}, 0.001)
    assert status == "Teiloffen/Unterzahlung"
