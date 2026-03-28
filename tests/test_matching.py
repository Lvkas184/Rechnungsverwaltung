import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from matching import amount_similarity, compute_score, extract_invoice_number, name_similarity


def test_extract_invoice_number_patterns():
    assert extract_invoice_number("SVWZ+ReNr 252325 947SZ") == 252325
    assert extract_invoice_number("KREF NONREF SVWZ RE. NR. 250590100514") == 250590


def test_amount_similarity():
    assert amount_similarity(100, 100) == 1.0
    assert amount_similarity(100, 95) > 0.9


def test_name_similarity():
    assert name_similarity("Linkenheim-Hochstetten", "Linkenheim Hochstetten") > 0.8


def test_compute_score_bounds():
    assert 0 <= compute_score(0.85, 0.7, 0.8) <= 1
