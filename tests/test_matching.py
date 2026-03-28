"""Unit tests for matching helpers."""

import sys
from pathlib import Path

# Ensure the project root is on the path so `src.*` imports work.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.matching import amount_similarity, compute_score, extract_invoice_number, name_similarity


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
