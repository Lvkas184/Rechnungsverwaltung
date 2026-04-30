"""Tests for reference number extraction used by split-assistant suggestions."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import app as app_module


def test_extract_reference_numbers_maps_structured_12digit_to_invoice_id():
    numbers = app_module._extract_reference_numbers("SVWZ+.261085194451")
    assert 261085194451 in numbers
    assert 261085 in numbers


def test_extract_reference_numbers_does_not_map_unstructured_12digit_to_invoice_id():
    numbers = app_module._extract_reference_numbers("SVWZ+.261085294451")
    assert 261085294451 in numbers
    assert 261085 not in numbers

