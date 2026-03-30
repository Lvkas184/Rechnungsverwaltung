"""Helpers for payment classification rules."""

import re

from src.invoice_rules import is_akonto_invoice_id


EREF_BLOCK_RE = re.compile(
    r"EREF\+[\s\S]*?(?=(?:SVWZ|KREF|MREF|BREF)\+|$)",
    re.I,
)
AKONTO_REF_RE = re.compile(r"(?<!\d)(9\d{5})(?!\d)")


def sanitize_reference_text(text):
    """Remove EREF blocks and normalize text for matching."""
    value = str(text or "")
    return EREF_BLOCK_RE.sub(" ", value)


def extract_akonto_invoice_ids(reference_text):
    """Return unique 9xxxxx numbers found in the payment reference."""
    value = sanitize_reference_text(reference_text)
    seen = set()
    out = []
    for raw in AKONTO_REF_RE.findall(value):
        try:
            inv_id = int(raw)
        except ValueError:
            continue
        if inv_id not in seen:
            seen.add(inv_id)
            out.append(inv_id)
    return out


def is_akonto_payment(reference_text, invoice_id=None):
    """True when payment points to an Abschlagsrechnung (Akonto)."""
    return is_akonto_invoice_id(invoice_id) or bool(extract_akonto_invoice_ids(reference_text))
