"""Helpers for invoice classification rules."""


def is_akonto_invoice_id(invoice_id):
    """Return True for 6-digit invoice IDs starting with 9 (900000-999999)."""
    try:
        value = int(str(invoice_id).strip())
    except (TypeError, ValueError):
        return False
    return 900000 <= value <= 999999
