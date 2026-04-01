"""Helpers for invoice classification rules."""


def is_akonto_invoice_id(invoice_id):
    """Return True for 6-digit invoice IDs starting with 9 (900000-999999)."""
    try:
        value = int(str(invoice_id).strip())
    except (TypeError, ValueError):
        return False
    return 900000 <= value <= 999999


def is_schadensrechnung_invoice_id(invoice_id):
    """Return True for 6-digit invoice IDs starting with 8 (800000-899999)."""
    try:
        value = int(str(invoice_id).strip())
    except (TypeError, ValueError):
        return False
    return 800000 <= value <= 899999


def classify_special_invoice_status(invoice_id):
    """Return special invoice status for prefixed invoice IDs, else None."""
    if is_akonto_invoice_id(invoice_id):
        return "Akonto"
    if is_schadensrechnung_invoice_id(invoice_id):
        return "Schadensrechnungen"
    return None
