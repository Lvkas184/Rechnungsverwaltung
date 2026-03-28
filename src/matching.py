"""Matching-Engine for payment-to-invoice allocation.

Rules (in priority order):
1. Regex invoice-number extraction from reference_text
2. Manual map lookup (exact signature, then substring)
3. Exact amount + name match
4. Fuzzy name + amount similarity
5. Splitting for collective payments (Sammelzahlungen)
"""

import json
import re
import sqlite3
from datetime import datetime
from difflib import SequenceMatcher

try:
    from rapidfuzz import fuzz  # type: ignore
except Exception:  # pragma: no cover — rapidfuzz is optional
    fuzz = None

from src.db import PARAM_PATH


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

def load_params(path=PARAM_PATH):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


PARAMS = load_params()
TOLERANCE = float(PARAMS.get("Toleranz", 0.001))
AUTO_THRESHOLD = float(PARAMS.get("match_score_auto", 0.85))
REVIEW_THRESHOLD = float(PARAMS.get("match_score_review", 0.6))
SPLIT_THRESHOLD = float(PARAMS.get("split_threshold", 0.01))


# ---------------------------------------------------------------------------
# Regex patterns for invoice-number detection
# ---------------------------------------------------------------------------

RE_PATTERNS = [
    re.compile(
        r"(?:RE(?:\.)?|RN|ReNr|RENr|re\s*nr|re\.?\s*nr|Re\.*\s*Nr\.?)\s*[:\-]?\s*([0-9]{4,12})",
        re.I,
    ),
    re.compile(r"\b([0-9]{6,9})\b"),  # fallback: 6-9 digit numbers
]


def extract_invoice_number(text):
    """Return the first invoice number found in *text*, or None."""
    if not text:
        return None
    value = str(text)
    for p in RE_PATTERNS:
        m = p.search(value)
        if m:
            try:
                raw = m.group(1)
                # Truncate overly long numbers (e.g. combined reference strings)
                if len(raw) > 9:
                    raw = raw[:6]
                return int(raw)
            except (ValueError, IndexError):
                continue
    return None


# ---------------------------------------------------------------------------
# Similarity helpers
# ---------------------------------------------------------------------------

def amount_similarity(a, b):
    """Return 0..1 similarity between two monetary amounts."""
    try:
        if a is None or b is None:
            return 0.0
        a, b = float(a), float(b)
        if a == b:
            return 1.0
        denom = max(abs(a), abs(b), 1.0)
        return max(0.0, 1 - abs(a - b) / denom)
    except Exception:
        return 0.0


def name_similarity(a, b):
    """Return 0..1 fuzzy similarity between two name strings."""
    if not a or not b:
        return 0.0
    if fuzz is not None:
        return fuzz.token_set_ratio(str(a), str(b)) / 100.0
    return SequenceMatcher(None, str(a).lower(), str(b).lower()).ratio()


def compute_score(base_weight, name_score=0.0, amount_score=0.0):
    """Weighted combination: base*0.6 + name*0.25 + amount*0.15."""
    return min(1.0, base_weight * 0.6 + name_score * 0.25 + amount_score * 0.15)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def load_db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn


def find_invoice_by_id(conn, invoice_id):
    if not invoice_id:
        return None
    return conn.execute(
        "SELECT * FROM invoices WHERE invoice_id = ?", (invoice_id,)
    ).fetchone()


def find_candidates_by_amount(conn, amount, pct=0.05, limit=200):
    """Return open invoices whose amount_gross is within ±pct of *amount*."""
    if amount is None:
        return conn.execute(
            "SELECT * FROM invoices WHERE COALESCE(status, 'Offen') != 'Bezahlt' LIMIT ?",
            (limit,),
        ).fetchall()
    low = amount * (1 - pct)
    high = amount * (1 + pct)
    return conn.execute(
        """SELECT * FROM invoices
           WHERE (COALESCE(status, 'Offen') != 'Bezahlt')
             AND amount_gross BETWEEN ? AND ?
           LIMIT ?""",
        (low, high, limit),
    ).fetchall()


# ---------------------------------------------------------------------------
# Core matching logic
# ---------------------------------------------------------------------------

def match_payment_row(conn, payment):
    """Attempt to match a single payment row to an invoice.

    Returns a dict with keys: invoice_id, score, rule, and optionally split.
    """
    ref = payment["reference_text"]
    amt = payment["amount_eur"]
    ben = payment["beneficiary_name"] or ref or ""

    # 1) Regex invoice number
    inv_id = extract_invoice_number(ref)
    if inv_id:
        inv = find_invoice_by_id(conn, inv_id)
        if inv:
            return {"invoice_id": inv["invoice_id"], "score": 1.0, "rule": "regex_invoice"}

    # 2) Manual map — exact signature
    row = conn.execute(
        "SELECT mapped_invoice_id FROM manual_map WHERE signature = ?",
        (ref,),
    ).fetchone()
    if row and row["mapped_invoice_id"]:
        inv = find_invoice_by_id(conn, row["mapped_invoice_id"])
        if inv:
            return {"invoice_id": inv["invoice_id"], "score": 0.95, "rule": "manual_map_exact"}

    # 2b) Manual map — substring match
    for mrow in conn.execute("SELECT mapped_invoice_id, signature FROM manual_map").fetchall():
        sig = mrow["signature"]
        if sig and ref and sig in ref:
            inv = find_invoice_by_id(conn, mrow["mapped_invoice_id"])
            if inv:
                return {"invoice_id": inv["invoice_id"], "score": 0.92, "rule": "manual_map_contains"}

    # 3) Exact amount + name contains
    if amt is not None:
        rows = conn.execute(
            "SELECT * FROM invoices WHERE amount_gross = ? AND name LIKE ? LIMIT 5",
            (amt, f"%{ben}%"),
        ).fetchall()
        if rows:
            return {"invoice_id": rows[0]["invoice_id"], "score": 0.9, "rule": "exact_amount_name"}

    # 4) Fuzzy candidates by amount ±5% and name similarity
    best = None
    for inv in find_candidates_by_amount(conn, amt):
        nscore = name_similarity(inv["name"], ben)
        ascore = amount_similarity(amt, inv["amount_gross"])
        score = compute_score(0.85, nscore, ascore)
        if best is None or score > best["score"]:
            best = {"invoice_id": inv["invoice_id"], "score": score, "rule": "fuzzy_name_amount"}

    if best and best["score"] >= REVIEW_THRESHOLD:
        return best

    # 5) Splitting: detect "Sammel" keywords or very large payments
    text_lower = (ref or "").lower()
    if (
        "sammel" in text_lower
        or "sammelüberweisung" in text_lower
        or "sammelueberweisung" in text_lower
        or (amt and float(amt) > 10000)
    ):
        invs = conn.execute(
            """SELECT * FROM invoices
               WHERE COALESCE(status, 'Offen') != 'Bezahlt'
               ORDER BY invoice_id ASC
               LIMIT 200"""
        ).fetchall()
        remaining = float(amt or 0)
        splits = []
        for inv in invs:
            need = (inv["amount_gross"] or 0) - (inv["paid_sum_eur"] or 0)
            if need <= 0:
                continue
            alloc = min(need, remaining)
            if alloc >= SPLIT_THRESHOLD:
                splits.append((inv["invoice_id"], round(float(alloc), 2)))
                remaining -= alloc
            if remaining <= SPLIT_THRESHOLD:
                break
        if splits:
            return {"invoice_id": None, "score": 0.8, "rule": "split_collective", "split": splits}

    # No match
    return {"invoice_id": None, "score": 0.0, "rule": "no_match"}


# ---------------------------------------------------------------------------
# Apply matching to all unmatched payments
# ---------------------------------------------------------------------------

def _apply_single_invoice(conn, payment, res):
    """Write a single-invoice match to the DB."""
    conn.execute(
        """UPDATE payments
           SET invoice_id = ?, matched = 1, match_score = ?, match_rule = ?, created_by = 'auto'
           WHERE payment_id = ?""",
        (res["invoice_id"], res["score"], res["rule"], payment["payment_id"]),
    )
    conn.execute(
        "INSERT INTO audit_log(payment_id, invoice_id, match_score, rule_used, automated, user) VALUES (?,?,?,?,1,'system')",
        (payment["payment_id"], res["invoice_id"], res["score"], res["rule"]),
    )
    conn.execute(
        """UPDATE invoices
           SET paid_sum_eur = COALESCE(paid_sum_eur, 0) + ?,
               payment_count = COALESCE(payment_count, 0) + 1,
               last_payment_date = ?
           WHERE invoice_id = ?""",
        (payment["amount_eur"], datetime.utcnow().isoformat(), res["invoice_id"]),
    )


def apply_matching(auto_commit=True):
    """Match all unmatched payments and update DB accordingly."""
    conn = load_db()
    rows = conn.execute(
        "SELECT * FROM payments WHERE COALESCE(matched, 0) = 0"
    ).fetchall()
    print(f"Zu matchende Zahlungen: {len(rows)}")

    for p in rows:
        res = match_payment_row(conn, p)

        if res.get("invoice_id") and res.get("score", 0.0) >= AUTO_THRESHOLD:
            _apply_single_invoice(conn, p, res)

        elif res.get("rule") == "split_collective" and res.get("split"):
            for inv_id, alloc in res["split"]:
                conn.execute(
                    """INSERT INTO payments(
                         invoice_id, source, booking_date, value_date, amount_eur,
                         reference_text, matched, match_score, match_rule, created_by
                       ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, 'auto_split', 'auto')""",
                    (inv_id, p["source"], p["booking_date"], p["value_date"], alloc, p["reference_text"], 0.8),
                )
                child_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                conn.execute(
                    "INSERT INTO audit_log(payment_id, invoice_id, match_score, rule_used, automated, user) VALUES (?,?,?,?,1,'system')",
                    (child_id, inv_id, 0.8, "auto_split"),
                )
                conn.execute(
                    """UPDATE invoices
                       SET paid_sum_eur = COALESCE(paid_sum_eur, 0) + ?,
                           payment_count = COALESCE(payment_count, 0) + 1,
                           last_payment_date = ?
                       WHERE invoice_id = ?""",
                    (alloc, datetime.utcnow().isoformat(), inv_id),
                )
            # Mark original payment as processed
            conn.execute(
                "UPDATE payments SET matched = 1, match_rule = 'split_parent', created_by = 'auto' WHERE payment_id = ?",
                (p["payment_id"],),
            )
            conn.execute(
                "INSERT INTO audit_log(payment_id, invoice_id, match_score, rule_used, automated, user) VALUES (?,?,?,?,1,'system')",
                (p["payment_id"], None, 0.8, "split_parent"),
            )

        else:
            # Audit log for unmatched / below-threshold
            conn.execute(
                "INSERT INTO audit_log(payment_id, invoice_id, match_score, rule_used, automated, user) VALUES (?,?,?,?,1,'system')",
                (p["payment_id"], res.get("invoice_id"), res.get("score", 0.0), res.get("rule")),
            )
            # Store suggestion if above review threshold
            if res.get("invoice_id") and res.get("score", 0.0) >= REVIEW_THRESHOLD:
                conn.execute(
                    "UPDATE payments SET match_score = ?, match_rule = ? WHERE payment_id = ?",
                    (res["score"], res["rule"], p["payment_id"]),
                )

    if auto_commit:
        conn.commit()
    conn.close()


if __name__ == "__main__":
    apply_matching()
    print("Matching durchgeführt.")
