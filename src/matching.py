"""Matching-Engine for payment-to-invoice allocation.

Rules (in priority order):
1. Multi-invoice split from explicit invoice lists in reference_text
2. Single invoice-number extraction from reference_text
3. Manual map lookup (exact signature)

No automatic fuzzy amount/name matching and no collective split heuristics.
"""

import json
import re
from datetime import datetime
from difflib import SequenceMatcher

try:
    from rapidfuzz import fuzz  # type: ignore
except Exception:  # pragma: no cover — rapidfuzz is optional
    fuzz = None

from src.db import PARAM_PATH, get_db, init_db
from src.invoice_rules import classify_special_invoice_status
from src.payment_rules import classify_special_payment_status


# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

def load_params(path=PARAM_PATH):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _parse_float_param(value, default):
    try:
        if value is None:
            return float(default)
        if isinstance(value, str):
            cleaned = value.strip().replace("€", "").replace(" ", "").replace(",", ".")
            if cleaned == "":
                return float(default)
            return float(cleaned)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _load_mahngebuehren(params):
    """Return tuple (fee_1, fee_2, fee_3) with backward compatible defaults."""
    fee_1 = max(
        0.0,
        _parse_float_param(
            params.get("mahngebuehr_1_eur", params.get("mahngebuehr_eur", 0.0)),
            0.0,
        ),
    )
    fee_2 = max(0.0, _parse_float_param(params.get("mahngebuehr_2_eur", fee_1), fee_1))
    fee_3 = max(0.0, _parse_float_param(params.get("mahngebuehr_3_eur", fee_2), fee_2))
    return fee_1, fee_2, fee_3


def _extract_mahnstufe(reminder_status):
    value = str(reminder_status or "").strip()
    if value.startswith("1."):
        return 1
    if value.startswith("2."):
        return 2
    if value.startswith("3."):
        return 3
    return None


def _matches_mahngebuehr(reminder_status, deviation):
    """Match configured Mahngebuehr.

    If a Mahnstufe is set, only that stage fee matches.
    Without Mahnstufe, accept any configured fee as fallback.
    """
    stufe = _extract_mahnstufe(reminder_status)
    if stufe == 1:
        candidate_fees = [MAHNGEBUEHR_1_EUR]
    elif stufe == 2:
        candidate_fees = [MAHNGEBUEHR_2_EUR]
    elif stufe == 3:
        candidate_fees = [MAHNGEBUEHR_3_EUR]
    else:
        candidate_fees = [
            fee
            for fee in (MAHNGEBUEHR_1_EUR, MAHNGEBUEHR_2_EUR, MAHNGEBUEHR_3_EUR)
            if fee > 0
        ]
    return any(abs(deviation - fee) <= TOLERANCE for fee in candidate_fees)


PARAMS = load_params()
TOLERANCE = _parse_float_param(PARAMS.get("Toleranz", 0.001), 0.001)
AUTO_THRESHOLD = _parse_float_param(PARAMS.get("match_score_auto", 0.85), 0.85)
REVIEW_THRESHOLD = _parse_float_param(PARAMS.get("match_score_review", 0.6), 0.6)
SPLIT_THRESHOLD = _parse_float_param(PARAMS.get("split_threshold", 0.01), 0.01)
MAHNGEBUEHR_1_EUR, MAHNGEBUEHR_2_EUR, MAHNGEBUEHR_3_EUR = _load_mahngebuehren(PARAMS)


# ---------------------------------------------------------------------------
# Regex patterns for invoice-number detection
# ---------------------------------------------------------------------------

RE_PATTERNS = [
    re.compile(
        r"(?:RE(?:\.)?|RN|ReNr|RENr|re\s*nr|re\.?\s*nr|Re\.*\s*Nr\.?)\s*[:\-]?\s*([0-9]{4,12})",
        re.I,
    ),
    re.compile(r"(?<!\d)([0-9]{6})(?!\d)"),  # fallback: isolated 6-digit numbers
]
MULTI_SPLIT_PATTERN = re.compile(
    r"(?<!\d)\d{6}(?!\d)(?:\s*[+/,;]\s*(?<!\d)\d{6}(?!\d))+",
    re.I,
)


def _sanitize_reference_text(text):
    """Remove EREF blocks and normalize to plain string."""
    value = str(text or "")
    return re.sub(r"EREF\+[\s\S]*?(?=(?:SVWZ|KREF|MREF|BREF)\+|$)", " ", value, flags=re.I)


def _is_plausible_invoice_number(raw):
    """Keep only 6-digit invoice IDs with plausible year prefix (20..currentYY)."""
    if not raw or not re.fullmatch(r"\d{6}", str(raw)):
        return False
    if str(raw).startswith(("8", "9")):
        # Sonderfälle: Schadensrechnungen/Akonto
        return True
    yy = int(str(raw)[:2])
    current_yy = datetime.now().year % 100
    return 20 <= yy <= current_yy


def extract_invoice_number(text):
    """Return the first invoice number found in *text*, or None."""
    numbers = extract_invoice_numbers(text)
    return numbers[0] if numbers else None


def extract_invoice_numbers(text):
    """Return unique invoice numbers from *text* in appearance order."""
    if not text:
        return []
    value = _sanitize_reference_text(text)
    out = []
    seen = set()

    for p in RE_PATTERNS:
        for m in p.finditer(value):
            try:
                raw = m.group(1)
                # Truncate overly long numbers (e.g. combined reference strings)
                if len(raw) > 9:
                    raw = raw[:6]
                if not _is_plausible_invoice_number(raw):
                    continue
                inv_id = int(raw)
                if inv_id not in seen:
                    seen.add(inv_id)
                    out.append(inv_id)
            except (ValueError, IndexError):
                continue
    return out


def extract_explicit_multi_invoice_numbers(text):
    """Return invoice IDs only from explicit list syntax (e.g. 260643 +260644)."""
    if not text:
        return []
    value = _sanitize_reference_text(text)

    out = []
    seen = set()
    for run in MULTI_SPLIT_PATTERN.finditer(value):
        for raw in re.findall(r"(?<!\d)(\d{6})(?!\d)", run.group(0)):
            if not _is_plausible_invoice_number(raw):
                continue
            inv_id = int(raw)
            if inv_id not in seen:
                seen.add(inv_id)
                out.append(inv_id)

    return out


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
    init_db()
    return get_db()


def _has_column(conn, table_name, column_name):
    cols = {row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})")}
    return column_name in cols


def find_invoice_by_id(conn, invoice_id):
    if not invoice_id:
        return None
    row = conn.execute(
        "SELECT * FROM invoices WHERE invoice_id = ?", (invoice_id,)
    ).fetchone()
    if not row:
        return None
    doc_type = "rechnung"
    if "document_type" in row.keys():
        doc_type = str(row["document_type"] or "rechnung").strip().lower()
    if doc_type == "gutschrift":
        return None
    return row


def find_candidates_by_amount(conn, amount, pct=0.05, limit=200):
    """Return open invoices whose amount_gross is within ±pct of *amount*."""
    has_document_type = _has_column(conn, "invoices", "document_type")
    type_clause = "AND COALESCE(document_type, 'rechnung') = 'rechnung'" if has_document_type else ""
    if amount is None:
        return conn.execute(
            f"SELECT * FROM invoices WHERE COALESCE(status, 'Offen') NOT IN ('Bezahlt', 'Bezahlt mit Mahngebühr', 'Gutschrift') {type_clause} LIMIT ?",
            (limit,),
        ).fetchall()
    low = amount * (1 - pct)
    high = amount * (1 + pct)
    return conn.execute(
        f"""SELECT * FROM invoices
           WHERE (COALESCE(status, 'Offen') NOT IN ('Bezahlt', 'Bezahlt mit Mahngebühr', 'Gutschrift'))
             {type_clause}
             AND amount_gross BETWEEN ? AND ?
           LIMIT ?""",
        (low, high, limit),
    ).fetchall()


def _remaining_invoice_amount(inv, payment=None):
    """Open amount for an invoice (never negative).

    For remapping already matched single payments, treat the current payment as
    not yet applied to its currently linked invoice.
    """
    paid = float(inv["paid_sum_eur"] or 0)

    payment_matched = None
    payment_invoice_id = None
    payment_amount = None
    if payment is not None:
        payment_matched = payment["matched"] if "matched" in payment.keys() else None
        payment_invoice_id = payment["invoice_id"] if "invoice_id" in payment.keys() else None
        payment_amount = payment["amount_eur"] if "amount_eur" in payment.keys() else None

    if payment_matched == 1 and payment_invoice_id == inv["invoice_id"]:
        paid = max(0.0, paid - float(payment_amount or 0))
    return max(0.0, float(inv["amount_gross"] or 0) - paid)


def _is_fully_settled_by_credit(amount_gross, deviation, credit_applied_eur):
    try:
        amount = float(amount_gross or 0.0)
    except (TypeError, ValueError):
        amount = 0.0
    try:
        credit_applied = float(credit_applied_eur or 0.0)
    except (TypeError, ValueError):
        credit_applied = 0.0

    if credit_applied <= TOLERANCE:
        return False
    return (amount - credit_applied) <= TOLERANCE and abs(float(deviation or 0.0)) <= TOLERANCE


def _compute_invoice_status(
    invoice_id,
    amount_gross,
    paid_sum,
    reminder_status=None,
    credit_applied_eur=0.0,
):
    document_type = "rechnung"
    if isinstance(reminder_status, dict):
        document_type = str(reminder_status.get("document_type") or "rechnung").strip().lower()
        reminder_status = reminder_status.get("reminder_status")
    if document_type == "gutschrift":
        return "Gutschrift", 0.0

    amount = float(amount_gross or 0)
    paid = float(paid_sum or 0)
    deviation = paid - amount
    special_status = classify_special_invoice_status(invoice_id)
    if special_status:
        status = special_status
    elif paid == 0:
        status = "Offen"
    elif abs(deviation) <= TOLERANCE:
        status = (
            "Gutschrift"
            if _is_fully_settled_by_credit(amount, deviation, credit_applied_eur)
            else "Bezahlt"
        )
    elif deviation > TOLERANCE and _matches_mahngebuehr(reminder_status, deviation):
        status = "Bezahlt mit Mahngebühr"
    elif deviation > TOLERANCE:
        status = "Überzahlung"
    else:
        status = "Teiloffen/Unterzahlung"
    return status, deviation


def _rebuild_invoice_aggregates_and_status(conn):
    """Rebuild invoice aggregates/statuses from matched payments."""
    has_document_type = _has_column(conn, "invoices", "document_type")
    has_credit_target = _has_column(conn, "invoices", "credit_target_invoice_id")
    if has_document_type and has_credit_target:
        conn.execute(
            """
            UPDATE invoices
            SET paid_sum_eur = CASE
                WHEN COALESCE(invoices.document_type, 'rechnung') = 'rechnung' THEN (
                    SELECT COALESCE(SUM(amount_eur), 0)
                    FROM payments
                    WHERE payments.invoice_id = invoices.invoice_id
                      AND payments.matched = 1
                ) + (
                    SELECT COALESCE(SUM(amount_gross), 0)
                    FROM invoices credits
                    WHERE COALESCE(credits.document_type, 'rechnung') = 'gutschrift'
                      AND credits.credit_target_invoice_id = invoices.invoice_id
                )
                ELSE 0
            END,
                payment_count = CASE
                WHEN COALESCE(invoices.document_type, 'rechnung') = 'rechnung' THEN (
                    SELECT COUNT(*)
                    FROM payments
                    WHERE payments.invoice_id = invoices.invoice_id
                      AND payments.matched = 1
                ) + (
                    SELECT COUNT(*)
                    FROM invoices credits
                    WHERE COALESCE(credits.document_type, 'rechnung') = 'gutschrift'
                      AND credits.credit_target_invoice_id = invoices.invoice_id
                )
                ELSE 0
            END,
                last_payment_date = CASE
                WHEN COALESCE(invoices.document_type, 'rechnung') = 'rechnung' THEN (
                    SELECT MAX(dt) FROM (
                        SELECT COALESCE(value_date, booking_date, created_at) AS dt
                        FROM payments
                        WHERE payments.invoice_id = invoices.invoice_id
                          AND payments.matched = 1
                        UNION ALL
                        SELECT COALESCE(issue_date, updated_at, created_at) AS dt
                        FROM invoices credits
                        WHERE COALESCE(credits.document_type, 'rechnung') = 'gutschrift'
                          AND credits.credit_target_invoice_id = invoices.invoice_id
                    )
                )
                ELSE NULL
            END
            """
        )
    else:
        conn.execute(
            """
            UPDATE invoices
            SET paid_sum_eur = (
                SELECT COALESCE(SUM(amount_eur), 0)
                FROM payments
                WHERE payments.invoice_id = invoices.invoice_id
                  AND payments.matched = 1
            ),
                payment_count = (
                SELECT COUNT(*)
                FROM payments
                WHERE payments.invoice_id = invoices.invoice_id
                  AND payments.matched = 1
            ),
                last_payment_date = (
                SELECT MAX(COALESCE(value_date, booking_date, created_at))
                FROM payments
                WHERE payments.invoice_id = invoices.invoice_id
                  AND payments.matched = 1
            )
            """
        )

    credit_applied_by_invoice = {}
    if has_document_type and has_credit_target:
        for row in conn.execute(
            """
            SELECT credit_target_invoice_id AS invoice_id,
                   COALESCE(SUM(amount_gross), 0) AS credit_applied_eur
            FROM invoices
            WHERE COALESCE(document_type, 'rechnung') = 'gutschrift'
              AND credit_target_invoice_id IS NOT NULL
            GROUP BY credit_target_invoice_id
            """
        ).fetchall():
            credit_applied_by_invoice[int(row["invoice_id"])] = float(row["credit_applied_eur"] or 0.0)

    if has_document_type:
        rows = conn.execute(
            """
            SELECT invoice_id, amount_gross, paid_sum_eur,
                   COALESCE(document_type, 'rechnung') AS document_type,
                   reminder_status,
                   COALESCE(status_manual, 0) AS status_manual
            FROM invoices
            """
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT invoice_id, amount_gross, paid_sum_eur,
                   'rechnung' AS document_type,
                   reminder_status,
                   COALESCE(status_manual, 0) AS status_manual
            FROM invoices
            """
        ).fetchall()
    for inv in rows:
        status, deviation = _compute_invoice_status(
            inv["invoice_id"],
            inv["amount_gross"],
            inv["paid_sum_eur"],
            {
                "document_type": inv["document_type"] if "document_type" in inv.keys() else "rechnung",
                "reminder_status": inv["reminder_status"],
            },
            credit_applied_by_invoice.get(int(inv["invoice_id"]), 0.0),
        )
        if int(inv["status_manual"] or 0) == 1:
            conn.execute(
                "UPDATE invoices SET deviation_eur = ? WHERE invoice_id = ?",
                (deviation, inv["invoice_id"]),
            )
        else:
            conn.execute(
                "UPDATE invoices SET status = ?, deviation_eur = ? WHERE invoice_id = ?",
                (status, deviation, inv["invoice_id"]),
            )


def _cleanup_legacy_collective_splits(conn):
    """Remove historic heuristic collective-split assignments.

    Those assignments were not based on explicit invoice numbers from the
    payment reference and are now considered invalid.
    """
    parent_rows = conn.execute(
        "SELECT payment_id FROM payments WHERE parent_payment_id IS NULL AND match_rule = 'split_collective'"
    ).fetchall()
    if not parent_rows:
        return 0, 0

    parent_ids = [row["payment_id"] for row in parent_rows]
    parent_marks = ",".join(["?"] * len(parent_ids))

    child_rows = conn.execute(
        f"SELECT payment_id FROM payments WHERE parent_payment_id IN ({parent_marks})",
        parent_ids,
    ).fetchall()
    child_ids = [row["payment_id"] for row in child_rows]

    if child_ids:
        child_marks = ",".join(["?"] * len(child_ids))
        conn.execute(f"DELETE FROM audit_log WHERE payment_id IN ({child_marks})", child_ids)
        conn.execute(f"DELETE FROM payments WHERE payment_id IN ({child_marks})", child_ids)

    conn.execute(f"DELETE FROM audit_log WHERE payment_id IN ({parent_marks})", parent_ids)
    conn.execute(
        f"""UPDATE payments
            SET invoice_id = NULL,
                matched = 0,
                match_score = NULL,
                match_rule = NULL,
                created_by = 'auto'
            WHERE payment_id IN ({parent_marks})""",
        parent_ids,
    )

    _rebuild_invoice_aggregates_and_status(conn)
    return len(parent_ids), len(child_ids)


def _try_split_by_referenced_invoices(conn, payment, invoice_ids):
    """Build a split allocation if reference lists multiple valid invoice IDs."""
    if len(invoice_ids) < 2:
        return None

    amount = payment["amount_eur"]
    if amount is None:
        return None

    try:
        remaining = float(amount)
    except (TypeError, ValueError):
        return None
    if remaining <= 0:
        return None

    splits = []
    for inv_id in invoice_ids:
        inv = find_invoice_by_id(conn, inv_id)
        if not inv:
            continue
        need = _remaining_invoice_amount(inv, payment=payment)
        if need <= SPLIT_THRESHOLD:
            continue
        alloc = min(need, remaining)
        if alloc >= SPLIT_THRESHOLD:
            splits.append((inv_id, round(float(alloc), 2)))
            remaining -= alloc
        if remaining <= SPLIT_THRESHOLD:
            break

    if len(splits) < 2:
        return None

    allocated = sum(alloc for _, alloc in splits)
    allowed_delta = max(TOLERANCE, SPLIT_THRESHOLD)
    if abs(allocated - float(amount)) > allowed_delta:
        return None

    return {
        "invoice_id": None,
        "score": 1.0,
        "rule": "split_multi_invoice_ref",
        "split": splits,
    }


# ---------------------------------------------------------------------------
# Core matching logic
# ---------------------------------------------------------------------------

def match_payment_row(conn, payment):
    """Attempt to match a single payment row to an invoice.

    Returns a dict with keys: invoice_id, score, rule, and optionally split.
    """
    ref = payment["reference_text"]
    special_payment_status = classify_special_payment_status(
        ref,
        payment["invoice_id"] if "invoice_id" in payment.keys() else None,
    )
    if special_payment_status == "Akonto":
        return {"invoice_id": None, "score": 0.0, "rule": "akonto_excluded"}
    if special_payment_status == "Schadensrechnungen":
        return {"invoice_id": None, "score": 0.0, "rule": "schadensrechnung_excluded"}

    split_invoice_ids = extract_explicit_multi_invoice_numbers(ref)
    referenced_invoice_ids = extract_invoice_numbers(ref)

    # 1) Split by multiple referenced invoice numbers
    split_from_reference = _try_split_by_referenced_invoices(conn, payment, split_invoice_ids)
    if split_from_reference:
        return split_from_reference

    # Explicit multi-reference was present but exact split could not be built.
    # Do not fallback to single-invoice matching in this case.
    if len(split_invoice_ids) >= 2:
        existing = [inv_id for inv_id in split_invoice_ids if find_invoice_by_id(conn, inv_id)]
        if len(existing) < len(split_invoice_ids):
            return {
                "invoice_id": None,
                "score": 0.0,
                "rule": "split_multi_invoice_ref_missing_invoice",
            }
        return {
            "invoice_id": None,
            "score": 0.0,
            "rule": "split_multi_invoice_ref_unbalanced_amount",
        }

    # 2) Regex invoice number
    for inv_id in referenced_invoice_ids:
        inv = find_invoice_by_id(conn, inv_id)
        if inv:
            return {"invoice_id": inv["invoice_id"], "score": 1.0, "rule": "regex_invoice"}

    # 3) Manual map — exact signature only
    try:
        row = conn.execute(
            "SELECT mapped_invoice_id FROM manual_map WHERE signature = ?",
            (ref,),
        ).fetchone()
    except Exception:
        row = None
    if row and row["mapped_invoice_id"]:
        inv = find_invoice_by_id(conn, row["mapped_invoice_id"])
        if inv:
            return {"invoice_id": inv["invoice_id"], "score": 0.95, "rule": "manual_map_exact"}

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


def _rollback_existing_single_assignment(conn, payment):
    """Undo invoice aggregate effects of an already matched single payment."""
    if not payment["invoice_id"]:
        return
    conn.execute(
        """UPDATE invoices
           SET paid_sum_eur = MAX(0, COALESCE(paid_sum_eur, 0) - ?),
               payment_count = CASE
                   WHEN COALESCE(payment_count, 0) > 0 THEN payment_count - 1
                   ELSE 0
               END,
               last_payment_date = CASE
                   WHEN COALESCE(payment_count, 0) <= 1 THEN NULL
                   ELSE last_payment_date
               END
           WHERE invoice_id = ?""",
        (payment["amount_eur"] or 0, payment["invoice_id"]),
    )


def apply_matching(auto_commit=True):
    """Match all unmatched payments and update DB accordingly."""
    global TOLERANCE, AUTO_THRESHOLD, REVIEW_THRESHOLD, SPLIT_THRESHOLD
    global MAHNGEBUEHR_1_EUR, MAHNGEBUEHR_2_EUR, MAHNGEBUEHR_3_EUR
    params = load_params()
    TOLERANCE = _parse_float_param(params.get("Toleranz", 0.001), 0.001)
    AUTO_THRESHOLD = _parse_float_param(params.get("match_score_auto", 0.85), 0.85)
    REVIEW_THRESHOLD = _parse_float_param(params.get("match_score_review", 0.6), 0.6)
    SPLIT_THRESHOLD = _parse_float_param(params.get("split_threshold", 0.01), 0.01)
    MAHNGEBUEHR_1_EUR, MAHNGEBUEHR_2_EUR, MAHNGEBUEHR_3_EUR = _load_mahngebuehren(params)

    conn = load_db()
    cleaned_parents, cleaned_children = _cleanup_legacy_collective_splits(conn)
    if cleaned_parents:
        print(
            f"Bereinigt: {cleaned_parents} alte Sammelzahlungs-Parent(s), "
            f"{cleaned_children} Child-Zahlung(en) entfernt"
        )

    rows = conn.execute(
        """SELECT * FROM payments
           WHERE parent_payment_id IS NULL
             AND COALESCE(created_by, '') != 'manual'
             AND COALESCE(status_manual, 0) = 0
             AND COALESCE(amount_eur, 0) > 0
             AND COALESCE(akonto, 0) = 0
             AND COALESCE(schadensrechnung, 0) = 0
             AND (
               COALESCE(matched, 0) = 0
               OR (
                 matched = 1
                 AND invoice_id IS NOT NULL
                 AND COALESCE(match_rule, '') NOT LIKE 'split_%'
               )
             )"""
    ).fetchall()
    print(f"Zu matchende Zahlungen: {len(rows)}")

    for p in rows:
        is_remap_candidate = bool(p["matched"] == 1 and p["invoice_id"])
        explicit_multi_ids = extract_explicit_multi_invoice_numbers(p["reference_text"])
        if is_remap_candidate and len(explicit_multi_ids) < 2:
            continue

        res = match_payment_row(conn, p)

        if is_remap_candidate and not res.get("split"):
            # Existing single assignment with explicit multi-reference should not stay
            # silently mapped to one invoice when split matching failed.
            if len(explicit_multi_ids) >= 2:
                _rollback_existing_single_assignment(conn, p)
                conn.execute(
                    """UPDATE payments
                       SET invoice_id = NULL, matched = 0, match_score = NULL, match_rule = ?, created_by = 'auto'
                       WHERE payment_id = ?""",
                    (res.get("rule", "no_match"), p["payment_id"]),
                )
                conn.execute(
                    "INSERT INTO audit_log(payment_id, invoice_id, match_score, rule_used, automated, user) VALUES (?,?,?,?,1,'system')",
                    (p["payment_id"], None, 0.0, res.get("rule", "no_match")),
                )
            continue

        if res.get("invoice_id") and res.get("score", 0.0) >= AUTO_THRESHOLD:
            _apply_single_invoice(conn, p, res)

        elif res.get("split"):
            if is_remap_candidate:
                _rollback_existing_single_assignment(conn, p)
            child_rule = f"{res.get('rule', 'split')}_child"
            for inv_id, alloc in res["split"]:
                conn.execute(
                    """INSERT INTO payments(
                         invoice_id, parent_payment_id, source, booking_date, value_date, amount_eur,
                         reference_text, iban, beneficiary_name, matched, match_score, match_rule, created_by
                       ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, 'auto')""",
                    (
                        inv_id,
                        p["payment_id"],
                        p["source"],
                        p["booking_date"],
                        p["value_date"],
                        alloc,
                        p["reference_text"],
                        p["iban"],
                        p["beneficiary_name"],
                        res.get("score", 0.8),
                        child_rule,
                    ),
                )
                child_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                conn.execute(
                    "INSERT INTO audit_log(payment_id, invoice_id, match_score, rule_used, automated, user) VALUES (?,?,?,?,1,'system')",
                    (child_id, inv_id, res.get("score", 0.8), child_rule),
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
                """UPDATE payments
                   SET invoice_id = NULL, matched = 1, match_score = ?, match_rule = ?, created_by = 'auto'
                   WHERE payment_id = ?""",
                (res.get("score", 0.8), res.get("rule", "split_parent"), p["payment_id"]),
            )
            conn.execute(
                "INSERT INTO audit_log(payment_id, invoice_id, match_score, rule_used, automated, user) VALUES (?,?,?,?,1,'system')",
                (p["payment_id"], None, res.get("score", 0.8), res.get("rule", "split_parent")),
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
            else:
                conn.execute(
                    "UPDATE payments SET match_score = ?, match_rule = ? WHERE payment_id = ?",
                    (None if res.get("score", 0.0) <= 0 else res.get("score"), res.get("rule", "no_match"), p["payment_id"]),
                )

    if auto_commit:
        conn.commit()
    conn.close()


if __name__ == "__main__":
    apply_matching()
    print("Matching durchgeführt.")
