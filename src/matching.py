import json
import re
import sqlite3
from datetime import datetime
from difflib import SequenceMatcher

DB = "rechnungsverwaltung.db"

try:
    from rapidfuzz import fuzz  # type: ignore

    def token_set_ratio(a: str, b: str) -> float:
        return fuzz.token_set_ratio(a, b) / 100.0

except Exception:

    def token_set_ratio(a: str, b: str) -> float:
        return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def load_parameters(path="parameters.json"):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


PARAMS = load_parameters()
TOLERANCE = float(PARAMS.get("Toleranz", 0.001))
AUTO_THRESHOLD = float(PARAMS.get("match_score_auto", 0.85))
REVIEW_THRESHOLD = float(PARAMS.get("match_score_review", 0.6))
SPLIT_THRESHOLD = float(PARAMS.get("split_threshold", 0.01))

RE_PATTERNS = [
    re.compile(
        r"(?:RE(?:\.)?|RN|ReNr|RENr|re\s*nr|re\.?\s*nr|Re\.*\s*Nr\.?)\s*[:\-]?\s*([0-9]{4,12})",
        re.I,
    ),
    re.compile(r"\b([0-9]{6,9})\b"),
]


def extract_invoice_number(text):
    if not text:
        return None
    value = str(text)
    for p in RE_PATTERNS:
        m = p.search(value)
        if m:
            try:
                raw = m.group(1)
                if len(raw) > 9:
                    raw = raw[:6]
                return int(raw)
            except Exception:
                continue
    return None


def amount_similarity(a, b):
    if a is None or b is None:
        return 0.0
    if a == 0 and b == 0:
        return 1.0
    return max(0.0, 1 - abs(a - b) / max(abs(a), abs(b), 1.0))


def name_similarity(a, b):
    if not a or not b:
        return 0.0
    return token_set_ratio(str(a), str(b))


def compute_score(rule_score, name_score=0.0, amount_score=0.0):
    return min(1.0, rule_score * 0.6 + name_score * 0.25 + amount_score * 0.15)


def load_db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn


def find_invoice_by_id(conn, invoice_id):
    if not invoice_id:
        return None
    return conn.execute("SELECT * FROM invoices WHERE invoice_id = ?", (invoice_id,)).fetchone()


def find_candidates_by_amount(conn, amount, limit=50):
    if amount is None:
        return []
    low = amount * (1 - 0.05)
    high = amount * (1 + 0.05)
    return conn.execute(
        """
        SELECT * FROM invoices
        WHERE (COALESCE(paid_sum_eur,0) < COALESCE(amount_gross,0) OR status != 'Bezahlt')
          AND amount_gross BETWEEN ? AND ?
        LIMIT ?
        """,
        (low, high, limit),
    ).fetchall()


def match_payment_row(conn, payment):
    inv_id = extract_invoice_number(payment["reference_text"])
    if inv_id:
        inv = find_invoice_by_id(conn, inv_id)
        if inv:
            return {"invoice_id": inv["invoice_id"], "score": 1.0, "rule": "regex_invoice"}

    row = conn.execute(
        "SELECT mapped_invoice_id FROM manual_map WHERE signature = ?",
        (payment["reference_text"],),
    ).fetchone()
    if row:
        inv = find_invoice_by_id(conn, row["mapped_invoice_id"])
        if inv:
            return {"invoice_id": inv["invoice_id"], "score": 0.95, "rule": "manual_map_exact"}

    amount = payment["amount_eur"]
    name = payment["beneficiary_name"] or payment["reference_text"] or ""

    rows = conn.execute(
        "SELECT * FROM invoices WHERE amount_gross = ? AND name LIKE ? LIMIT 5",
        (amount, f"%{name}%"),
    ).fetchall()
    if rows:
        return {"invoice_id": rows[0]["invoice_id"], "score": 0.9, "rule": "exact_amount_name"}

    best = None
    for inv in find_candidates_by_amount(conn, amount):
        n_score = name_similarity(inv["name"], name)
        a_score = amount_similarity(amount or 0, inv["amount_gross"] or 0)
        score = compute_score(0.85, n_score, a_score)
        if best is None or score > best["score"]:
            best = {"invoice_id": inv["invoice_id"], "score": score, "rule": "fuzzy_name_amount"}

    if best and best["score"] >= REVIEW_THRESHOLD:
        return best

    ref = str(payment["reference_text"] or "").lower()
    if (
        "sammel" in ref
        or "sammelüberweisung" in ref
        or "sammelueberweisung" in ref
        or (payment["amount_eur"] and payment["amount_eur"] > 10000)
    ):
        invoices = conn.execute(
            """
            SELECT * FROM invoices
            WHERE COALESCE(status,'Offen') != 'Bezahlt'
            ORDER BY invoice_id ASC
            LIMIT 100
            """
        ).fetchall()
        remaining = payment["amount_eur"] or 0.0
        assigned = []
        for inv in invoices:
            needed = (inv["amount_gross"] or 0.0) - (inv["paid_sum_eur"] or 0.0)
            if needed <= 0:
                continue
            alloc = min(needed, remaining)
            if alloc > 0:
                assigned.append((inv["invoice_id"], alloc))
                remaining -= alloc
            if remaining <= SPLIT_THRESHOLD:
                break
        if assigned:
            return {"invoice_id": None, "score": 0.8, "rule": "split_collective", "split": assigned}

    return {"invoice_id": None, "score": 0.0, "rule": "no_match"}


def _apply_single_invoice(conn, payment, res):
    conn.execute(
        """
        UPDATE payments
        SET invoice_id = ?, matched = 1, match_score = ?, match_rule = ?, created_by = 'auto'
        WHERE payment_id = ?
        """,
        (res["invoice_id"], res["score"], res["rule"], payment["payment_id"]),
    )
    conn.execute(
        "INSERT INTO audit_log(payment_id, invoice_id, match_score, rule_used, automated, user) VALUES (?,?,?,?,1,'system')",
        (payment["payment_id"], res["invoice_id"], res["score"], res["rule"]),
    )
    conn.execute(
        """
        UPDATE invoices
        SET paid_sum_eur = COALESCE(paid_sum_eur,0) + ?,
            payment_count = COALESCE(payment_count,0) + 1,
            last_payment_date = ?
        WHERE invoice_id = ?
        """,
        (payment["amount_eur"], datetime.utcnow().isoformat(), res["invoice_id"]),
    )


def apply_matching(auto_commit=True):
    conn = load_db()
    rows = conn.execute(
        "SELECT * FROM payments WHERE COALESCE(matched, 0) = 0"
    ).fetchall()

    for p in rows:
        res = match_payment_row(conn, p)
        if res.get("invoice_id"):
            _apply_single_invoice(conn, p, res)
        elif res.get("rule") == "split_collective":
            for inv_id, alloc in res["split"]:
                conn.execute(
                    """
                    INSERT INTO payments(
                      invoice_id, source, booking_date, value_date, amount_eur,
                      reference_text, matched, match_score, match_rule, created_by
                    ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, 'auto_split', 'auto')
                    """,
                    (
                        inv_id,
                        p["source"],
                        p["booking_date"],
                        p["value_date"],
                        alloc,
                        p["reference_text"],
                        0.8,
                    ),
                )
                child_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                conn.execute(
                    "INSERT INTO audit_log(payment_id, invoice_id, match_score, rule_used, automated, user) VALUES (?,?,?,?,1,'system')",
                    (child_id, inv_id, 0.8, "auto_split"),
                )
                conn.execute(
                    """
                    UPDATE invoices
                    SET paid_sum_eur = COALESCE(paid_sum_eur,0) + ?,
                        payment_count = COALESCE(payment_count,0) + 1,
                        last_payment_date = ?
                    WHERE invoice_id = ?
                    """,
                    (alloc, datetime.utcnow().isoformat(), inv_id),
                )
            conn.execute(
                "UPDATE payments SET matched = 1, match_rule = 'split_parent', created_by='auto' WHERE payment_id = ?",
                (p["payment_id"],),
            )
            conn.execute(
                "INSERT INTO audit_log(payment_id, invoice_id, match_score, rule_used, automated, user) VALUES (?,?,?,?,1,'system')",
                (p["payment_id"], None, 0.8, "split_parent"),
            )
        else:
            conn.execute(
                "INSERT INTO audit_log(payment_id, invoice_id, match_score, rule_used, automated, user) VALUES (?,?,?,?,1,'system')",
                (p["payment_id"], None, 0.0, res["rule"]),
            )

    if auto_commit:
        conn.commit()
    conn.close()
