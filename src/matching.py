"""Matching-Engine for payment-to-invoice allocation."""

import json
import re
import sqlite3
from datetime import datetime
from difflib import SequenceMatcher

try:
    from rapidfuzz import fuzz  # type: ignore
except Exception:  # pragma: no cover
    fuzz = None

DB = "rechnungsverwaltung.db"
PARAM_PATH = "parameters.json"


def load_params(path=PARAM_PATH):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


params = load_params()
TOLERANCE = float(params.get("Toleranz", 0.001))
AUTO_THRESHOLD = float(params.get("match_score_auto", 0.85))
REVIEW_THRESHOLD = float(params.get("match_score_review", 0.6))
SPLIT_THRESHOLD = float(params.get("split_threshold", 0.01))

RE_PATTERNS = [
    re.compile(r"(?:RE(?:\.)?\s*Nr\.?|RN|RENr|ReNr|re\s*nr|re\.?\s*nr|Re\.*\s*Nr\.?)\s*[:\-]?\s*([0-9]{4,12})", re.I),
    re.compile(r"\b([0-9]{6,9})\b"),
]


def extract_invoice_number(text):
    if not text:
        return None
    for p in RE_PATTERNS:
        m = p.search(str(text))
        if m:
            raw = m.group(1)
            if len(raw) > 9:
                raw = raw[:6]
            try:
                return int(raw)
            except ValueError:
                continue
    return None


def amount_similarity(a, b):
    try:
        if a is None or b is None:
            return 0.0
        a = float(a)
        b = float(b)
        if a == b:
            return 1.0
        denom = max(abs(a), abs(b), 1.0)
        return max(0.0, 1 - abs(a - b) / denom)
    except Exception:
        return 0.0


def name_similarity(a, b):
    if not a or not b:
        return 0.0
    if fuzz is not None:
        return fuzz.token_set_ratio(str(a), str(b)) / 100.0
    return SequenceMatcher(None, str(a).lower(), str(b).lower()).ratio()


def compute_score(base_weight, name_score=0.0, amount_score=0.0):
    return min(1.0, base_weight * 0.6 + name_score * 0.25 + amount_score * 0.15)


def load_db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn


def find_invoice_by_id(conn, invoice_id):
    if not invoice_id:
        return None
    return conn.execute("SELECT * FROM invoices WHERE invoice_id = ?", (invoice_id,)).fetchone()


def find_candidates_by_amount(conn, amount, pct=0.05, limit=200):
    if amount is None:
        return conn.execute("SELECT * FROM invoices WHERE status != 'Bezahlt' LIMIT ?", (limit,)).fetchall()
    low = amount * (1 - pct)
    high = amount * (1 + pct)
    return conn.execute(
        "SELECT * FROM invoices WHERE (status != 'Bezahlt' OR status IS NULL) AND amount_gross BETWEEN ? AND ? LIMIT ?",
        (low, high, limit),
    ).fetchall()


def match_payment_row(conn, payment_row):
    ref = payment_row["reference_text"]
    amt = payment_row["amount_eur"]
    ben = payment_row["beneficiary_name"] or ref

    inv_id = extract_invoice_number(ref)
    if inv_id:
        inv = find_invoice_by_id(conn, inv_id)
        if inv:
            return {"invoice_id": inv_id, "score": 1.0, "rule": "regex_invoice"}

    exact_map = conn.execute("SELECT mapped_invoice_id FROM manual_map WHERE signature = ?", (ref,)).fetchone()
    if exact_map and exact_map["mapped_invoice_id"]:
        inv = find_invoice_by_id(conn, exact_map["mapped_invoice_id"])
        if inv:
            return {"invoice_id": inv["invoice_id"], "score": 0.95, "rule": "manual_map_exact"}

    for row in conn.execute("SELECT mapped_invoice_id, signature FROM manual_map").fetchall():
        signature = row["signature"]
        if signature and ref and signature in ref:
            inv = find_invoice_by_id(conn, row["mapped_invoice_id"])
            if inv:
                return {"invoice_id": inv["invoice_id"], "score": 0.92, "rule": "manual_map_contains"}

    if amt is not None:
        rows = conn.execute(
            "SELECT * FROM invoices WHERE amount_gross = ? AND name LIKE ? LIMIT 5",
            (amt, f"%{ben}%"),
        ).fetchall()
        if rows:
            return {"invoice_id": rows[0]["invoice_id"], "score": 0.9, "rule": "exact_amount_name"}

    best = None
    for inv in find_candidates_by_amount(conn, amt):
        nscore = name_similarity(inv["name"], ben)
        ascore = amount_similarity(amt, inv["amount_gross"])
        score = compute_score(0.85, nscore, ascore)
        if not best or score > best["score"]:
            best = {"invoice_id": inv["invoice_id"], "score": score, "rule": "fuzzy_name_amount"}

    if best:
        if best["score"] >= AUTO_THRESHOLD:
            return best
        if best["score"] >= REVIEW_THRESHOLD:
            return best

    text_lower = (ref or "").lower()
    if "sammel" in text_lower or "sammelüberweisung" in text_lower or (amt and float(amt) > 10000):
        invs = conn.execute(
            "SELECT * FROM invoices WHERE status != 'Bezahlt' OR status IS NULL ORDER BY invoice_id ASC LIMIT 200"
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

    return {"invoice_id": None, "score": 0.0, "rule": "no_match"}


def apply_matching(auto_commit=True):
    conn = load_db()
    rows = conn.execute("SELECT * FROM payments WHERE matched = 0 OR matched IS NULL").fetchall()
    print(f"Zu matchende Zahlungen: {len(rows)}")

    for p in rows:
        res = match_payment_row(conn, p)

        if res.get("invoice_id") and res.get("score", 0.0) >= AUTO_THRESHOLD:
            conn.execute(
                "UPDATE payments SET invoice_id=?, matched=1, match_score=?, match_rule=?, created_by='auto' WHERE payment_id=?",
                (res["invoice_id"], res["score"], res["rule"], p["payment_id"]),
            )
            conn.execute(
                "INSERT INTO audit_log(payment_id, invoice_id, match_score, rule_used, automated, user) VALUES (?,?,?,?,1,'system')",
                (p["payment_id"], res["invoice_id"], res["score"], res["rule"]),
            )
            conn.execute(
                "UPDATE invoices SET paid_sum_eur = COALESCE(paid_sum_eur,0) + ?, payment_count = COALESCE(payment_count,0)+1, last_payment_date = ? WHERE invoice_id=?",
                (p["amount_eur"], datetime.utcnow().isoformat(), res["invoice_id"]),
            )

        elif res.get("rule") == "split_collective" and res.get("split"):
            for inv_id, alloc in res["split"]:
                conn.execute(
                    "INSERT INTO payments(invoice_id, source, booking_date, value_date, amount_eur, reference_text, matched, match_score, match_rule, created_by) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (inv_id, p["source"], p["booking_date"], p["value_date"], alloc, p["reference_text"], 1, 0.8, "auto_split", "auto"),
                )
                child_payment_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                conn.execute(
                    "INSERT INTO audit_log(payment_id, invoice_id, match_score, rule_used, automated, user) VALUES (?,?,?,?,1,'system')",
                    (child_payment_id, inv_id, 0.8, "auto_split"),
                )
                conn.execute(
                    "UPDATE invoices SET paid_sum_eur = COALESCE(paid_sum_eur,0) + ?, payment_count = COALESCE(payment_count,0)+1, last_payment_date = ? WHERE invoice_id=?",
                    (alloc, datetime.utcnow().isoformat(), inv_id),
                )

            conn.execute("UPDATE payments SET matched=1, match_rule='split_parent', created_by='auto' WHERE payment_id=?", (p["payment_id"],))
            conn.execute(
                "INSERT INTO audit_log(payment_id, invoice_id, match_score, rule_used, automated, user) VALUES (?,?,?,?,1,'system')",
                (p["payment_id"], None, 0.8, "split_parent"),
            )

        else:
            conn.execute(
                "INSERT INTO audit_log(payment_id, invoice_id, match_score, rule_used, automated, user) VALUES (?,?,?,?,1,'system')",
                (p["payment_id"], res.get("invoice_id"), res.get("score", 0.0), res.get("rule")),
            )
            if res.get("invoice_id") and res.get("score", 0.0) >= REVIEW_THRESHOLD:
                conn.execute(
                    "UPDATE payments SET match_score=?, match_rule=? WHERE payment_id=?",
                    (res["score"], res["rule"], p["payment_id"]),
                )

    if auto_commit:
        conn.commit()
    conn.close()


if __name__ == "__main__":
    apply_matching()
    print("Matching durchgeführt.")
