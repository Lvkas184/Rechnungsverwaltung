"""Import-Historie und batchweises Rollback."""

import json

from src.db import get_db


def _row_to_dict(row):
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def _json_dumps(value):
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def _json_loads(value, default=None):
    if not value:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def _fetch_invoice_row(conn, invoice_id):
    return conn.execute("SELECT * FROM invoices WHERE invoice_id = ?", (invoice_id,)).fetchone()


def _fetch_payment_row(conn, payment_id):
    return conn.execute("SELECT * FROM payments WHERE payment_id = ?", (payment_id,)).fetchone()


def begin_import_batch(conn, import_type, source_label, filename=None, created_by="upload"):
    cur = conn.execute(
        """
        INSERT INTO import_batches(import_type, source_label, filename, created_by, status)
        VALUES (?, ?, ?, ?, 'active')
        """,
        (import_type, source_label, filename, created_by),
    )
    return cur.lastrowid


def finish_import_batch(conn, import_batch_id, imported, skipped):
    conn.execute(
        """
        UPDATE import_batches
        SET row_count_imported = ?, row_count_skipped = ?
        WHERE import_batch_id = ?
        """,
        (int(imported or 0), int(skipped or 0), import_batch_id),
    )


def record_invoice_import(conn, import_batch_id, invoice_id, before_row, touched_fields):
    after_row = _fetch_invoice_row(conn, invoice_id)
    action = "insert" if before_row is None else "update"
    conn.execute(
        """
        INSERT INTO import_batch_items(
            import_batch_id, entity_type, entity_id, action,
            fields_touched, before_state, after_state
        ) VALUES (?, 'invoice', ?, ?, ?, ?, ?)
        """,
        (
            import_batch_id,
            invoice_id,
            action,
            _json_dumps(list(touched_fields or [])),
            _json_dumps(_row_to_dict(before_row)),
            _json_dumps(_row_to_dict(after_row)),
        ),
    )


def record_payment_import(conn, import_batch_id, payment_id):
    after_row = _fetch_payment_row(conn, payment_id)
    conn.execute(
        """
        INSERT INTO import_batch_items(
            import_batch_id, entity_type, entity_id, action,
            fields_touched, before_state, after_state
        ) VALUES (?, 'payment', ?, 'insert', NULL, NULL, ?)
        """,
        (
            import_batch_id,
            payment_id,
            _json_dumps(_row_to_dict(after_row)),
        ),
    )


def _expand_child_payment_ids(conn, payment_ids):
    expanded = set(int(pid) for pid in payment_ids)
    frontier = set(expanded)
    while frontier:
        marks = ",".join(["?"] * len(frontier))
        rows = conn.execute(
            f"SELECT payment_id FROM payments WHERE parent_payment_id IN ({marks})",
            list(frontier),
        ).fetchall()
        next_ids = {int(row["payment_id"]) for row in rows if int(row["payment_id"]) not in expanded}
        expanded.update(next_ids)
        frontier = next_ids
    return sorted(expanded)


def _invoice_fields_changed(current_row, expected_after_state, touched_fields):
    if current_row is None:
        return True
    current = _row_to_dict(current_row)
    expected = expected_after_state or {}
    for field in touched_fields:
        if current.get(field) != expected.get(field):
            return True
    return False


def _count_invoice_external_dependencies(conn, invoice_id, excluded_payment_ids):
    params = [invoice_id]
    payment_clause = ""
    if excluded_payment_ids:
        marks = ",".join(["?"] * len(excluded_payment_ids))
        payment_clause = f" AND payment_id NOT IN ({marks})"
        params.extend(excluded_payment_ids)
    payment_count = conn.execute(
        f"SELECT COUNT(*) FROM payments WHERE invoice_id = ?{payment_clause}",
        params,
    ).fetchone()[0]
    audit_params = [invoice_id]
    audit_clause = ""
    if excluded_payment_ids:
        marks = ",".join(["?"] * len(excluded_payment_ids))
        audit_clause = f" AND (payment_id IS NULL OR payment_id NOT IN ({marks}))"
        audit_params.extend(excluded_payment_ids)
    audit_count = conn.execute(
        f"SELECT COUNT(*) FROM audit_log WHERE invoice_id = ?{audit_clause}",
        audit_params,
    ).fetchone()[0]
    map_count = conn.execute(
        "SELECT COUNT(*) FROM manual_map WHERE mapped_invoice_id = ?",
        (invoice_id,),
    ).fetchone()[0]
    return int(payment_count or 0), int(map_count or 0), int(audit_count or 0)


def _rollback_payment_items(conn, payment_ids):
    if not payment_ids:
        return 0
    all_payment_ids = _expand_child_payment_ids(conn, payment_ids)
    marks = ",".join(["?"] * len(all_payment_ids))
    conn.execute(f"DELETE FROM audit_log WHERE payment_id IN ({marks})", all_payment_ids)
    conn.execute(f"DELETE FROM payments WHERE payment_id IN ({marks})", all_payment_ids)
    return len(all_payment_ids)


def rollback_import_batch(import_batch_id, db_path=None):
    conn = get_db(db_path)
    try:
        batch = conn.execute(
            "SELECT * FROM import_batches WHERE import_batch_id = ?",
            (import_batch_id,),
        ).fetchone()
        if not batch:
            return {"ok": False, "error": "Import nicht gefunden."}
        if (batch["status"] or "").lower() == "rolled_back":
            return {"ok": False, "error": "Dieser Import wurde bereits rückgängig gemacht."}

        items = conn.execute(
            """
            SELECT *
            FROM import_batch_items
            WHERE import_batch_id = ?
            ORDER BY import_batch_item_id DESC
            """,
            (import_batch_id,),
        ).fetchall()
        if not items:
            conn.execute(
                """
                UPDATE import_batches
                SET status = 'rolled_back',
                    rollback_note = 'Keine importierten Datensätze vorhanden.',
                    rollback_at = CURRENT_TIMESTAMP
                WHERE import_batch_id = ?
                """,
                (import_batch_id,),
            )
            conn.commit()
            return {
                "ok": True,
                "message": "Import ohne Datensätze als rückgängig markiert.",
                "deleted_payments": 0,
                "deleted_invoices": 0,
                "restored_invoices": 0,
            }

        imported_payment_ids = [
            int(item["entity_id"])
            for item in items
            if item["entity_type"] == "payment" and item["action"] == "insert"
        ]
        payment_delete_ids = _expand_child_payment_ids(conn, imported_payment_ids) if imported_payment_ids else []

        blockers = []
        invoice_items = [item for item in items if item["entity_type"] == "invoice"]
        for item in invoice_items:
            invoice_id = int(item["entity_id"])
            touched_fields = _json_loads(item["fields_touched"], []) or []
            after_state = _json_loads(item["after_state"], {}) or {}
            current_row = _fetch_invoice_row(conn, invoice_id)

            if item["action"] == "insert":
                if current_row is None:
                    continue
                if _invoice_fields_changed(current_row, after_state, touched_fields):
                    blockers.append(
                        f"Rechnung #{invoice_id} wurde nach dem Import in den importierten Feldern geändert."
                    )
                    continue
                payment_count, map_count, audit_count = _count_invoice_external_dependencies(
                    conn, invoice_id, payment_delete_ids
                )
                if payment_count or map_count or audit_count:
                    blockers.append(
                        f"Rechnung #{invoice_id} wird noch verwendet "
                        f"({payment_count} Zahlung(en), {map_count} manuelle Zuordnung(en), {audit_count} Audit-Eintrag/Eintraege)."
                    )
            else:
                if current_row is None:
                    blockers.append(f"Rechnung #{invoice_id} existiert nicht mehr.")
                    continue
                if _invoice_fields_changed(current_row, after_state, touched_fields):
                    blockers.append(
                        f"Rechnung #{invoice_id} wurde nach dem Import geändert und kann nicht sauber zurückgesetzt werden."
                    )

        if blockers:
            return {
                "ok": False,
                "error": "Rollback abgebrochen: " + " ".join(blockers[:5]),
            }

        deleted_payments = _rollback_payment_items(conn, imported_payment_ids)
        deleted_invoices = 0
        restored_invoices = 0

        for item in invoice_items:
            invoice_id = int(item["entity_id"])
            touched_fields = _json_loads(item["fields_touched"], []) or []
            before_state = _json_loads(item["before_state"], {}) or {}

            if item["action"] == "insert":
                current_row = _fetch_invoice_row(conn, invoice_id)
                if current_row is not None:
                    conn.execute("DELETE FROM invoices WHERE invoice_id = ?", (invoice_id,))
                    deleted_invoices += 1
                continue

            if not touched_fields:
                continue
            set_sql = ", ".join(f"{field} = ?" for field in touched_fields)
            params = [before_state.get(field) for field in touched_fields]
            params.append(invoice_id)
            conn.execute(f"UPDATE invoices SET {set_sql} WHERE invoice_id = ?", params)
            restored_invoices += 1

        conn.execute(
            """
            UPDATE import_batches
            SET status = 'rolled_back',
                rollback_at = CURRENT_TIMESTAMP,
                rollback_note = ?
            WHERE import_batch_id = ?
            """,
            (
                (
                    f"{deleted_payments} Zahlung(en) entfernt, "
                    f"{deleted_invoices} Rechnung(en) gelöscht, "
                    f"{restored_invoices} Rechnung(en) zurückgesetzt"
                ),
                import_batch_id,
            ),
        )
        conn.commit()
        return {
            "ok": True,
            "message": (
                f"Import rückgängig gemacht: {deleted_payments} Zahlung(en) entfernt, "
                f"{deleted_invoices} Rechnung(en) gelöscht, "
                f"{restored_invoices} Rechnung(en) zurückgesetzt."
            ),
            "deleted_payments": deleted_payments,
            "deleted_invoices": deleted_invoices,
            "restored_invoices": restored_invoices,
        }
    except Exception as exc:
        conn.rollback()
        return {"ok": False, "error": f"Rollback fehlgeschlagen: {exc}"}
    finally:
        conn.close()


def _build_item_preview(item):
    after_state = _json_loads(item["after_state"], {}) or {}
    doc_type = str(after_state.get("document_type") or "").strip().lower()
    status = str(after_state.get("status") or "").strip().lower()
    is_credit_note = item["entity_type"] == "invoice" and (
        doc_type == "gutschrift" or status == "gutschrift"
    )
    preview = {
        "entity_type": item["entity_type"],
        "display_type": "Gutschrift" if is_credit_note else ("Rechnung" if item["entity_type"] == "invoice" else "Zahlung"),
        "entity_id": item["entity_id"],
        "action": item["action"],
    }
    if item["entity_type"] == "invoice":
        preview.update(
            {
                "title": f"{'Gutschrift' if is_credit_note else 'Rechnung'} #{item['entity_id']}",
                "subtitle": after_state.get("name") or "—",
                "amount": after_state.get("amount_gross"),
                "date": after_state.get("issue_date"),
            }
        )
    else:
        preview.update(
            {
                "title": f"Zahlung #{item['entity_id']}",
                "subtitle": after_state.get("beneficiary_name") or "—",
                "amount": after_state.get("amount_eur"),
                "date": after_state.get("booking_date") or after_state.get("value_date"),
                "reference_text": after_state.get("reference_text") or "",
                "source": after_state.get("source") or "",
            }
        )
    return preview


def fetch_import_batches(limit=25, db_path=None):
    conn = get_db(db_path)
    try:
        batches = conn.execute(
            """
            SELECT *
            FROM import_batches
            ORDER BY import_batch_id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        if not batches:
            return []

        batch_ids = [int(batch["import_batch_id"]) for batch in batches]
        marks = ",".join(["?"] * len(batch_ids))
        items = conn.execute(
            f"""
            SELECT *
            FROM import_batch_items
            WHERE import_batch_id IN ({marks})
            ORDER BY import_batch_id DESC, import_batch_item_id ASC
            """,
            batch_ids,
        ).fetchall()

        grouped_items = {batch_id: [] for batch_id in batch_ids}
        for item in items:
            grouped_items[int(item["import_batch_id"])].append(_build_item_preview(item))

        result = []
        for batch in batches:
            batch_id = int(batch["import_batch_id"])
            previews = grouped_items.get(batch_id, [])
            inserted_rechnungen = sum(
                1
                for item in previews
                if item["entity_type"] == "invoice"
                and item["action"] == "insert"
                and item.get("display_type") == "Rechnung"
            )
            inserted_gutschriften = sum(
                1
                for item in previews
                if item["entity_type"] == "invoice"
                and item["action"] == "insert"
                and item.get("display_type") == "Gutschrift"
            )
            updated_rechnungen = sum(
                1
                for item in previews
                if item["entity_type"] == "invoice"
                and item["action"] == "update"
                and item.get("display_type") == "Rechnung"
            )
            updated_gutschriften = sum(
                1
                for item in previews
                if item["entity_type"] == "invoice"
                and item["action"] == "update"
                and item.get("display_type") == "Gutschrift"
            )
            inserted_invoices = sum(
                1 for item in previews if item["entity_type"] == "invoice" and item["action"] == "insert"
            )
            updated_invoices = sum(
                1 for item in previews if item["entity_type"] == "invoice" and item["action"] == "update"
            )
            inserted_payments = sum(
                1 for item in previews if item["entity_type"] == "payment" and item["action"] == "insert"
            )
            result.append(
                {
                    **_row_to_dict(batch),
                    "items": previews,
                    "inserted_rechnungen": inserted_rechnungen,
                    "inserted_gutschriften": inserted_gutschriften,
                    "updated_rechnungen": updated_rechnungen,
                    "updated_gutschriften": updated_gutschriften,
                    "inserted_invoices": inserted_invoices,
                    "updated_invoices": updated_invoices,
                    "inserted_payments": inserted_payments,
                    "item_count": len(previews),
                }
            )
        return result
    finally:
        conn.close()
