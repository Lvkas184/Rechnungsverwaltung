# Rechnungsverwaltung – Codex Version

## Workflow
1. Google Sheets → JSON: `python src/export_sheets.py` (Service Account).
2. Import JSON → DB: `python src/import_to_db.py`.
3. Matching ausführen: `python src/cli.py --match`.
4. Status aktualisieren: `python src/cli.py --status`.
5. Mahnlogik ausführen: `python src/cli.py --mahnung`.

## Parameter
Siehe `parameters.json`. Dort lassen sich z. B. `Toleranz`, `due_days_1`, `match_score_auto` anpassen.

## Tests
- `pytest`
- oder `python -m unittest discover tests`

## Hinweise
- `data/` enthält die exportierten Sheet-Dumps.
- `Manuelle ReNr Map` wird für regelbasiertes Matching genutzt.
- `audit_log` speichert automatische Zuordnungen und Unmatched-Entscheidungen.
- `apply_matching()` ist idempotent, weil nur Zahlungen mit `matched = 0` verarbeitet werden.
