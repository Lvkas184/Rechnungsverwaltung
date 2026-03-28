# Rechnungsverwaltung – Codex Version

Dieses Repository enthält eine Basis-Implementierung für die Rechnungsverwaltung mit:
- Import aus Google Sheets (über JSON-Export),
- persistenter SQLite-Datenbank,
- Matching-Engine für Zahlungen,
- Status-Berechnung,
- Mahnlogik.

## Struktur

```text
rechnungsverwaltung-codex/
├─ data/
├─ schema/
│  └─ schema.sql
├─ src/
│  ├─ export_sheets.py
│  ├─ import_to_db.py
│  ├─ matching.py
│  ├─ status.py
│  ├─ mahnung.py
│  └─ cli.py
├─ tests/
├─ parameters.json
└─ requirements.txt
```

## Vorbereitung

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Dann in `src/export_sheets.py` anpassen:
- `SPREADSHEET_ID`
- `SERVICE_ACCOUNT_FILE`

## Workflow

1. Exportiere alle Sheets:

```bash
python src/export_sheets.py
```

2. Importiere JSON nach SQLite:

```bash
python src/import_to_db.py
```

3. Matching:

```bash
python src/cli.py --match
```

4. Status aktualisieren:

```bash
python src/cli.py --status
```

5. Mahnlauf:

```bash
python src/cli.py --mahnung
```

6. Tests:

```bash
pytest -q
```

## Hinweise
- `manual_map` wird für regelbasiertes Matching verwendet.
- Jede automatische Entscheidung wird in `audit_log` geschrieben.
- `apply_matching()` verarbeitet nur Zahlungen mit `matched = 0` (idempotent bei Wiederholung).
