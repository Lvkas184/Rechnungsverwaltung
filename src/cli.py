"""CLI Entrypoint für Export/Import/Matching/Status/Mahnung."""

import argparse
import subprocess

from src.import_to_db import import_data
from src.matching import apply_matching
from src.status import update_all
from src.mahnung import run_mahnung


def main():
    parser = argparse.ArgumentParser(description="Rechnungsverwaltung CLI")
    parser.add_argument("--export", action="store_true", help="Exportiere Google Sheets zu data/*.json")
    parser.add_argument("--import", dest="do_import", action="store_true", help="Importiere data/*.json in die SQLite DB")
    parser.add_argument("--match", action="store_true", help="Führe Matching durch")
    parser.add_argument("--status", action="store_true", help="Aktualisiere Rechnungsstatus")
    parser.add_argument("--mahnung", action="store_true", help="Führe Mahnlogik aus")
    args = parser.parse_args()

    if args.export:
        subprocess.run(["python", "src/export_sheets.py"], check=False)
    if args.do_import:
        import_data()
        print("Import abgeschlossen.")
    if args.match:
        apply_matching()
        print("Matching done.")
    if args.status:
        update_all()
        print("Status updated.")
    if args.mahnung:
        run_mahnung()
        print("Mahnungen verarbeitet.")


if __name__ == "__main__":
    main()
