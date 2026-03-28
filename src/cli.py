"""CLI Entrypoint für Export/Import/Matching/Status/Mahnung."""

import argparse
import subprocess

from import_to_db import import_data
import argparse

from mahnung import run_mahnung
from matching import apply_matching
from status import update_all


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
    if args.match:
        apply_matching()
    if args.status:
        update_all()
    if args.mahnung:
        run_mahnung()


if __name__ == "__main__":
    main()
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--match", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--mahnung", action="store_true")
    args = parser.parse_args()

    if args.match:
        apply_matching()
        print("Matching done.")
    if args.status:
        update_all()
        print("Status updated.")
    if args.mahnung:
        run_mahnung()
        print("Mahnungen verarbeitet.")
