import argparse

from mahnung import run_mahnung
from matching import apply_matching
from status import update_all


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
