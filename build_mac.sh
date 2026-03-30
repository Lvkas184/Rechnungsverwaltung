#!/bin/bash
set -euo pipefail

echo "==================================================="
echo "Rechnungsverwaltung - Installer erstellen (Mac)"
echo "==================================================="
echo ""

VENV_DIR=".venv-build"
PY_BIN="${VENV_DIR}/bin/python"
PYINSTALLER_BIN="${VENV_DIR}/bin/pyinstaller"

if [ ! -d "${VENV_DIR}" ]; then
  echo "Erstelle Build-Umgebung (${VENV_DIR})..."
  python3 -m venv "${VENV_DIR}"
fi

echo "Installiere/aktualisiere Build-Abhaengigkeiten..."
"${PY_BIN}" -m pip install --upgrade pip setuptools wheel
"${PY_BIN}" -m pip install -r requirements.txt pyinstaller

echo ""
echo "Erstelle die ausfuehrbare Rechnungsverwaltung.app..."
"${PYINSTALLER_BIN}" --noconfirm --clean --onedir --windowed --name "Rechnungsverwaltung" \
  --add-data "templates:templates/" \
  --add-data "static:static/" \
  --add-data "schema:schema/" \
  --add-data "parameters.json:." \
  run_desktop.py

echo ""
echo "Kopiere die fertige App auf den Desktop..."
rm -rf ~/Desktop/Rechnungsverwaltung.app
cp -R dist/Rechnungsverwaltung.app ~/Desktop/Rechnungsverwaltung.app

echo ""
echo "==================================================="
echo "FERTIG!"
echo "Die App wurde aktualisiert und liegt auf deinem Schreibtisch."
echo "==================================================="
