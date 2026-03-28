#!/bin/bash
echo "==================================================="
echo "Rechnungsverwaltung - Installer erstellen (Mac)"
echo "==================================================="
echo ""
echo "Installiere notwendige Pakete..."
pip install -r requirements.txt
pip install pyinstaller

echo ""
echo "Erstelle die ausfuehrbare Rechnungsverwaltung.app..."
pyinstaller --noconfirm --onedir --windowed --name "Rechnungsverwaltung" \
  --add-data "templates:templates/" \
  --add-data "static:static/" \
  --add-data "schema:schema/" \
  --add-data "parameters.json:." \
  run_desktop.py

echo ""
echo "Kopiere die fertige App auf den Desktop..."
rm -rf ~/Desktop/Rechnungsverwaltung.app
cp -r dist/Rechnungsverwaltung.app ~/Desktop/Rechnungsverwaltung.app

echo ""
echo "==================================================="
echo "FERTIG!"
echo "Die App wurde aktualisiert und liegt auf deinem Schreibtisch."
echo "==================================================="
