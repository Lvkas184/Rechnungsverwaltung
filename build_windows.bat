@echo off
echo ===================================================
echo Rechnungsverwaltung - Installer erstellen (Windows)
echo ===================================================
echo.
echo Installiere notwendige Pakete...
pip install -r requirements.txt
pip install pyinstaller

echo.
echo Erstelle die ausfuehrbare Rechnungsverwaltung.exe...
pyinstaller --noconfirm --onedir --windowed --name "Rechnungsverwaltung" ^
  --add-data "templates;templates/" ^
  --add-data "static;static/" ^
  --add-data "schema;schema/" ^
  --add-data "parameters.json;." ^
  run_desktop.py

echo.
echo ===================================================
echo FERTIG! 
echo Die App wurde im Ordner "dist\Rechnungsverwaltung" erstellt.
echo Du kannst den kopletten Ordner "Rechnungsverwaltung" 
echo nun an deine Mutter kopieren.
echo Zum Starten soll sie einfach doppelt auf
echo "Rechnungsverwaltung.exe" klicken.
echo ===================================================
pause
