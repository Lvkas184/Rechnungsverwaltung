@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "APP_NAME=Rechnungsverwaltung"
set "VENV_DIR=.venv-build-win"
set "PY_BIN=%VENV_DIR%\Scripts\python.exe"
set "PYI_BIN=%VENV_DIR%\Scripts\pyinstaller.exe"
set "RELEASE_DIR=release\%APP_NAME%-Windows"
set "RELEASE_ZIP=release\%APP_NAME%-Windows.zip"
set "SOURCE_DIST=dist\%APP_NAME%"

echo ===================================================
echo Rechnungsverwaltung - Windows Build + USB Paket
echo ===================================================
echo.

if not exist "%VENV_DIR%\Scripts\python.exe" (
  echo Erstelle Build-Umgebung %VENV_DIR% ...
  where py >nul 2>nul
  if %ERRORLEVEL% EQU 0 (
    py -3 -m venv "%VENV_DIR%"
  ) else (
    python -m venv "%VENV_DIR%"
  )
  if %ERRORLEVEL% NEQ 0 (
    echo FEHLER: Konnte keine Python-Umgebung erstellen.
    pause
    exit /b 1
  )
)

echo Installiere/aktualisiere Build-Abhaengigkeiten...
"%PY_BIN%" -m pip install --upgrade pip setuptools wheel
if %ERRORLEVEL% NEQ 0 goto :fail
"%PY_BIN%" -m pip install -r requirements.txt pyinstaller
if %ERRORLEVEL% NEQ 0 goto :fail

echo.
echo Erstelle die ausfuehrbare %APP_NAME%.exe...
"%PYI_BIN%" --noconfirm --clean --onedir --windowed --name "%APP_NAME%" ^
  --add-data "templates;templates/" ^
  --add-data "static;static/" ^
  --add-data "schema;schema/" ^
  --add-data "parameters.json;." ^
  run_desktop.py
if %ERRORLEVEL% NEQ 0 goto :fail

if not exist "%SOURCE_DIST%\%APP_NAME%.exe" (
  echo FEHLER: Build fertig, aber %SOURCE_DIST%\%APP_NAME%.exe nicht gefunden.
  pause
  exit /b 1
)

echo.
echo Erzeuge USB-Release-Ordner...
if exist "%RELEASE_DIR%" rmdir /s /q "%RELEASE_DIR%"
mkdir "%RELEASE_DIR%"
if %ERRORLEVEL% NEQ 0 goto :fail

robocopy "%SOURCE_DIST%" "%RELEASE_DIR%\%APP_NAME%" /E /R:1 /W:1 >nul
if !ERRORLEVEL! GEQ 8 (
  echo FEHLER: Konnte App-Dateien nicht ins Release kopieren.
  pause
  exit /b 1
)

copy /Y "windows\update_from_usb.bat" "%RELEASE_DIR%\update_from_usb.bat" >nul
if %ERRORLEVEL% NEQ 0 goto :fail
copy /Y "windows\ANLEITUNG_UPDATE.txt" "%RELEASE_DIR%\ANLEITUNG_UPDATE.txt" >nul
if %ERRORLEVEL% NEQ 0 goto :fail

echo Erzeuge ZIP-Datei...
powershell -NoProfile -Command "if (Test-Path '%RELEASE_ZIP%') { Remove-Item '%RELEASE_ZIP%' -Force }; Compress-Archive -Path '%RELEASE_DIR%\*' -DestinationPath '%RELEASE_ZIP%'"
if %ERRORLEVEL% NEQ 0 (
  echo WARNUNG: ZIP konnte nicht erstellt werden. Ordner ist trotzdem fertig.
)

echo.
echo ===================================================
echo FERTIG!
echo 1) USB-Ordner: %RELEASE_DIR%
echo 2) Optional ZIP: %RELEASE_ZIP%
echo.
echo Auf dem Geschaefts-Laptop:
echo - update_from_usb.bat doppelklicken
echo - Daten werden automatisch gesichert
echo - App wird aktualisiert und gestartet
echo ===================================================
pause
exit /b 0

:fail
echo.
echo FEHLER beim Windows-Build.
pause
exit /b 1
