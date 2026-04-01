@echo off
setlocal EnableExtensions EnableDelayedExpansion

set "APP_NAME=Rechnungsverwaltung"
set "SCRIPT_DIR=%~dp0"
set "SOURCE_APP_DIR=%SCRIPT_DIR%%APP_NAME%"
set "SOURCE_EXE=%SOURCE_APP_DIR%\%APP_NAME%.exe"

set "TARGET_ROOT=%LOCALAPPDATA%\%APP_NAME%"
set "TARGET_APP_DIR=%TARGET_ROOT%\%APP_NAME%"
set "TARGET_EXE=%TARGET_APP_DIR%\%APP_NAME%.exe"

set "DATA_DIR=%USERPROFILE%\Documents\Rechnungsverwaltung_Daten"
set "BACKUP_ROOT=%USERPROFILE%\Documents\Rechnungsverwaltung_Backups"

echo ===================================================
echo %APP_NAME% - Update vom USB-Stick
echo ===================================================
echo.

if not exist "%SOURCE_EXE%" (
  echo FEHLER:
  echo Die Datei "%SOURCE_EXE%" wurde nicht gefunden.
  echo Bitte pruefe, ob der Ordner "%APP_NAME%" neben diesem Skript liegt.
  pause
  exit /b 1
)

for /f %%I in ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd_HHmmss"') do set "TS=%%I"
set "BACKUP_DIR=%BACKUP_ROOT%\%TS%"

echo [1/4] Beende laufende App (falls offen)...
taskkill /IM "%APP_NAME%.exe" /F >nul 2>nul

echo [2/4] Erstelle Datensicherung...
if exist "%DATA_DIR%" (
  if not exist "%BACKUP_DIR%" mkdir "%BACKUP_DIR%"
  robocopy "%DATA_DIR%" "%BACKUP_DIR%" /E /R:1 /W:1 >nul
  if !ERRORLEVEL! GEQ 8 (
    echo FEHLER: Backup konnte nicht erstellt werden.
    pause
    exit /b 1
  )
  echo Backup gespeichert unter:
  echo %BACKUP_DIR%
) else (
  echo Kein Datenordner gefunden. Vermutlich Erstinstallation.
)

echo [3/4] Aktualisiere App-Dateien...
if not exist "%TARGET_ROOT%" mkdir "%TARGET_ROOT%"
if exist "%TARGET_APP_DIR%" rmdir /S /Q "%TARGET_APP_DIR%"

robocopy "%SOURCE_APP_DIR%" "%TARGET_APP_DIR%" /E /R:1 /W:1 >nul
if !ERRORLEVEL! GEQ 8 (
  echo FEHLER: App-Dateien konnten nicht kopiert werden.
  pause
  exit /b 1
)

if not exist "%TARGET_EXE%" (
  echo FEHLER: "%TARGET_EXE%" wurde nach dem Kopieren nicht gefunden.
  pause
  exit /b 1
)

echo [4/4] Starte App...
start "" "%TARGET_EXE%"

echo.
echo ===================================================
echo Update abgeschlossen.
echo Wenn die App nicht startet, bitte ANLEITUNG_UPDATE.txt lesen.
echo ===================================================
pause
exit /b 0
