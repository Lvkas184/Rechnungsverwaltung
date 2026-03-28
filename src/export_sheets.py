"""Export all sheets from a Google Spreadsheet to data/*.json."""

import json
import os

from google.oauth2 import service_account
from googleapiclient.discovery import build

SPREADSHEET_ID = "1JR-BqjB6ynx3qQzRfiIVb4opR5u69mL5GsH-OywyBXA"
SERVICE_ACCOUNT_FILE = "service_account.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def col_letter(n: int) -> str:
    res = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        res = chr(65 + r) + res
    return res


def export_all() -> None:
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError(f"Service account file not found: {SERVICE_ACCOUNT_FILE}")

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES,
    )
    service = build("sheets", "v4", credentials=creds)
    meta = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    sheets = meta.get("sheets", [])

    os.makedirs("data", exist_ok=True)
    for s in sheets:
        title = s["properties"]["title"]
        row_count = s["properties"].get("gridProperties", {}).get("rowCount", 1000)
        col_count = s["properties"].get("gridProperties", {}).get("columnCount", 26)
        range_a1 = f"'{title}'!A1:{col_letter(col_count)}{row_count}"

        print(f"Exporting: {title} -> data/{title.replace(' ', '_')}.json ({range_a1})")
        resp = (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=SPREADSHEET_ID,
                range=range_a1,
                valueRenderOption="UNFORMATTED_VALUE",
            )
            .execute()
        )

        out_path = os.path.join("data", f"{title.replace(' ', '_')}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump({"title": title, "values": resp.get("values", [])}, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    export_all()
    print("Export abgeschlossen. JSON-Dateien liegen im data/-Verzeichnis.")
