from __future__ import annotations

from typing import List

from google.oauth2 import service_account
from googleapiclient.discovery import build

LOGIN_HISTORY_HEADERS = ["date", "username", "logged_in_at"]
SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"


def _build_sheets_service(service_account_json_path: str):
    credentials = service_account.Credentials.from_service_account_file(
        service_account_json_path,
        scopes=[SHEETS_SCOPE],
    )
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def _get_sheet_titles(service, spreadsheet_id: str) -> List[str]:
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    return [sheet["properties"]["title"] for sheet in metadata.get("sheets", [])]


def _read_values(service, spreadsheet_id: str, range_name: str):
    response = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
    )
    return response.get("values", [])


def ensure_login_history_sheet(
    service_account_json_path: str,
    spreadsheet_id: str,
    worksheet_name: str,
) -> None:
    service = _build_sheets_service(service_account_json_path)
    titles = _get_sheet_titles(service, spreadsheet_id)
    if worksheet_name not in titles:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": worksheet_name}}}]},
        ).execute()

    existing = _read_values(service, spreadsheet_id, f"{worksheet_name}!1:1")
    if not existing or existing[0] != LOGIN_HISTORY_HEADERS:
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{worksheet_name}!A1",
            valueInputOption="RAW",
            body={"values": [LOGIN_HISTORY_HEADERS]},
        ).execute()


def append_login_history_row(
    service_account_json_path: str,
    spreadsheet_id: str,
    worksheet_name: str,
    row: List[str],
) -> None:
    ensure_login_history_sheet(service_account_json_path, spreadsheet_id, worksheet_name)
    service = _build_sheets_service(service_account_json_path)
    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=f"{worksheet_name}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": [row]},
    ).execute()
