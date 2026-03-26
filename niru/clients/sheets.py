"""Google Sheets client."""

from __future__ import annotations

import json
from typing import Any

from niru.config import GoogleSettings


class GoogleSheetsClient:
    """Minimal Google Sheets reader/writer."""

    def __init__(self, settings: GoogleSettings) -> None:
        self._settings = settings
        self._service = self._build_service()

    def read_roster_rows(self) -> list[str]:
        """Read the configured roster column from the raw_data tab."""

        start = self._settings.roster_start_row
        column = self._settings.roster_column
        range_name = f"{self._settings.raw_tab_name}!{column}{start}:{column}"
        response = (
            self._service.spreadsheets()
            .values()
            .get(spreadsheetId=self._settings.sheet_id, range=range_name)
            .execute()
        )
        values = response.get("values", [])
        return [row[0] if row else "" for row in values]

    def write_output_rows(self, header: list[str], rows: list[list[object]]) -> int:
        """Rewrite the output table section in the configured tab."""

        start_cell = self._settings.output_start_cell
        tab_name = self._settings.raw_tab_name
        start_column = "".join(ch for ch in start_cell if ch.isalpha())
        end_column = _column_name(_column_number(start_column) + len(header) - 1)
        clear_range = f"{tab_name}!{start_column}:{end_column}"
        (
            self._service.spreadsheets()
            .values()
            .clear(
                spreadsheetId=self._settings.sheet_id,
                range=clear_range,
                body={},
            )
            .execute()
        )

        body = {
            "values": [header, *[_normalize_sheet_row(row) for row in rows]],
        }
        (
            self._service.spreadsheets()
            .values()
            .update(
                spreadsheetId=self._settings.sheet_id,
                range=f"{tab_name}!{start_cell}",
                valueInputOption="RAW",
                body=body,
            )
            .execute()
        )
        return len(rows)

    def _build_service(self) -> Any:
        from google.oauth2.service_account import Credentials
        from googleapiclient.discovery import build

        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        if self._settings.service_account_json:
            info = json.loads(self._settings.service_account_json)
            credentials = Credentials.from_service_account_info(info, scopes=scopes)
        elif self._settings.service_account_file:
            credentials = Credentials.from_service_account_file(
                self._settings.service_account_file,
                scopes=scopes,
            )
        else:
            raise ValueError(
                "Provide GOOGLE_SERVICE_ACCOUNT_FILE or GOOGLE_SERVICE_ACCOUNT_JSON."
            )
        return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def _column_number(column_name: str) -> int:
    value = 0
    for char in column_name:
        value = value * 26 + (ord(char) - ord("A") + 1)
    return value


def _column_name(column_number: int) -> str:
    chars: list[str] = []
    current = column_number
    while current > 0:
        current, remainder = divmod(current - 1, 26)
        chars.append(chr(ord("A") + remainder))
    return "".join(reversed(chars))


def _normalize_sheet_row(row: list[object]) -> list[object]:
    return ["" if value is None else value for value in row]
