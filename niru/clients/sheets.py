"""Google Sheets client."""

from __future__ import annotations

import json
from typing import Any
from datetime import datetime

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

    def write_output_rows(
        self,
        header: list[str],
        rows: list[list[object]],
        metadata_rows: list[tuple[object, object]] | None = None,
    ) -> int:
        """Rewrite the output table section in the configured tab."""

        start_cell = self._settings.output_start_cell
        tab_name = self._settings.raw_tab_name
        start_column = "".join(ch for ch in start_cell if ch.isalpha())
        summary_end_column = _column_name(_column_number(start_column) + len(header) - 1)
        metadata_label_column = _column_name(_column_number(summary_end_column) + 1)
        metadata_value_column = _column_name(_column_number(summary_end_column) + 2)
        (
            self._service.spreadsheets()
            .values()
            .clear(
                spreadsheetId=self._settings.sheet_id,
                range=f"{tab_name}!{start_column}:{metadata_value_column}",
                body={},
            )
            .execute()
        )

        timestamp_column = _find_timestamp_column(header=header, start_column=start_column)
        metadata = _build_metadata_rows(
            timestamp_column=timestamp_column,
            extra_metadata_rows=metadata_rows,
        )
        values = _build_sheet_values(header=header, rows=rows, metadata_rows=metadata)
        (
            self._service.spreadsheets()
            .values()
            .update(
                spreadsheetId=self._settings.sheet_id,
                range=f"{tab_name}!{start_cell}",
                valueInputOption="USER_ENTERED",
                body={"values": values},
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


def _normalize_sheet_row(
    row: list[object], *, include_metadata_columns: bool = False
) -> list[object]:
    normalized = [_normalize_sheet_value(value) for value in row]
    if include_metadata_columns:
        normalized.extend(["", ""])
    return normalized


def _normalize_sheet_value(value: object) -> object:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value


def _timestamp_column_index(header: list[str]) -> int:
    return header.index("last_successful_sync_time_pacific")


def _find_timestamp_column(*, header: list[str], start_column: str) -> str | None:
    if "last_successful_sync_time_pacific" not in header:
        return None
    return _column_name(_column_number(start_column) + _timestamp_column_index(header))


def _build_last_updated_formula(timestamp_column: str) -> str:
    return f'=IFERROR(MAX({timestamp_column}2:{timestamp_column}), "")'


def _build_metadata_rows(
    *,
    timestamp_column: str | None,
    extra_metadata_rows: list[tuple[object, object]] | None = None,
) -> list[tuple[object, object]]:
    metadata_rows: list[tuple[object, object]] = []
    if timestamp_column is not None:
        metadata_rows.append(
            ("last_updated_pacific", _build_last_updated_formula(timestamp_column))
        )
    if extra_metadata_rows:
        metadata_rows.extend(extra_metadata_rows)
    return metadata_rows


def _build_sheet_values(
    *,
    header: list[str],
    rows: list[list[object]],
    metadata_rows: list[tuple[object, object]],
) -> list[list[object]]:
    include_metadata_columns = bool(metadata_rows)
    values = [
        _normalize_sheet_row(list(header), include_metadata_columns=include_metadata_columns)
    ]
    values.extend(
        _normalize_sheet_row(row, include_metadata_columns=include_metadata_columns)
        for row in rows
    )
    for row_index, (label, value) in enumerate(metadata_rows):
        while len(values) <= row_index:
            values.append([""] * len(header))
            if include_metadata_columns:
                values[-1].extend(["", ""])
        values[row_index][-2:] = [
            _normalize_sheet_value(label),
            _normalize_sheet_value(value),
        ]
    return values
