from datetime import datetime
import unittest

from niru.clients.sheets import (
    _build_metadata_rows,
    _build_sheet_values,
    _build_last_updated_formula,
    _find_timestamp_column,
    _normalize_sheet_row,
)


class GoogleSheetsHelpersTests(unittest.TestCase):
    def test_finds_last_successful_sync_column(self) -> None:
        header = [
            "region",
            "realm",
            "name",
            "current_total_mythic_plus_rating",
            "last_successful_sync_time_pacific",
        ]

        self.assertEqual(_find_timestamp_column(header=header, start_column="C"), "G")

    def test_normalizes_datetime_for_user_entered_sheet_write(self) -> None:
        self.assertEqual(
            _normalize_sheet_row(
                [datetime(2026, 3, 26, 5, 0), 123.4],
                include_metadata_columns=True,
            ),
            ["2026-03-26 05:00:00", 123.4, "", ""],
        )

    def test_builds_last_updated_formula(self) -> None:
        self.assertEqual(
            _build_last_updated_formula("G"),
            '=IFERROR(MAX(G2:G), "")',
        )

    def test_builds_metadata_rows_with_last_updated_and_extra_rows(self) -> None:
        self.assertEqual(
            _build_metadata_rows(
                timestamp_column="G",
                extra_metadata_rows=[("unique_runs", 5)],
            ),
            [
                ("last_updated_pacific", '=IFERROR(MAX(G2:G), "")'),
                ("unique_runs", 5),
            ],
        )

    def test_builds_sheet_values_with_multi_row_metadata(self) -> None:
        self.assertEqual(
            _build_sheet_values(
                header=["a", "b"],
                rows=[[1, 2], [3, 4]],
                metadata_rows=[
                    ("last_updated_pacific", '=IFERROR(MAX(G2:G), "")'),
                    ("unique_runs", 5),
                ],
            ),
            [
                ["a", "b", "last_updated_pacific", '=IFERROR(MAX(G2:G), "")'],
                [1, 2, "unique_runs", 5],
                [3, 4, "", ""],
            ],
        )


if __name__ == "__main__":
    unittest.main()
