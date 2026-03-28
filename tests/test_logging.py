import logging
import unittest

from niru.logging_utils import ExtraFieldsFormatter


class LoggingFormatterTests(unittest.TestCase):
    def test_formatter_appends_extra_fields(self) -> None:
        formatter = ExtraFieldsFormatter("%(levelname)s %(name)s %(message)s")
        record = logging.makeLogRecord(
            {
                "name": "niru.test",
                "levelno": logging.INFO,
                "levelname": "INFO",
                "msg": "hello",
                "row_number": 12,
                "player_key": "us/proudmoore/maggiesue",
            }
        )

        rendered = formatter.format(record)

        self.assertEqual(
            rendered,
            "INFO niru.test hello player_key=us/proudmoore/maggiesue row_number=12",
        )


if __name__ == "__main__":
    unittest.main()
