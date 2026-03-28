"""Logging setup."""

from __future__ import annotations

import logging
from typing import Any


_STANDARD_RECORD_FIELDS = set(logging.makeLogRecord({}).__dict__) | {"message", "asctime"}


class ExtraFieldsFormatter(logging.Formatter):
    """Append custom LogRecord fields added via logger `extra=`."""

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        extra_items: list[tuple[str, Any]] = []
        for key, value in sorted(record.__dict__.items()):
            if key in _STANDARD_RECORD_FIELDS:
                continue
            extra_items.append((key, value))
        if not extra_items:
            return message
        rendered = " ".join(f"{key}={value}" for key, value in extra_items)
        return f"{message} {rendered}"


def configure_logging(level: str) -> None:
    """Configure application logging."""

    handler = logging.StreamHandler()
    handler.setFormatter(
        ExtraFieldsFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    )
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        handlers=[handler],
        force=True,
    )
