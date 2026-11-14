"""Best-effort reporting of completed worker cycles to Healthchecks.io."""

import json
import logging
import ssl
from typing import Any
from urllib.request import Request, urlopen

import certifi

LOGGER = logging.getLogger(__name__)
PING_TIMEOUT_SECONDS = 5


def report_sync_success(ping_url: str | None, cycle: dict[str, Any]) -> None:
    """Report a completed cycle without allowing monitoring to fail the worker.

    Parameters
    ----------
    ping_url : str or None
        Secret success URL. An unset URL disables reporting.
    cycle : dict
        Persisted summary of a completed, non-interrupted sync cycle.
    """
    if not ping_url:
        return

    try:
        # Send only operational counts, never roster identities or warning text.
        summary = {
            key: cycle[key]
            for key in (
                "season",
                "duration_seconds",
                "active_players",
                "invalid_players",
                "baseline_due_players_synced",
                "accelerated_players_synced",
                "new_runs",
                "partial",
            )
        }
        summary["status"] = (
            "degraded" if cycle["partial"] or cycle["invalid_players"] else "healthy"
        )
        request = Request(
            ping_url,
            data=json.dumps(summary).encode("utf-8"),
            headers={"Content-Type": "application/json", "User-Agent": "Niru/0.1"},
            method="POST",
        )
        with urlopen(
            request,
            timeout=PING_TIMEOUT_SECONDS,
            context=ssl.create_default_context(cafile=certifi.where()),
        ) as response:
            # Unknown UUIDs and rate-limited pings can also return HTTP 200.
            if response.status != 200 or response.read(128).strip() != b"OK":
                LOGGER.warning("Healthchecks did not acknowledge the success ping")
                return
        LOGGER.info(
            "Healthchecks success ping sent", extra={"cycle_status": summary["status"]}
        )
    except Exception as exc:
        # Exception messages and tracebacks can expose the secret ping URL.
        LOGGER.warning(
            "Healthchecks success ping failed; sync remains complete",
            extra={"error_type": type(exc).__name__},
        )
