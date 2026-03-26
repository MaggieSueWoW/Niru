# Agent Guidelines for Pebble

This document is for Codex agents contributing to the project. It captures coding style, libraries, and best practices.

## Language & Tools

- **Python** (3.13+ preferred)
- **NumPy** for numerical operations
- **Pandas** for tabular data management
- **Redis** for lightweight queues and caching -- always configured and available locally.
- **MongoDB** for long-term data storage -- always configured and available locally.

## Services

- **raider.io)** for data about mythic plus dungeon runs
- **Google Sheets**

## Coding Conventions

- Follow PEP 8 with **Black** auto-formatting and **isort** for imports.
- Prefer type annotations and **mypy** checks.
- Write **docstrings** using **NumPy style** (preferred in scientific Python).
- Configuration via YAML/JSON; secrets in `.env` with `python-dotenv`.

## Design Philosophy

- KISS -- Keep it super simple.
- YANGI -- You aren't gonna need it.
- Strive for simplicity in design and coding.
- Do not add test-only paths to the main codebase unless absolutely necessary and mark them clearly. Modify the tests
  to adapt to the main codebase whenever possible.
- Minimize reads/writes to external services like Google Sheets and raider.io. Use Redis for caching when
  appropriate.
- This is a V1 project: do not worry about backwards compatibility with prior versions. Assume all components are
  in-sync, and all data stores start fresh.

## Repo-Specific Product Rules

- Track **current-season Mythic+ data only** unless the user explicitly changes the product scope.
- Treat Raider.IO public API data as **best effort**. Do not add scraping without explicit user approval.
- The Google Sheet contract for V1 is:
  - tab name `raw_data`
  - roster entries in column `A`, starting at `A2`
  - roster format `region/realm/name`
  - summary output starts at `C1`
- MongoDB is the source of truth for players, runs, and sync-cycle metadata.
- Prefer storing raw Raider.IO payloads alongside normalized fields so future reporting can reuse the data.
- Use `keystone_run_id` as the unique run key and avoid duplicate inserts.
- Logging matters for this repo. Favor clear operational logs around cadence, retries, roster errors, new runs, and sheet writes.
- Redis is optional and should stay out of V1 unless there is a concrete need.

## Implementation Notes

- Prefer standard-library HTTP unless a third-party dependency materially simplifies the code.
- Keep external service usage conservative:
  - batch or cache where reasonable
  - avoid unnecessary Raider.IO detail fetches
  - do not rewrite roster columns in Google Sheets
- When tests can be written against pure functions or fakes, prefer that over adding heavy integration-test scaffolding.
