# Niru

`Niru` is a small Python service that watches a Google Sheet roster, pulls current-season Mythic+ run data from [Raider.IO](https://raider.io), stores normalized run data in MongoDB, and rewrites a raw summary table back into the sheet every 15 minutes.

The project is named for [Niru Datagear](https://warcraft.wiki.gg/wiki/Niru_Datagear), the mechagnome tinkerer from Rustbolt.

This bot is intentionally conservative:

- It uses Raider.IO's public API only.
- It stores every run it can positively identify by `keystone_run_id`.
- It does not scrape.
- It marks potential data gaps instead of pretending history is complete.

## What It Does

- Reads roster entries from the `raw_data` tab, column `A`, starting at `A2`
- Expects roster cells in `region/realm/name` format, for example `us/area-52/Mythics`
- Syncs current-season Raider.IO Mythic+ profile data for each valid player
- Stores player state, normalized runs, and sync cycle metadata in MongoDB
- Caches current-season dungeon metadata, including Raider.IO short names, in MongoDB
- Rewrites a summary table starting at `raw_data!C1`

## Output Columns

The summary table now contains one row per player.

Fixed player columns:

- `region`
- `realm`
- `name`
- `current_total_mythic_plus_rating`
- `last_successful_sync_time_pacific`

For each current-season dungeon, the bot adds four columns using the Raider.IO dungeon short name:

- `{short_name}_current_score`
- `{short_name}_best_key_level`
- `{short_name}_best_upgrade_level`
- `{short_name}_total_runs`

## Important Limitation

Raider.IO's public character endpoints expose recent runs plus season scoring views such as best and alternate runs. That is enough to discover many runs and keep current summaries fresh, but Raider.IO does not guarantee complete historical coverage through these public endpoints alone. When the bot detects missed polling windows or service problems, it records that coverage may be incomplete.

## Configuration

Copy `.env.example` to `.env` and fill in the secrets:

```bash
cp .env.example .env
```

Edit [config.yaml](config.yaml) for non-secret settings.

### `.env`

- `MONGODB_URI`
- `GOOGLE_SHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_FILE` or `GOOGLE_SERVICE_ACCOUNT_JSON`
- `RAIDERIO_ACCESS_KEY` optional

### `config.yaml`

- `google.raw_tab_name`
- `google.roster_column`
- `google.roster_start_row`
- `google.output_start_cell`
- `sync.interval_minutes`
- `sync.current_season`
- `sync.max_players_per_cycle`
- `raiderio.base_url`
- `raiderio.requests_per_minute_cap`
- `mongodb.database`
- `logging.level`

## Google Sheets Setup

1. Create or choose a Google Sheet.
2. Add a tab named `raw_data`.
3. Put roster entries in column `A`, starting at `A2`.
4. Share the sheet with the Google service account email.
5. Leave columns `C` onward available for bot output.

## Local Run

Install dependencies:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .
```

Start the bot:

```bash
python main.py
```

Run a single sync cycle:

```bash
python main.py --mode once
```

Run continuously:

```bash
python main.py --mode loop
```

Use a custom config path:

```bash
python main.py --config /path/to/config.yaml --mode once
```

## Docker

Build and run directly:

```bash
docker build -t niru .
docker run --rm --env-file .env -v "$(pwd)/config.yaml:/app/config.yaml:ro" niru
```

Or use the helper script:

```bash
./scripts/docker.sh once
./scripts/docker.sh loop
```

`./scripts/docker.sh loop` now starts the container detached by default and uses Docker's `unless-stopped` restart policy so it stays up across failures and restarts until you stop it explicitly.

The helper script does three important things for local runs:

- builds the image first
- mounts `config.yaml` into `/app/config.yaml`
- remaps `service-account.json` into the container and overrides `GOOGLE_SERVICE_ACCOUNT_FILE`

When the active Docker context is remote, the helper script switches behavior automatically:

- it uses the `config.yaml` baked into the image instead of bind-mounting your local file
- it reads your local `service-account.json` and sends it as `GOOGLE_SERVICE_ACCOUNT_JSON`
- it still passes your local `.env` with `--env-file`

Useful flags:

- `./scripts/docker.sh build`
- `./scripts/docker.sh once --no-build`
- `./scripts/docker.sh loop --detach`
- `./scripts/docker.sh loop --attach`
- `./scripts/docker.sh loop --restart on-failure`
- `./scripts/docker.sh once -- --add-host host.docker.internal:host-gateway`
- `./scripts/docker.sh once --service-account-json`

Notes:

- If MongoDB is running on your laptop, `localhost` from inside the container will not reach it. Use a host that the container can resolve, such as `host.docker.internal` on Docker Desktop.
- If you switch Docker context to a remote daemon such as Synology, bind mounts refer to paths on that remote host, not your laptop. The helper script avoids that by default, but config changes now require a rebuild because the remote container uses the config baked into the image.

## Testing

The included unit tests cover roster parsing, summary generation, and incremental sync behavior:

```bash
PYTHONPATH=. python -m unittest discover -s tests
```

## Logging

Logs are written to stdout and include:

- sync cycle start and finish
- new run discovery
- invalid roster rows
- Raider.IO SSL and network failures
- Raider.IO retries
- potential gap flags

## Repo Docs

- Public setup and usage: [README.md](README.md)
- Product and implementation detail: [SPEC.md](SPEC.md)
- Repo-specific agent guidance: [AGENTS.md](AGENTS.md)
