# Niru

`Niru` is a small Python service that watches a Google Sheet roster, pulls current-season Mythic+ run data from [Raider.IO](https://raider.io), stores normalized run data in MongoDB, uses Redis for restart-safe rate-limit control state, and rewrites a raw summary table on a bucketed base cadence with optional bucketed hot-player polling.

The project is named for [Niru Datagear](https://warcraft.wiki.gg/wiki/Niru_Datagear), the mechagnome tinkerer from Rustbolt.

This bot is intentionally conservative:

- It uses Raider.IO's public API only.
- It stores every run it can positively identify by `keystone_run_id`.
- It does not scrape.

## What It Does

- Reads roster entries from the `raw_data` tab, column `A`, starting at `A2`
- Expects roster cells in `region/realm/name` format, for example `us/area-52/Mythics`
- Syncs current-season Raider.IO Mythic+ profile data for each valid player
- Uses bucketed base polling plus predictive hot polling to decide which players to refresh from Raider.IO
- Stores player state, normalized runs, and sync cycle metadata in MongoDB
- Caches current-season dungeon metadata, including Raider.IO short names, in MongoDB
- Persists Raider.IO cooldown and rolling rate-limit state in Redis so restarts do not reset protections
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

Raider.IO's public character endpoints expose recent runs plus season scoring views such as best and alternate runs. That is enough to discover many runs and keep current summaries fresh, but Raider.IO does not guarantee complete historical coverage through these public endpoints alone.

## Configuration

Copy `.env.example` to `.env` and fill in the secrets:

```bash
cp .env.example .env
```

Edit [config.yaml](config.yaml) for non-secret settings.

### `.env`

- `MONGODB_URI`
- `REDIS_URL`
- `GOOGLE_SHEET_ID`
- `GOOGLE_SERVICE_ACCOUNT_FILE` or `GOOGLE_SERVICE_ACCOUNT_JSON`
- `RAIDERIO_ACCESS_KEY` optional

### `config.yaml`

- `google.raw_tab_name`
- `google.roster_column`
- `google.roster_start_row`
- `google.output_start_cell`
- `sync.interval_minutes`
- `sync.active_interval_minutes`
- `sync.active_idle_minutes`
- `sync.predictive_hot_enabled`
- `sync.predictive_hot_threshold`
- `sync.current_season`
- `sync.max_players_per_cycle`
- `sync.failure_backoff_seconds`
- `sync.max_failure_backoff_seconds`
- `raiderio.base_url`
- `raiderio.requests_per_minute_cap`
- `raiderio.circuit_breaker_threshold`
- `raiderio.circuit_breaker_cooldown_seconds`
- `redis.key_prefix`
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

The service now expects Redis to be reachable via `REDIS_URL` and uses it for restart-safe request throttling and Raider.IO cooldown state.

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

Seed predictive play profiles from stored runs:

```bash
niru-seed-play-profile
niru-seed-play-profile --player us/area-52/Mythics
niru-seed-play-profile --dry-run
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

`./scripts/docker.sh loop` now starts the container detached by default and uses Docker's `on-failure:5` restart policy. The app handles ordinary retry/backoff internally, and Docker only steps in if the process actually dies.

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
- `./scripts/docker.sh loop --restart on-failure:5`
- `./scripts/docker.sh once -- --add-host host.docker.internal:host-gateway`
- `./scripts/docker.sh once --service-account-json`

Notes:

- If MongoDB is running on your laptop, `localhost` from inside the container will not reach it. Use a host that the container can resolve, such as `host.docker.internal` on Docker Desktop.
- If Redis is running on your laptop, the same networking rule applies to `REDIS_URL`.
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
- bucketed base and hot polling outcomes
- predictive hot-poll queueing
- invalid roster rows
- Raider.IO SSL and network failures
- Raider.IO retries
- Raider.IO cooldown activation and cached-data fallback

## Repo Docs

- Public setup and usage: [README.md](README.md)
- Product and implementation detail: [SPEC.md](SPEC.md)
- Repo-specific agent guidance: [AGENTS.md](AGENTS.md)
