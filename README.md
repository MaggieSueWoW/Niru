# Niru

Niru is a season-aware World of Warcraft Mythic+ roster service. It reads a roster
from Google Sheets, combines official Blizzard profile data with
[Raider.IO](https://raider.io) data, stores player and run state in MongoDB, and
publishes current-season summaries back to the roster's sheet tab.

Each configured season has its own roster and output tab. Niru resolves the active
season from a UTC activation schedule, switches tabs at the configured cutoff without
a redeploy, and stops writing to prior-season tabs. A future tab can be populated in
advance with its roster, headers, and zero-state values.

The project is named for [Niru Datagear](https://warcraft.wiki.gg/wiki/Niru_Datagear), the mechagnome tinkerer from Rustbolt.

## How Niru Works

1. Resolve the active season and its Google Sheets tab from `google.season_tabs`.
2. Read that tab's roster from column `A`, beginning at `A2`.
3. Fetch player profiles from Blizzard and Raider.IO using the configured season
   identifiers for players whose base or hot polling window is due.
4. Merge summary scores field by field, normalize newly discovered runs, and persist
   the results in MongoDB.
5. Rebuild the active-season player summary from MongoDB and incrementally update the
   output beginning at `C1` without touching the roster column.
6. Publish the configured-window team-activity heatmap beginning at `C101` when
   enabled.

Niru runs continuously by default. Ordinary roster players use the configured base
cadence. Players who recently completed runs, or whose stored play profile predicts
activity, can be polled on the faster hot cadence. Redis preserves Raider.IO request
windows and cooldown state across restarts.

## Data-Source Rules

Niru uses both sources deliberately; it does not treat either payload as a wholesale
replacement for the other.

- Blizzard supplies the preferred total Mythic+ rating when it is present.
- An explicit Blizzard per-dungeon `map_rating` is preferred for that dungeon.
- Raider.IO fills only per-dungeon scores that Blizzard omitted.
- Conflicting values are never averaged or added across sources.
- Both sources are filtered to the configured active-season dungeon catalog before
  their scores or runs are accepted.
- Mixed score provenance is recorded as `blizzard+raiderio` in MongoDB.
- Run observations from the two sources are normalized and matched separately from
  the player-score merge. Raider.IO `keystone_run_id` values remain the stable run
  identifiers when available.

The service uses Blizzard's configured numeric season ID and Raider.IO's configured
season slug rather than asking either provider for an implicit “current” season. A run
must also be completed on or after the configured season activation and belong to the
season's dungeon catalog. These checks prevent stale transition-day responses from
being stored under the new season.

Niru is intentionally conservative: it uses supported Blizzard and Raider.IO APIs,
does not scrape, limits and retries upstream requests, and publishes cached MongoDB
state when Raider.IO is in cooldown.

## Google Sheets Output

Each season tab combines input and output:

- Column `A`, starting at `A2`: roster input in `region/realm/name` format.
- `C1` onward: one summary row per rostered player.
- `C101` onward: team-activity heatmap when `team_activity.enabled` is true.
- Columns `A` and `B` are never rewritten by Niru.

The summary's fixed player columns are:

- `region`
- `realm`
- `name`
- `current_total_mythic_plus_rating`
- `last_successful_sync_time_pacific`
- `weekly_10_plus_run_count`

For each current-season dungeon, Niru adds four columns using the configured Raider.IO
short name:

- `{short_name}_current_score`
- `{short_name}_best_key_level`
- `{short_name}_best_upgrade_level`
- `{short_name}_total_runs`

Valid players with no current-season rating or run data are published with numeric
zeroes. Missing upstream data caused by an actual sync failure remains blank so that a
failure is not disguised as a real zero.

The weekly 10+ count uses the current reset window for each player's region and stored
run completion times. It remains blank when Niru cannot determine that region's weekly
window. The activity table reports the average number of unique rostered players seen
per Pacific hour and weekday across `team_activity.window_weeks`.

## Important Limitations

Raider.IO's public character endpoints expose recent runs plus scoring-oriented best
and alternate views. Blizzard character profiles provide another view of season and
weekly runs. Together they discover many runs and keep summaries fresh, but neither
source guarantees a complete historical ledger through these endpoints. Niru stores
every run it can positively identify and treats `total_runs` and the activity heatmap
as best-effort views of the runs it has observed.

## Stored State

MongoDB is the source of truth for published data:

- `players` holds canonical identities, current score state, sync status, and polling
  metadata.
- `season_rosters` preserves membership and row order separately for each season.
- `runs` stores normalized observations and source payloads, with Raider.IO run IDs
  unique within a season rather than globally.
- `season_dungeons` caches the dungeon catalog and output abbreviations for each
  season.
- `weekly_periods` caches region-specific reset windows used for weekly counts.
- `sync_cycles` records operational counts, warnings, partial status, resolved season,
  and destination tab.

Redis contains only ephemeral Raider.IO throttling and cooldown state. It is not a
business-data store and can be rebuilt.

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
- `BLIZZARD_CLIENT_ID` and `BLIZZARD_CLIENT_SECRET` when `blizzard.enabled` is
  true

### `config.yaml`

- `google.season_tabs.<season_slug>.tab_name`
- `google.season_tabs.<season_slug>.activates_at`
- `google.season_tabs.<season_slug>.blizzard_season_id`
- `google.roster_column`
- `google.roster_start_row`
- `google.output_start_cell`
- `team_activity.enabled`
- `team_activity.window_weeks`
- `team_activity.start_hour`
- `team_activity.output_start_cell`
- `sync.interval_minutes`
- `sync.active_interval_minutes`
- `sync.active_idle_minutes`
- `sync.predictive_hot_enabled`
- `sync.predictive_hot_threshold`
- `sync.max_players_per_cycle`
- `sync.failure_backoff_seconds`
- `sync.max_failure_backoff_seconds`
- `raiderio.base_url`
- `raiderio.requests_per_minute_cap`
- `raiderio.circuit_breaker_threshold`
- `raiderio.circuit_breaker_cooldown_seconds`
- `blizzard.enabled`
- `blizzard.requests_per_hour_cap`
- `blizzard.requests_per_second_cap`
- `blizzard.run_fingerprint_fuzz_seconds`
- `redis.key_prefix`
- `mongodb.database`
- `mongodb.players_collection`
- `mongodb.runs_collection`
- `mongodb.sync_cycles_collection`
- `mongodb.season_rosters_collection`
- `logging.level`

## Google Sheets Setup

1. Create or choose a Google Sheet.
2. Create every tab named in `google.season_tabs`.
3. Put each season's roster in column `A` of its own tab, starting at `A2`.
4. Share the sheet with the Google service account email.
5. Leave columns `C` onward available for Niru output on each season tab.

The current config preserves Midnight S1 on `niru_raw_data` and uses `niru_raw_data_s2` for Midnight S2. Niru switches to S2 at `2026-08-18T15:00:00Z` (Tuesday at 8:00 a.m. Pacific) without a redeploy. Prior season tabs are no longer written after their configured cutoff.

Midnight S2 uses these Raider.IO abbreviations, in header order:

- `AOF` — Altar of Fangs
- `BV` — The Blinding Vale
- `DON` — Den of Nalorakk
- `KR` — Kings' Rest
- `MR` — Murder Row
- `RLP` — Ruby Life Pools
- `TOS` — Temple of Sethraliss
- `VSA` — Voidscar Arena

After creating `niru_raw_data_s2` and adding its roster in column `A`, populate its
complete zero-state output immediately with:

```bash
python main.py --prepare-season season-mn-2
```

The preparation command reads (but does not rewrite) the season roster in column `A`,
joins it to existing player records in MongoDB, and writes player fields plus zeroes for
unsynced S2 rating/run values starting at `C1`. It does not call player APIs or activate
the future roster early. Normal sync cycles also prepare headers automatically when
they see an existing future-season tab.

## Local Run

Install dependencies:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e .
```

For development and tests, install the optional development tools instead:

```bash
pip install -e '.[dev]'
```

Start the bot:

```bash
python main.py
```

The service expects MongoDB, Redis, Google credentials, and—when enabled—Blizzard API
credentials to be available before startup.

Run a single sync cycle:

```bash
python main.py --mode once
```

`--mode once` deliberately refreshes every valid roster player rather than applying
the ordinary due-time selection. Use `--player region/realm/name` with it to limit the
cycle to one active player.

Prepare a configured season tab's roster-based zero-state output without running a
player sync:

```bash
python main.py --prepare-season season-mn-2
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

`./scripts/docker.sh loop` starts the container detached by default and uses Docker's
`on-failure:5` restart policy. The app handles ordinary retry/backoff internally, and
Docker only steps in if the process actually dies.

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

The unit suite covers roster parsing, season resolution and rollover, source-specific
normalization and merging, run deduplication, summary generation, polling selection,
and future-season preparation:

```bash
python -m pytest -q
ruff check .
```

## Logging

Logs are written to stdout and include:

- sync cycle start and finish
- new run discovery
- bucketed base and hot polling outcomes
- predictive hot-poll queueing
- active season and destination sheet tab
- invalid roster rows
- Blizzard and Raider.IO API call counts
- cross-source run differences
- Raider.IO SSL and network failures
- Raider.IO retries
- Raider.IO cooldown activation and cached-data fallback

## Repo Docs

- Public setup and usage: [README.md](README.md)
- Product and implementation detail: [SPEC.md](SPEC.md)
- Repo-specific agent guidance: [AGENTS.md](AGENTS.md)
