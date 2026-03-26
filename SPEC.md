# Niru V1

## Goal

Build Niru, a small service that tracks a roster of WoW Mythic+ characters, stores Raider.IO run data in MongoDB, and publishes summary output to Google Sheets on a 15-minute cadence.

## Scope

V1 is current-season only and best effort.

- Use Raider.IO public API endpoints only
- No scraping
- Google Sheets is the user-facing surface
- MongoDB is the system of record
- Redis is intentionally not used in V1
- Season rollover is manual for now via config update

## Roster Contract

- Sheet tab: `raw_data`
- Input range: column `A`, starting at `A2`
- Cell format: `region/realm/name`
- Region must be one of `us`, `eu`, `tw`, `kr`, `cn`
- Realm is normalized to Raider.IO slug format
- Duplicate roster rows are ignored after the first valid instance and surfaced as `invalid_player`

## Data Flow

### 1. Roster Sync

- Read the roster column from Google Sheets
- Parse and validate each row
- Upsert active roster rows into MongoDB
- Mark rows missing from the latest sheet snapshot as inactive

### 2. Player Sync

For each valid active player:

- Fetch Raider.IO character profile data with:
  - `mythic_plus_scores_by_season:current`
  - `mythic_plus_recent_runs`
  - `mythic_plus_best_runs:all`
  - `mythic_plus_alternate_runs:all`
- Derive current per-dungeon score from the best and alternate scoring runs returned by Raider.IO
- Collect run IDs from recent, best, and alternate sets
- Insert unseen run stubs into MongoDB

Important limitation:

- Raider.IO's public character profile fields expose recent runs plus scoring-oriented season views such as best and alternate runs.
- V1 therefore stores every run it can positively discover, but it does not guarantee a complete season history for every player from public API data alone.
- If polling windows are missed or Raider.IO data is unavailable, the bot should continue publishing best-effort summaries while marking affected players as potentially incomplete.

### 3. Sheet Publish

- Query active roster players and relevant runs from MongoDB
- Build one summary row per `player + dungeon`
- Fully clear columns `C:O` in `raw_data`
- Rewrite the output table starting at `C1`

## MongoDB Collections

### `players`

- canonical identity and current roster metadata
- validity and sync status
- last successful sync timestamp
- current per-dungeon score map
- gap flags and last error

### `runs`

- one document per `keystone_run_id`
- deduplicated by unique index
- season, dungeon, score, timings, affixes, and other normalized summary fields available from the character-profile run lists
- discovered roster player keys for summary joins

### `sync_cycles`

- start and finish timestamps
- API call counts
- new run counts
- sheet row counts
- warnings and partial-cycle marker

## Summary Semantics

- `current_score`: current Raider.IO dungeon score derived from best and alternate scoring runs in the latest profile payload
- `best_key_level`: highest-value stored run chosen by score, then key level, then completion time
- `best_completed_in_time`: `TRUE` when the best run had one or more keystone upgrades
- `best_upgrade_level`: the best run's `num_keystone_upgrades`
- `total_runs`: count of stored runs for that player and dungeon
- `last_run_at`: latest known completion timestamp for that player and dungeon

If the player is invalid, has sync failures, or may have a data gap, the row remains visible with a non-`ok` status.

## Gap Handling

The bot flags `partial_gap` when:

- a player misses more than the configured sync-window threshold
- Raider.IO data retrieval fails after retries

The public API does not expose a guaranteed complete player-history feed, so gap flags are advisory and may require manual review.

Known recovery requirement:

- If V2 reintroduces selective `run-details` fetching, a run that is known by `keystone_run_id` but missing desired detail fields should be eligible for a later detail retry instead of being treated as fully complete.

## External Integrations

### Raider.IO

- Base URL configurable in YAML
- Unauthenticated mode supported by default
- Conservative client-side rate limit defaults to 60 requests/minute
- Retries for `429` and transient `5xx` errors

### Google Sheets

- Service account auth in V1
- Roster read from the same tab used for summary output
- Output rewrite intentionally avoids touching roster column `A`

## Runtime

- Runs as a long-lived process in Docker
- Executes one sync immediately on startup
- Sleeps until the next configured interval
- Supports CLI modes for one-shot and looping execution
- Handles `SIGINT` and `SIGTERM` gracefully
- Logs to stdout with standard Python logging

## V2 Candidates

### Current Week Metrics

Add weekly summary metrics based on the active game week for the player's region.

Planned outputs:

- `weekly_run_count`: number of Mythic+ dungeons completed in the current game week
- `weekly_10_plus_run_count`: number of Mythic+ dungeons completed at level 10 or above in the current game week

Notes:

- Use Raider.IO period data to define the current game week rather than hardcoding dates.
- Add a config option for the weekly-region reference and set it to `us` in the initial V2 rollout.
- For the initial V2 rollout, the intended behavior is US Tuesday-to-Tuesday weekly windows.
- Compute these metrics from stored MongoDB run data using `completed_at`, not by adding new per-player API fetches during summary generation.
- Persist the resolved weekly window in sync metadata so reset-boundary behavior is easy to debug.
- Future improvement: support region-specific weekly windows derived from each rostered player's own region instead of one configured region for the whole project.
- Raider.IO `run-details` is not needed for these weekly counts; the normalized run fields already stored in V1 are sufficient.

### Weekly Gilded Crest Tracking

Investigate adding the number of gilded crests earned by a player during the current game week.

Current understanding:

- Raider.IO's public API does not appear to expose gilded crest earnings directly.
- This likely requires Blizzard API data, or a derived calculation from official reward rules plus stored run data.

Open questions for implementation:

- Whether Blizzard exposes current crest balance only, or also weekly earned crest totals
- Whether weekly gilded crests can be derived accurately enough from stored run data and season rules
- Whether this should live in the existing bot or in a companion bot that reads the roster from MongoDB and writes additional summary inputs

Preferred direction:

- Keep weekly run-count metrics inside Niru.
- Treat crest tracking as a separate subsystem first, because it may require different Blizzard authentication, data collection, and persistence behavior.

### Optional Future Use Of Raider.IO Run Details

V1 intentionally does not fetch Raider.IO `run-details`, because the endpoint is large and not required for current summary outputs.

Possible future reasons to reintroduce selective `run-details` fetches:

- storing who a player ran with
- capturing run roster/class/spec context for later analysis
- storing extra timing or chest-detail fields that are not required for V1 or the current V2 metrics

If this is added later, prefer an on-demand or opt-in sync path rather than fetching full run-detail payloads for every new run by default.
