# Raider.IO Internal API Backfill Spec

## Status

Draft only. This document records current learnings and a proposed design for a future backfill tool. It does not change the V1 runtime contract in [SPEC.md](../SPEC.md), which remains public-API-first.

## Why This Exists

The current bot intentionally uses Raider.IO's documented public API only. That keeps the main sync path conservative, but it also means historical coverage is incomplete because the public character profile payload only exposes:

- recent runs
- best scoring runs
- alternate scoring runs

That is enough for live summary freshness, but not enough to reliably reconstruct an entire season history after missed polling windows.

We have now identified Raider.IO website endpoints that appear to expose a fuller run history and rich run details. Those endpoints are promising for one-off or occasional backfill, but they are likely internal frontend APIs and should be treated as brittle.

## What We Learned

### Internal endpoints discovered

When expanding a dungeon group on the Raider.IO character page, the site requests:

`/api/characters/mythic-plus-runs`

Observed query shape:

- `season`
- `characterId`
- `dungeonId`
- `role`
- `specId`
- `mode`
- `affixes`
- `date`

Example:

```text
/api/characters/mythic-plus-runs?season=season-mn-1&characterId=1063439&dungeonId=15808&role=all&specId=0&mode=scored&affixes=all&date=all
```

Observed response shape:

- `runs[]`
- `runs[].summary`
- `runs[].summary.keystone_run_id`
- `runs[].summary.logged_run_id`
- `runs[].summary.completed_at`
- `runs[].summary.mythic_level`
- `runs[].summary.clear_time_ms`
- `runs[].summary.keystone_time_ms`
- `runs[].summary.num_chests`
- `runs[].summary.weekly_modifiers`
- `runs[].score`
- `ui`

When opening an individual run from that group, the site requests:

`/api/mythic-plus/runs/{season}/{keystone_run_id}`

Example:

```text
/api/mythic-plus/runs/season-mn-1/66715
```

Observed response shape:

- `keystoneRun`
- `keystoneRun.keystone_run_id`
- `keystoneRun.logged_run_id`
- `keystoneRun.completed_at`
- `keystoneRun.mythic_level`
- `keystoneRun.score`
- `keystoneRun.logged_details`
- `keystoneRun.logged_details.deaths`
- `keystoneRun.logged_details.encounters`
- `keystoneRun.roster`
- `keystoneRun.roster[].character`
- `keystoneRun.roster[].items`
- `keystoneRun.roster[].talentLoadout`

### Important implications

- These endpoints expose richer data than the documented public API.
- They appear to be website-facing JSON endpoints, not a stable external developer contract.
- They include IDs the public API does not consistently expose at this granularity, especially:
  - `characterId`
  - `dungeonId`
  - `keystone_run_id`
  - `logged_run_id`
- The run detail payload is much larger than the public profile payload and includes roster snapshots, deaths, encounter timing, talents, and gear.

## Product Positioning

This should be treated as a separate ingestion mode:

- not part of the default sync loop
- not required for summary publishing
- used for manual recovery or targeted historical backfill

The main bot should continue to use the public API for normal operation. The internal API backfill path should exist because it helps recover missing history, not because it is safer or more supported.

## Non-Goals

- Do not replace the normal public API sync path.
- Do not make summary publishing depend on internal API availability.
- Do not assume the internal API is complete for every player forever.
- Do not assume request parameters or response fields are stable.

## Core Design Principles

- Keep the backfill path isolated from normal sync logic.
- Reuse the existing Mongo identity model and `runs` collection.
- Match runs by `keystone_run_id` so public and internal discoveries converge on the same document.
- Store raw internal payloads for future analysis because this API may change.
- Be conservative with request volume.
- Make the job resumable and safe to rerun.
- Prefer explicit operator-driven time windows over "scan everything."

## Canonical Matching Rules

### Primary run key

`keystone_run_id` remains the canonical run identifier.

This aligns with the existing repository design:

- `runs` already has a unique index on `keystone_run_id`
- live sync already deduplicates on `keystone_run_id`

### Secondary identifiers to store

The backfill path should also store:

- `logged_run_id`
- `characterId` values observed during discovery
- any stable replay or correlation identifiers that may help future debugging

These are useful metadata, but they should not replace `keystone_run_id` as the unique key.

### Merge behavior

If a run is discovered from both sources:

- keep one `runs` document
- merge fields onto that document
- preserve source provenance
- never create a duplicate by source

Backfill should be additive. It can enrich an existing run document, but it should not create a parallel "internal copy."

## Proposed Mongo Shape Changes

The current `runs` collection already works well for convergence on `keystone_run_id`. Future backfill work should extend, not replace, that shape.

### Existing fields to keep using

- `keystone_run_id`
- `season`
- `dungeon`
- `short_name`
- `mythic_level`
- `completed_at`
- `score`
- `participants`
- `discovered_from_player_keys`

### New run-level metadata to add

- `sources`
  - array of source labels such as `public_profile` and `raiderio_internal_api`
- `internal_api_summary`
  - normalized summary from `/api/characters/mythic-plus-runs`
- `internal_api_detail`
  - normalized detail summary from `/api/mythic-plus/runs/{season}/{id}`
- `internal_api_raw_summary`
  - raw summary payload as stored JSON
- `internal_api_raw_detail`
  - raw detail payload as stored JSON
- `logged_run_id`
- `detail_source`
  - indicates where the most detailed payload came from
- `backfill_last_seen_at`
- `backfill_window_tags`
  - optional record of which backfill window found this run

### Player-level metadata to consider

The current `players.requires_backfill` field is still useful. A backfill path may also want:

- `last_backfill_attempt_at`
- `last_backfill_success_at`
- `backfill_status`
- `backfill_notes`

These are optional for the first implementation, but useful for visibility.

## Proposed Backfill Script

### Script purpose

Create a manual script that backfills runs for a bounded time range using Raider.IO's internal website API.

### Key behavior

The script should:

1. Choose target players.
2. Choose a target time range.
3. Discover candidate runs from the internal list endpoint.
4. Filter candidates to the requested window using `completed_at`.
5. Upsert summary data into Mongo by `keystone_run_id`.
6. Optionally fetch detail payloads for matching runs.
7. Merge the run into the existing canonical `runs` document.
8. Log progress and outcomes clearly.

### Interface proposal

The first version should be CLI-driven and manual.

Possible shape:

```text
python -m niru.backfill \
  --season season-mn-1 \
  --start 2026-03-01T00:00:00Z \
  --end 2026-03-26T00:00:00Z \
  --players us/proudmoore/maggiesue
```

Additional flags worth supporting later:

- `--all-active-players`
- `--player-file /path/to/file.txt`
- `--include-details`
- `--dry-run`
- `--limit-players N`
- `--limit-runs N`
- `--resume`

## Discovery Strategy

### Inputs needed per player

The internal run-list endpoint requires Raider.IO website IDs that our current roster model does not store:

- `characterId`
- `dungeonId`

That means backfill will need a discovery phase.

### Character discovery

The backfill tool will need a way to resolve a rostered player to Raider.IO's internal `characterId`.

This can be implemented later in one of two ways:

- derive it from a website-facing payload already available for the player
- perform a lightweight one-time lookup using a website endpoint

This spec does not choose the exact mechanism yet, but it should be cached in Mongo once resolved.

### Dungeon discovery

We already persist season dungeon metadata in `season_dungeons`. Backfill should extend that metadata with the internal dungeon identifier where available:

- `dungeon_id`
- `map_challenge_mode_id`
- `slug`
- `short_name`

The internal `dungeonId` should be stored once per season dungeon and reused.

### Per-player run discovery loop

For each target player:

1. Resolve `characterId`.
2. Load current-season dungeons from Mongo.
3. For each dungeon, request `/api/characters/mythic-plus-runs` with:
   - target `season`
   - resolved `characterId`
   - target `dungeonId`
   - broad filters such as `role=all`, `specId=0`, `affixes=all`, `date=all`
4. Read `runs[].summary.completed_at`.
5. Keep only runs in the requested `[start, end)` window.

This is intentionally simple and robust against the internal API not offering a reliable server-side date filter.

## Detail Fetch Strategy

Once a candidate run falls inside the requested time range, the script may fetch:

`/api/mythic-plus/runs/{season}/{keystone_run_id}`

### Recommendation

Support two modes:

- summary-only backfill
- summary-plus-detail backfill

Summary-only is safer and cheaper for broad recovery windows.

Summary-plus-detail is useful when we want:

- roster snapshots
- deaths
- encounters
- gear-at-time-of-run
- richer future analytics

The first implementation can default to summary-only and make detail fetches opt-in.

## Normalization Rules

The backfill path should normalize fields into the same top-level run shape already used by the live bot wherever possible.

### Fields that should align directly

- `keystone_run_id`
- `season`
- `mythic_level`
- `completed_at`
- `score`
- `short_name`
- `dungeon`
- `clear_time_ms`

### Fields that need mapping

Internal API uses a slightly different shape than the public profile run stubs. We should normalize:

- `keystone_time_ms` to the equivalent par/timer concept
- `num_chests` into `num_keystone_upgrades` if that mapping is semantically correct for the season data
- `weekly_modifiers` into the same affix representation used elsewhere, if possible

If the semantics do not exactly match, store both:

- normalized field for common reporting
- raw field for fidelity

### Participant normalization

When detail payloads are fetched, participants should still normalize into the current `participants` array shape:

- `player_key`
- `region`
- `realm`
- `name`
- `role`
- `class`
- `spec`
- `raw`

This keeps joins and summary behavior consistent.

## Time-Range Semantics

The backfill script should accept an explicit UTC time range:

- `start` inclusive
- `end` exclusive

Comparison should be performed against normalized UTC `completed_at`.

### Why explicit timestamps

- easier to resume
- easier to rerun safely
- avoids ambiguity around local timezone and weekly reset windows
- fits Mongo queries naturally

### Expected operator usage

Good target windows:

- a known outage window
- the last N days after a bug fix
- the time between first roster import and first healthy live sync

Bad target windows:

- whole season for every player by default
- unbounded scans

## Idempotency and Resume Behavior

The backfill must be safe to rerun.

### Required behavior

- Upsert by `keystone_run_id`.
- Add source metadata with `$addToSet`-style semantics.
- Do not duplicate participants.
- Do not overwrite richer detail payloads with poorer payloads.
- Update `last_seen` timestamps when a run is reconfirmed.

### Resume support

The future script should be able to restart after interruption without corrupting data.

A simple first version can achieve this by relying on idempotent upserts alone. A richer version may also persist a backfill job record with:

- job id
- requested time window
- target players
- per-player cursor state
- counts of discovered and merged runs

## Logging Expectations

Backfill is operationally riskier than live sync, so logging should be explicit.

The script should log:

- target season
- target time window
- player count
- dungeon count
- character ID resolution failures
- request retries and failures
- runs discovered
- runs matched to existing documents
- runs newly inserted
- runs enriched with detail
- total API calls
- completion summary

## Failure Handling

Because this depends on internal endpoints, failure should degrade safely.

### Principles

- Failure for one player should not abort the whole job by default.
- Failure to fetch detail should not discard a discovered summary run.
- Unexpected schema changes should be surfaced loudly in logs.
- Backfill jobs should end with a partial-success summary rather than silent truncation.

### Schema drift handling

If required keys disappear, treat that as a warning or error and keep the raw response when possible for debugging.

## Relationship To Existing V1 Spec

This document does not change the current V1 runtime behavior:

- normal sync remains public API only
- summary publishing remains based on Mongo state
- `keystone_run_id` remains the unique run key

Instead, this adds a future operator tool that can enrich Mongo with historical runs discovered through the website's internal API.

## Open Questions

- What is the cleanest way to resolve and cache Raider.IO `characterId` for rostered players?
- Which source should populate canonical `affixes` and `num_keystone_upgrades` when public and internal payloads differ slightly?
- Should detail fetches be on by default for backfill, or opt-in?
- Do we want a separate `backfill_jobs` collection in V1.5, or is idempotent rerun behavior enough initially?
- Should backfill update `requires_backfill` and gap flags automatically, or leave that to a later reconciliation pass?

## Recommended Implementation Order

When we implement this later, the safest order is:

1. Add spec-approved storage fields and merge rules.
2. Add a small internal API client separate from the public client.
3. Add summary-only backfill for explicit players and explicit UTC windows.
4. Add optional detail fetching.
5. Add job-level resume metadata if needed.
