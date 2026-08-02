# stats-importer

## Usage

First place demo files to be processed in a directory

Fill out `.env` from `.example.env` and run

`stats-importer --directory /path/to/demo/directory`

or if building/running with cargo

`cargo run --release -- --directory /path/to/demo/directory`

Note: CSC-Stats will need to have read access to this directory

use `--season` and `--tier` flags when importing demos that do not have data in CSC-Core.

### Other Info

- Demos can be re-imported even if they exist in the stats DB.

- `--help` exists

## Cleanup

Demos that were imported successfully will be moved into the `_completed` folder in the directory provided.
Demos that were skipped/errored will be placed in `_skipped`, to reprocess just move them back into the root directory provided.

## Historical round-player-stat backfill

The `backfill` command inventories one Core season at a time, skips explicit
forfeits and legacy 1-0/0-1 forfeits, downloads each match's public Backblaze
archive (or its exact legacy CSC DigitalOcean/CSC CDN location), validates it
with `7z`, recursively discovers demos (including old `demo/` and `demos/`
layouts), and asks CSC-Stats to fingerprint every demo. A historical BO3 archive
is processed as one match-sized unit. It is dry-run only unless `--apply` and a
matching `--confirm-season` are both provided.

For an existing Stats map, the Stats endpoint locks that exact map, rechecks
reviewed demo/current-data hashes, and transactionally replaces only its Round
subtree. Match-level player stats and TeamStats are fingerprinted before and
after. If the logical Stats match does not exist, the reviewed apply uses the
normal full-ingest path in create-only mode; it cannot replace a match that
appeared after review. Both paths originate from a Core season match and use
Core's season, tier, match-day, series, and played-map metadata.
Both round repairs and full imports send Core's completion date to Stats,
falling back to the scheduled date. Applied reparses overwrite the stored Stats
match date so historical matches retain their real chronology rather than the
import date.

Historical BO3 map suffixes are preserved when usable. A stale embedded match
ID is replaced with the authoritative Core ID only when it does not identify a
different Core match in that season. A fully unnamed, complete BO3 archive can
fall back to Core's distinct played-map order; partial or mixed-naming archives
cannot use that fallback. The original archive path, identity source, and any
displaced ID are retained in the ledger. An archive containing fewer demos than
Core's played-map count is recorded as `partial_archive`, but each independently
attributable demo is still validated and recovered.

Review/apply binds `parserOutputChecksum` to the canonical repair inputs rather
than the worker's raw JSON serialization. The endpoint also records
`rawParserOutputChecksum` for audit: historical demoScrape output contains
irrelevant floating-point noise and a map-order-dependent negative
`distanceToTeammates` sentinel, which the repair path normalizes to `-999999`
before hashing and writing.

Prerequisites:

- `7z`, `timeout`, `nice`, and `ionice` on the runner host.
- A release build: `cargo build --release`.
- CSC-Stats configured with `STATS_REPAIR_TOKEN`,
  `STATS_REPAIR_STAGING_ROOT`, and an attested `STATS_REPAIR_PARSER_VERSION`.
- A host `--workspace` mounted into CSC-Stats at `--api-path-root` (or the
  runner-side `STATS_REPAIR_API_PATH_ROOT` environment variable). The latter
  is the container-visible counterpart of CSC-Stats' staging root, not a
  replacement for `STATS_REPAIR_STAGING_ROOT`.
- A verified database backup before any apply run.

Dry-run a season in low-priority mode:

```bash
scripts/run-backfill-nice.sh \
  --season 18 \
  --workspace /home/csc-core/core-docker/demos/round-repair-work \
  --api-path-root /demos/round-repair-work \
  --parser-version 'worker-vX-demoScrape-vY@sha256:image-digest'
```

Pilot one match and retain its files:

```bash
scripts/run-backfill-nice.sh \
  --season 18 --match-id 7000 --limit 1 --keep-successful \
  --workspace /home/csc-core/core-docker/demos/round-repair-work \
  --api-path-root /demos/round-repair-work \
  --parser-version 'worker-vX-demoScrape-vY@sha256:image-digest'
```

Use `--keep-all` instead when every attempted workspace must be retained,
including parse, validation, and apply failures. This is useful for a shared
development cache where re-downloading historical archives would incur egress.
`--keep-all` and `--keep-successful` are mutually exclusive. A reviewed apply
can reuse a retained archive only when its SHA-256 matches that match's checksum
in the approved dry-run ledger. Unreviewed dry runs download Core's current URL
again because an object may have been replaced without changing its URL.

A later dry run (for example, against production after a development inventory)
may reuse retained archives without weakening review by supplying the immutable
source ledger and its digest:

```bash
scripts/run-backfill-nice.sh \
  --season 12 \
  --cached-source-ledger /mnt/cs2-demos/round-repair-work/season-12-recovery-dry-run-v2.jsonl \
  --cached-source-ledger-sha256 '<sha256-from-the-source-review>' \
  --workspace /mnt/cs2-demos/round-repair-work \
  --api-path-root /round-repair-work \
  --parser-version 'worker-vX-demoScrape-vY@sha256:image-digest'
```

This option is dry-run-only. It reuses a file only when its SHA-256 equals the
per-match `archiveChecksum` in the source ledger, then reparses it and evaluates
the current database normally. Matches without a reviewed archive checksum are
downloaded from Core's current URL. Each successful download records an
`archive_cached` event before extraction or parsing, so `--keep-all` retries can
reuse parser-failed archives without treating the failed match as complete.
The digest authenticates the selected ledger bytes; it does not prove that the
ledger was reviewed or that Core's remote object is still current. The operator
must review and approve that source ledger before intentionally choosing cache
reuse over a fresh download.

After the dry run completes with no failures, freeze its ledger and record its
digest. Apply refuses to run without this exact dry-run inventory (complete for
the full season, or complete for every explicitly selected `--match-id`) and
re-validates every parser-output and database-state hash before writing:

```bash
sha256sum /home/csc-core/core-docker/demos/round-repair-work/season-18-dry-run.jsonl
```

Apply a reviewed season:

```bash
scripts/run-backfill-nice.sh \
  --season 18 --apply --confirm-season 18 \
  --reviewed-ledger /home/csc-core/core-docker/demos/round-repair-work/season-18-dry-run.jsonl \
  --reviewed-ledger-sha256 '<sha256-from-the-review>' \
  --workspace /home/csc-core/core-docker/demos/round-repair-work \
  --api-path-root /demos/round-repair-work \
  --parser-version 'worker-vX-demoScrape-vY@sha256:image-digest'
```

For a one-pass recovery, `--direct-apply` validates each demo and immediately
submits that exact checksum and fingerprint evidence for writing. It does not
use or require a reviewed dry-run ledger. The explicit season confirmation is
still mandatory, repairs retain the endpoint's optimistic-concurrency guards,
and missing matches remain create-only. Each map commits independently;
successful matches are resumable while failed matches are retried on a later
invocation of the same command and ledger.

```bash
scripts/run-backfill-nice.sh \
  --season 12 --direct-apply --confirm-season 12 --keep-all \
  --cached-source-ledger /mnt/cs2-demos/round-repair-work/season-12-source.jsonl \
  --cached-source-ledger-sha256 '<sha256-of-source-ledger>' \
  --workspace /mnt/cs2-demos/round-repair-work \
  --api-path-root /round-repair-work \
  --parser-version 'worker-vX-demoScrape-vY@sha256:image-digest'
```

Use a new ledger path for direct apply. Because there is no approval boundary,
the operator must verify the Core season, parser attestation, database backup,
cache inventory digest, and target environment before starting.

Every status transition is appended and fsynced to a JSONL ledger under the
workspace. Completed matches resume without replay. Extracted demos are deleted
with their archive when that match finishes, and the whole per-attempt workspace
is also deleted after a failure. `--keep-successful` retains completed matches;
`--keep-all` retains completed and failed attempts. Peak working disk is
therefore bounded to one compressed match archive plus that archive's extracted
contents and the small ledger, subject to the configured size limits when
neither retention flag is used. Retained workspaces accumulate and must be
capacity-planned separately. A process
kill during the final ledger append discards only the incomplete trailing record
on resume; newline-terminated/interior corruption still fails closed.
Clean endpoint verdicts that cannot be recovered (`ingest_incomplete`,
`fingerprint_mismatch`, and `ambiguous`) are recorded as terminal
`skipped_not_repairable` results. `no_matching_candidate` is instead a reviewed
create-only full import, while a mixed BO3 handles each uniquely identified map
according to its verdict. The Core-match loop is
strictly sequential, with no task spawning or buffered concurrency, and the
runner pauses five seconds between matches by default.

## Full reparse (eco/swing stats and other ingest-side fixes)

Round-level repair only replaces a match's Round subtree; it explicitly leaves
match-level `PlayerMatchStats`, `TeamStats`, and `Match` untouched (it
fingerprints them before and after to prove that). That means round-repair
never backfills the fragg-3.0 eco/swing columns (`Match.ecoStatsOK`,
`PlayerMatchStats.swing_rating`/`eco*`) onto historical matches — those live on
the match-level row. To get eco stats (or any other add-match-ingest-side fix)
onto historical matches, use `--full-reparse` instead: it reuses the same
season inventory and demo discovery as round-repair, but POSTs every
discovered demo through the normal `/api/add-match` ingest path with
`createOnly: false` rather than `/api/repair-round-stats`. CSC-Stats deletes
and recreates the match transactionally (parse happens before the delete, so
a parser failure never destroys existing data), so this is safe to rerun.

For season-20+ BO3s, `--full-reparse` prefers
`matches_demoprocessingstatus.s3_keys` (the canonical per-map upload order)
over the legacy `matches_matches.demo_url` column, which only ever points at
map 1's archive. Each key is downloaded and extracted as its own archive; the
key's position in the array — not any digit embedded in its filename, which
is known to repeat across different maps in some matches — is the map
order. The resulting `statsMatchId` uses that zero-based array position
(`{coreMatchId}_0`, `_1`, ...), matching CSC-Stats' own per-map numbering
convention as observed on live-ingested historical matches; it is not Core's
1-based `matches_matchstats.map_number`.

Before downloading anything, `s3_keys` is validated against Core's scored
maps (`matches_matchstats` rows with a real result, excluding the
unplayed-placeholder row a BO3 gets for a map it never reached) on both
count and order — each key must contain its position's map name. In
practice `s3_keys` is not reliably a *complete* per-map array: most
season-19 BO3s have a `DemoProcessingStatus` row whose `s3_keys` holds just
one key (a single per-map upload alongside the real bundled archive at
`demo_url`) even though multiple maps were played, and a handful of
season-20 matches have a corrupt count (a duplicate upload from a mid-match
restart) or order (same count, keys out of sequence). A validation failure
logs a clear `s3_keys_mismatch` ledger event and falls back to the legacy
single-`demo_url` path for that match — the same path used for BO1s and for
matches with no `s3_keys` row at all — rather than guessing or regressing a
match that already reparses correctly today. This `s3_keys` path is
full-reparse only — round-repair's reviewed-ledger/idempotency machinery is
keyed on one archive per match, so BO3 round-repair on season-20+ matches
remains a known follow-up.

`--full-reparse` has no round-level fingerprint/checksum review flow and no
`--parser-version` requirement (add-match has no parser-version attestation
concept) — a match is either reparsed or it isn't. Without `--confirm-season`
it only downloads and discovers demos and records what it *would* reparse
(`full_reparse_planned` ledger events); nothing is written. It cannot be
combined with `--apply`, `--direct-apply`, or the reviewed/cached-source-ledger
flags.

Dry-run (discover only, no writes):

```bash
scripts/run-backfill-nice.sh \
  --season 19 --full-reparse \
  --workspace /home/csc-core/core-docker/demos/round-repair-work \
  --api-path-root /demos/round-repair-work
```

Apply:

```bash
scripts/run-backfill-nice.sh \
  --season 19 --full-reparse --confirm-season 19 \
  --workspace /home/csc-core/core-docker/demos/round-repair-work \
  --api-path-root /demos/round-repair-work
```

Before running this against a season with live traffic, confirm CSC-Stats'
core-identity mirror is fresh (add-match fails closed with a 503 if it's stale
by more than 26h) — a season-length run will otherwise burn archive-download
egress only to 503 on every write.

`--bo3-only` restricts the season inventory to `is_bo3 = true` matches,
skipping BO1s. It is a match-selection filter, not a mode — it composes with
`--match-id`, `--full-reparse`, `--apply`, `--direct-apply`, and dry-run the
same way `--match-id` does, rather than being mutually exclusive with any of
them. Useful for validating the s3_keys multi-map path (or any other
BO3-specific change) against just a season's BO3s instead of reparsing every
BO1 too:

```bash
scripts/run-backfill-nice.sh \
  --season 20 --full-reparse --bo3-only \
  --workspace /home/csc-core/core-docker/demos/round-repair-work \
  --api-path-root /demos/round-repair-work
```
