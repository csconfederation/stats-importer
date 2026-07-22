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
downloaded from Core's current URL.

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
