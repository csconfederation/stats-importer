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
with `7z`, recursively discovers demos (including old
`demo/` and `demos/` layouts), and asks CSC-Stats to fingerprint every demo.
It is dry-run only unless `--apply` and a matching `--confirm-season` are both
provided.

The Stats endpoint performs the mutation: it locks one exact Stats map,
rechecks reviewed demo/current-data hashes, and transactionally replaces only
its Round subtree. Match-level player stats and TeamStats are fingerprinted
before and after. Any mismatch rolls the transaction back. Historical BO3 map
suffixes are preserved from filenames, so zero-based, one-based, and
non-contiguous series are supported; archive order is never used.

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
workspace. Completed matches resume without replay; failures retain their
isolated workspace. A process kill during the final ledger append discards only
the incomplete trailing record on resume; newline-terminated/interior corruption
still fails closed. Clean endpoint verdicts that cannot be repaired
(`ingest_incomplete`, `no_matching_candidate`, `fingerprint_mismatch`, and
`ambiguous`) are recorded as terminal `skipped_not_repairable` results, while a
mixed BO3 still repairs its uniquely validated maps. The runner is strictly
sequential and pauses five seconds between matches by default.
