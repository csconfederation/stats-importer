# stats-importer

## Usage

Fill out `.env` from `.example.env` and run

`stats-importer --directory /path/to/demo/directory`

or if building/running with cargo

`cargo run -- --directory /path/to/demo/directory`

use `--season` and `--tier` flags when importing demos that do not have data in CSC-Core.

### Other Info

- Demos can be re-imported even if they exist in the stats DB.

- `--help` exists

## Cleanup

Demos that were imported successfully will be moved into the `_completed` folder in the directory provided.
Demos that were skipped/errored will be placed in `_skipped`, to reprocess just move them back into the root directory provided.
