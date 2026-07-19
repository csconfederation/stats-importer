#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
binary="${STATS_IMPORTER_BIN:-${script_dir}/target/release/stats-importer}"

if [[ ! -x "${binary}" ]]; then
  echo "Missing release binary: ${binary}" >&2
  echo "Build it first with: cargo build --release" >&2
  exit 2
fi

# niceness 15 plus idle-class disk I/O keeps the coordinator and its 7z child
# low priority. The backfill keeps only one match/BO3 workspace at a time and
# pauses between matches.
exec nice -n "${STATS_REPAIR_NICE:-15}" ionice -c 3 "${binary}" backfill "$@"
