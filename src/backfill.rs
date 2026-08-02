use std::{
    collections::{HashMap, HashSet},
    env,
    fs::{self, OpenOptions},
    io::{ErrorKind, Write},
    path::{Component, Path, PathBuf},
    process::Command,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context, Result};
use clap::Args;
use fs2::FileExt;
use futures_util::StreamExt;
use regex::Regex;
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{FromRow, PgPool};
use tokio::{fs::File, io::AsyncWriteExt, time::sleep};
use walkdir::WalkDir;

const B2_HOST: &str = "f005.backblazeb2.com";
const B2_PATH_PREFIX: &str = "/file/csc-demo-archive/";
const LEGACY_DO_HOSTS: [&str; 2] = [
    "cscdemos.nyc3.digitaloceanspaces.com",
    "cscdemos.nyc3.cdn.digitaloceanspaces.com",
];

#[derive(Args, Debug, Clone)]
pub struct BackfillArgs {
    /// Core season number to inventory or repair.
    #[arg(long)]
    season: i32,

    /// Apply verified repairs. Without this flag the run is read-only.
    #[arg(long, conflicts_with = "full_reparse")]
    apply: bool,

    /// Validate and immediately apply each recoverable demo without a reviewed ledger.
    #[arg(
        long,
        conflicts_with_all = ["apply", "full_reparse", "reviewed_ledger", "reviewed_ledger_sha256"]
    )]
    direct_apply: bool,

    /// Skip round-level repair and instead reparse every season demo through the
    /// full add-match ingest path (createOnly=false), so eco/swing stats and any
    /// ingest-side fixes land on historical matches. Without --confirm-season this
    /// only downloads/discovers demos and reports what would be reparsed. There is
    /// no reviewed-ledger flow for this mode: add-match reparses are safe to rerun
    /// (existing rows are replaced transactionally), so it does not need the
    /// round-repair path's dry-run/apply review gate.
    #[arg(
        long,
        conflicts_with_all = ["apply", "direct_apply", "reviewed_ledger", "reviewed_ledger_sha256", "cached_source_ledger", "cached_source_ledger_sha256", "combines"]
    )]
    full_reparse: bool,

    /// Reparse a season's combine matches (matches_combinematches) instead of
    /// league matches (matches_matches). Combines have no round-repair concept
    /// (no per-round stat correction target) and are always a single map, so
    /// this always behaves like --full-reparse: it downloads/discovers demos
    /// and, once --confirm-season is supplied, reparses them through the same
    /// add-match ingest path used for league matches, with the resulting
    /// stats match id prefixed `combines-{id}` so CSC-Stats' add-match handler
    /// treats it as a combine import. Combine matches have no season foreign
    /// key in Core's schema, so season scoping is inferred from the `sNN/`
    /// path segment CSC's demo archival tooling puts in demo_url.
    #[arg(
        long,
        conflicts_with_all = ["apply", "direct_apply", "full_reparse", "reviewed_ledger", "reviewed_ledger_sha256", "cached_source_ledger", "cached_source_ledger_sha256", "bo3_only"]
    )]
    combines: bool,

    /// Required with --apply, --direct-apply, --full-reparse, or --combines to make writes explicit.
    #[arg(long)]
    confirm_season: Option<i32>,

    /// Version/profile configured by CSC-Stats for the pinned parser. Required for
    /// round-repair modes; unused (and not required) by --full-reparse or
    /// --combines, neither of which has a parser-version attestation concept
    /// on the add-match endpoint.
    #[arg(
        long,
        env = "STATS_REPAIR_PARSER_VERSION",
        required_unless_present_any = ["full_reparse", "combines"]
    )]
    parser_version: Option<String>,

    /// Host directory used for downloads/extraction; it must be shared with CSC-Stats.
    #[arg(long, default_value = "./round-repair-work")]
    workspace: PathBuf,

    /// Path corresponding to --workspace inside the CSC-Stats container.
    #[arg(long, env = "STATS_REPAIR_API_PATH_ROOT")]
    api_path_root: PathBuf,

    /// Append-only JSONL status ledger. Defaults under the workspace.
    #[arg(long)]
    ledger: Option<PathBuf>,

    /// Complete dry-run JSONL ledger approved for this apply run.
    #[arg(long, requires = "apply")]
    reviewed_ledger: Option<PathBuf>,

    /// SHA-256 of --reviewed-ledger, required for apply.
    #[arg(long, requires = "apply")]
    reviewed_ledger_sha256: Option<String>,

    /// Prior dry-run ledger used only to checksum-verify retained source archives.
    #[arg(
        long,
        requires = "cached_source_ledger_sha256",
        conflicts_with = "apply"
    )]
    cached_source_ledger: Option<PathBuf>,

    /// SHA-256 of --cached-source-ledger.
    #[arg(long, requires = "cached_source_ledger", conflicts_with = "apply")]
    cached_source_ledger_sha256: Option<String>,

    /// Seconds to pause after each Core match (default 5).
    #[arg(long, default_value_t = 5)]
    pause_seconds: u64,

    /// Stop after this many non-resumed Core matches (useful for canaries).
    #[arg(long)]
    limit: Option<usize>,

    /// Process only one Core match ID (repeatable).
    #[arg(long)]
    match_id: Vec<i64>,

    /// Restrict the season inventory to BO3 matches only (is_bo3 = true),
    /// skipping BO1s. A match-selection filter, not a mode: it composes
    /// with --full-reparse/--apply/--direct-apply/dry-run the same way
    /// --match-id does, rather than being mutually exclusive with them.
    /// Useful for validating a BO3-specific fix (e.g. the s3_keys
    /// multi-map path) without reparsing an entire season's BO1s too.
    /// Mutually exclusive with --combines: combine matches are always
    /// synthesized with is_bo3 = false, so --bo3-only would silently
    /// filter out every combine and produce an empty run.
    #[arg(long, conflicts_with = "combines")]
    bo3_only: bool,

    /// Keep successful per-match workspaces instead of deleting them.
    #[arg(long, conflicts_with = "keep_all")]
    keep_successful: bool,

    /// Keep every per-match workspace, including failed attempts.
    #[arg(long, conflicts_with = "keep_successful")]
    keep_all: bool,

    /// Maximum archive download size in GiB.
    #[arg(long, default_value_t = 8)]
    max_archive_gib: u64,

    /// Maximum total uncompressed archive size in GiB.
    #[arg(long, default_value_t = 32)]
    max_extracted_gib: u64,

    /// Maximum archive member count.
    #[arg(long, default_value_t = 100)]
    max_archive_members: usize,
}

#[derive(Debug, FromRow, Clone)]
struct CoreMatch {
    match_id: i64,
    is_bo3: bool,
    demo_url: Option<String>,
    map_count: i64,
    played_map_numbers: Vec<i32>,
    match_day: String,
    match_date: String,
    tier: Option<String>,
    marked_forfeit: bool,
    legacy_one_zero: bool,
    has_forfeit_audit: bool,
    // Map names, in played (map_number) order, for the maps that were
    // actually played (excludes the unplayed-placeholder matches_matchstats
    // row a BO3 gets for a map it never reached, e.g. a 2-0 sweep's map 3).
    // Used only to validate the s3_keys match-count/order gate below; it is
    // NOT a substitute for played_map_numbers in the legacy discover_demos
    // path, which intentionally still counts placeholder rows.
    scored_map_names: Vec<String>,
    // Canonical per-map upload order from matches_demoprocessingstatus.s3_keys
    // (a JSONB array of S3 object keys, index 0 = map 1, ...). This is the
    // source of truth for BO3 map order on season-20+ matches; the legacy
    // matches_matches.demo_url column only ever points at map 1's archive.
    // Absent (NULL/empty) for BO1s and for matches that predate per-map
    // uploads (those got a single bundled archive at demo_url instead).
    s3_keys: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LedgerEvent {
    schema_version: u8,
    timestamp_unix: u64,
    season: i32,
    mode: String,
    match_id: i64,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    stats_match_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    evidence: Option<Value>,
}

#[derive(Debug, Clone)]
struct DemoCandidate {
    path: PathBuf,
    relative_path: String,
    stats_match_id: String,
    checksum: String,
    identity_source: String,
    displaced_match_id: Option<i64>,
}

#[derive(Debug)]
struct Validation {
    candidate: DemoCandidate,
    response: Value,
}

struct AttemptWorkspace {
    root: PathBuf,
    path: PathBuf,
    retained: bool,
    retain_on_drop: bool,
}

impl AttemptWorkspace {
    fn new(root: &Path, path: PathBuf, retain_on_drop: bool) -> Self {
        Self {
            root: root.to_path_buf(),
            path,
            retained: false,
            retain_on_drop,
        }
    }

    fn finish(&mut self, retain: bool) -> Result<()> {
        if retain {
            self.retained = true;
            return Ok(());
        }
        remove_isolated_directory(&self.root, &self.path)?;
        self.retained = true;
        Ok(())
    }
}

impl Drop for AttemptWorkspace {
    fn drop(&mut self) {
        if self.retained || self.retain_on_drop || !self.path.exists() {
            return;
        }
        if let Err(error) = remove_isolated_directory(&self.root, &self.path) {
            eprintln!(
                "failed to clean attempt workspace {}: {error:#}",
                self.path.display()
            );
        }
    }
}

struct ReviewedInventory {
    checksum: String,
    ready: HashMap<(i64, String, String), Value>,
    terminal_matches: HashSet<i64>,
    terminal_status: HashMap<i64, String>,
    ready_sets: HashMap<i64, HashSet<(String, String)>>,
    importable: HashMap<(i64, String, String), Value>,
    importable_sets: HashMap<i64, HashSet<(String, String)>>,
    archive_checksums: HashMap<i64, String>,
}

#[derive(Debug)]
struct CachedSourceInventory {
    checksum: String,
    archive_checksums: HashMap<i64, String>,
}

impl BackfillArgs {
    fn writes(&self) -> bool {
        self.apply
            || self.direct_apply
            || ((self.full_reparse || self.combines) && self.confirm_season.is_some())
    }

    fn mode(&self) -> &'static str {
        if self.direct_apply {
            "direct-apply"
        } else if self.apply {
            "apply"
        } else if self.combines {
            // Distinct mode strings keep combine ledger/workspace bookkeeping
            // out of the league-match id space; matches_combinematches.id and
            // matches_matches.id are independent sequences that can collide.
            if self.confirm_season.is_some() {
                "combines-full-reparse"
            } else {
                "combines-full-reparse-dry-run"
            }
        } else if self.full_reparse {
            if self.confirm_season.is_some() {
                "full-reparse"
            } else {
                "full-reparse-dry-run"
            }
        } else {
            "dry-run"
        }
    }
}

impl CachedSourceInventory {
    fn load(args: &BackfillArgs) -> Result<Option<Self>> {
        let Some(path) = &args.cached_source_ledger else {
            return Ok(None);
        };
        let expected = args.cached_source_ledger_sha256.as_deref().ok_or_else(|| {
            anyhow!("--cached-source-ledger requires --cached-source-ledger-sha256")
        })?;
        let bytes = fs::read(path)?;
        let checksum = hex::encode(Sha256::digest(&bytes));
        if checksum != expected {
            bail!("cached source ledger SHA-256 mismatch");
        }
        let content = String::from_utf8(bytes).context("cached source ledger is not UTF-8")?;
        let mut archive_checksums = HashMap::new();
        for (line_number, line) in content.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let event: LedgerEvent = serde_json::from_str(line).with_context(|| {
                format!(
                    "invalid cached source ledger JSON at line {}",
                    line_number + 1
                )
            })?;
            if event.schema_version != 1 || event.season != args.season || event.mode != "dry-run" {
                bail!(
                    "cached source ledger line {} is not this season's schema-v1 dry run",
                    line_number + 1
                );
            }
            if matches!(
                event.status.as_str(),
                "archive_cached"
                    | "using_cached_archive"
                    | "match_complete"
                    | "skipped_not_repairable"
            ) {
                if let Some(value) = event
                    .evidence
                    .as_ref()
                    .and_then(|item| item.get("archiveChecksum"))
                    .and_then(Value::as_str)
                {
                    if archive_checksums
                        .insert(event.match_id, value.to_owned())
                        .is_some_and(|previous| previous != value)
                    {
                        bail!(
                            "cached source ledger has conflicting archive checksums for match {}",
                            event.match_id
                        );
                    }
                }
            }
        }
        Ok(Some(Self {
            checksum,
            archive_checksums,
        }))
    }
}

impl ReviewedInventory {
    fn load(args: &BackfillArgs) -> Result<Option<Self>> {
        if !args.apply {
            return Ok(None);
        }
        let path = args
            .reviewed_ledger
            .as_ref()
            .ok_or_else(|| anyhow!("--apply requires --reviewed-ledger"))?;
        let expected = args
            .reviewed_ledger_sha256
            .as_deref()
            .ok_or_else(|| anyhow!("--apply requires --reviewed-ledger-sha256"))?;
        let bytes = fs::read(path)?;
        let checksum = hex::encode(Sha256::digest(&bytes));
        if checksum != expected {
            bail!("reviewed ledger SHA-256 mismatch");
        }
        let content = String::from_utf8(bytes).context("reviewed ledger is not UTF-8")?;
        let mut ready = HashMap::new();
        let mut terminal_matches = HashSet::new();
        let mut terminal_status = HashMap::new();
        let mut ready_sets: HashMap<i64, HashSet<(String, String)>> = HashMap::new();
        let mut importable = HashMap::new();
        let mut importable_sets: HashMap<i64, HashSet<(String, String)>> = HashMap::new();
        let mut archive_checksums = HashMap::new();
        for (line_number, line) in content.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let event: LedgerEvent = serde_json::from_str(line).with_context(|| {
                format!("invalid reviewed ledger JSON at line {}", line_number + 1)
            })?;
            if event.schema_version != 1 || event.season != args.season || event.mode != "dry-run" {
                bail!(
                    "reviewed ledger line {} is not this season's schema-v1 dry run",
                    line_number + 1
                );
            }
            if is_terminal_status(&event.status) {
                terminal_matches.insert(event.match_id);
                terminal_status.insert(event.match_id, event.status.clone());
                if matches!(
                    event.status.as_str(),
                    "match_complete" | "skipped_not_repairable"
                ) {
                    let checksum = event
                        .evidence
                        .as_ref()
                        .and_then(|value| value.get("archiveChecksum"))
                        .and_then(Value::as_str)
                        .ok_or_else(|| anyhow!("reviewed match_complete has no archiveChecksum"))?;
                    archive_checksums.insert(event.match_id, checksum.to_owned());
                }
            }
            if event.status == "demo_validated" {
                let stats_match_id = event
                    .stats_match_id
                    .ok_or_else(|| anyhow!("reviewed demo event has no stats_match_id"))?;
                let evidence = event
                    .evidence
                    .ok_or_else(|| anyhow!("reviewed demo event has no evidence"))?;
                let demo_checksum = evidence
                    .get("demoChecksum")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("reviewed demo event has no demoChecksum"))?
                    .to_owned();
                let result = evidence
                    .get("result")
                    .cloned()
                    .ok_or_else(|| anyhow!("reviewed demo event has no result"))?;
                if result.get("classification").and_then(Value::as_str) == Some("ready") {
                    ready_sets
                        .entry(event.match_id)
                        .or_default()
                        .insert((stats_match_id.clone(), demo_checksum.clone()));
                    ready.insert((event.match_id, stats_match_id, demo_checksum), result);
                } else if result.get("classification").and_then(Value::as_str)
                    == Some("no_matching_candidate")
                {
                    importable_sets
                        .entry(event.match_id)
                        .or_default()
                        .insert((stats_match_id.clone(), demo_checksum.clone()));
                    importable.insert((event.match_id, stats_match_id, demo_checksum), result);
                }
            }
        }
        Ok(Some(Self {
            checksum,
            ready,
            terminal_matches,
            terminal_status,
            ready_sets,
            importable,
            importable_sets,
            archive_checksums,
        }))
    }
}

fn verify_reviewed_terminal(
    inventory: Option<&ReviewedInventory>,
    match_id: i64,
    status: &str,
) -> Result<()> {
    if let Some(inventory) = inventory {
        if inventory.terminal_status.get(&match_id).map(String::as_str) != Some(status)
            || inventory
                .ready_sets
                .get(&match_id)
                .is_some_and(|set| !set.is_empty())
            || inventory
                .importable_sets
                .get(&match_id)
                .is_some_and(|set| !set.is_empty())
        {
            bail!("current {status} classification differs from reviewed inventory");
        }
    }
    Ok(())
}

struct Ledger {
    file: fs::File,
    completed: HashSet<(i32, String, i64)>,
}

fn is_terminal_status(status: &str) -> bool {
    matches!(
        status,
        "match_complete"
            | "skipped_forfeit"
            | "skipped_not_repairable"
            | "artifact_missing"
            | "artifact_unsupported"
    )
}

fn is_clean_non_repairable(classification: Option<&str>) -> bool {
    matches!(
        classification,
        Some("ingest_incomplete" | "fingerprint_mismatch" | "ambiguous")
    )
}

fn verify_reviewed_import(reviewed: &Value, current: &Value) -> Result<()> {
    for field in [
        "sourceChecksum",
        "parserOutputChecksum",
        "parserVersion",
        "parsedSubtreeHash",
    ] {
        if reviewed.get(field).and_then(Value::as_str).is_none() {
            bail!("reviewed missing-match candidate omitted {field}");
        }
        if reviewed.get(field) != current.get(field) {
            bail!("current missing-match validation differs from reviewed inventory at {field}");
        }
    }
    Ok(())
}

fn parse_full_import_response(status: StatusCode, body: &str) -> Result<Value> {
    if !status.is_success() {
        bail!("Stats full-import endpoint returned {status}: {body}");
    }
    serde_json::from_str(body)
        .with_context(|| format!("Stats full-import endpoint returned {status} with non-JSON body"))
}

struct WorkspaceLock {
    _file: fs::File,
}

impl WorkspaceLock {
    fn acquire(workspace: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(workspace.join(".backfill.lock"))?;
        file.try_lock_exclusive()
            .map_err(|error| anyhow!("workspace is already in use by another runner: {error}"))?;
        Ok(Self { _file: file })
    }
}

impl Ledger {
    fn open(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        file.try_lock_exclusive()
            .map_err(|error| anyhow!("ledger is already locked by another runner: {error}"))?;
        let mut completed = HashSet::new();
        let bytes = fs::read(&path)?;
        let complete_len = bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |position| position + 1);
        let mut events = Vec::new();
        for (line_number, line) in bytes[..complete_len]
            .split(|byte| *byte == b'\n')
            .enumerate()
        {
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let event: LedgerEvent = serde_json::from_slice(line)
                .with_context(|| format!("invalid ledger JSON at line {}", line_number + 1))?;
            events.push(event);
        }
        let trailing = &bytes[complete_len..];
        if !trailing.iter().all(u8::is_ascii_whitespace) {
            match serde_json::from_slice::<LedgerEvent>(trailing) {
                Ok(event) => {
                    events.push(event);
                    file.write_all(b"\n")?;
                    file.sync_all()?;
                }
                Err(_) => {
                    file.set_len(complete_len as u64)?;
                    file.sync_all()?;
                    eprintln!(
                        "discarded an incomplete trailing ledger record at byte {complete_len}"
                    );
                }
            }
        }
        for event in events {
            if event.schema_version != 1 {
                bail!("unsupported ledger schema version {}", event.schema_version);
            }
            if is_terminal_status(&event.status) {
                completed.insert((event.season, event.mode, event.match_id));
            }
        }
        Ok(Self { file, completed })
    }

    fn is_complete(&self, season: i32, mode: &str, match_id: i64) -> bool {
        self.completed
            .contains(&(season, mode.to_owned(), match_id))
    }

    fn append(&mut self, event: LedgerEvent) -> Result<()> {
        let mut record = serde_json::to_vec(&event)?;
        record.push(b'\n');
        self.file.write_all(&record)?;
        self.file.sync_all()?;
        if is_terminal_status(&event.status) {
            self.completed
                .insert((event.season, event.mode, event.match_id));
        }
        println!(
            "[match {}/season {}] {}{}",
            event.match_id,
            event.season,
            event.status,
            event
                .message
                .as_deref()
                .map(|m| format!(": {m}"))
                .unwrap_or_default()
        );
        Ok(())
    }
}

fn event(
    args: &BackfillArgs,
    match_id: i64,
    status: &str,
    stats_match_id: Option<String>,
    message: Option<String>,
    evidence: Option<Value>,
) -> LedgerEvent {
    LedgerEvent {
        schema_version: 1,
        timestamp_unix: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        season: args.season,
        mode: args.mode().to_owned(),
        match_id,
        status: status.to_owned(),
        stats_match_id,
        message,
        evidence,
    }
}

fn canonical_output_path(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return Ok(fs::canonicalize(path)?);
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path
        .file_name()
        .ok_or_else(|| anyhow!("invalid ledger path"))?;
    Ok(fs::canonicalize(parent)?.join(name))
}

/// Whether a Core match should be processed this run, given the two
/// orthogonal match-selection filters: `--bo3-only` (skip BO1s) and
/// `--match-id` (process only the named matches, repeatable; empty means
/// "no restriction"). Both apply together — a match must satisfy every
/// active filter, not just one.
fn is_selected(core_match: &CoreMatch, bo3_only: bool, selected: &HashSet<i64>) -> bool {
    if bo3_only && !core_match.is_bo3 {
        return false;
    }
    if !selected.is_empty() && !selected.contains(&core_match.match_id) {
        return false;
    }
    true
}

async fn season_matches(pool: &PgPool, season: i32) -> Result<Vec<CoreMatch>> {
    let rows = sqlx::query_as::<_, CoreMatch>(
        r#"
        WITH stat_flags AS (
          SELECT ms.match_id,
                 bool_or(ms.is_forfeit) AS marked_forfeit,
                 bool_or(
                   (ms.home_score = 1 AND ms.away_score = 0) OR
                   (ms.home_score = 0 AND ms.away_score = 1) OR
                   regexp_replace(coalesce(ms.score, ''), '\s+', '', 'g') IN ('1-0', '0-1')
                 ) AS legacy_one_zero,
                 count(DISTINCT ms.map_number)::bigint AS map_count,
                 array_agg(DISTINCT ms.map_number ORDER BY ms.map_number) AS played_map_numbers,
                 -- BO3s get a placeholder matches_matchstats row for map 3
                 -- created up front even when the series ends 2-0 and map 3
                 -- is never played (home_score=away_score=0, winner_id
                 -- NULL). map_count/played_map_numbers above intentionally
                 -- still include it (existing legacy-path consumers rely on
                 -- the raw count); this filtered pair is for the s3_keys
                 -- match-count gate only, which must not be fooled by an
                 -- unplayed placeholder into flagging a clean 2-map s3_keys
                 -- array as a mismatch (Core-planning#242 item 3). A
                 -- handful of old matches have a real score but a
                 -- never-backfilled null winner_id, so "played" is
                 -- winner_id set OR either score non-zero, not winner_id
                 -- alone.
                 -- map_name is nullable; coalesce so a NULL never lands in
                 -- the aggregate (sqlx errors decoding a NULL element into
                 -- Vec<String>, which would take down the whole season
                 -- query, not just one match).
                 array_agg(coalesce(ms.map_name, '') ORDER BY ms.map_number) FILTER (
                   WHERE ms.winner_id IS NOT NULL OR ms.home_score <> 0 OR ms.away_score <> 0
                 ) AS scored_map_names
          FROM matches_matchstats ms
          GROUP BY ms.match_id
        ), audit_flags AS (
          SELECT match_id, true AS has_forfeit_audit
          FROM matches_matchscoreaudit
          GROUP BY match_id
        )
        SELECT m.id AS match_id,
               m.is_bo3,
               m.demo_url,
               coalesce(sf.map_count, 0)::bigint AS map_count,
               coalesce(sf.played_map_numbers, ARRAY[]::integer[]) AS played_map_numbers,
               md.number::text AS match_day,
               to_char(coalesce(m.completed_at, m.scheduled_date) AT TIME ZONE 'UTC',
                       'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS match_date,
               coalesce(home_tier.name, away_tier.name) AS tier,
               coalesce(sf.marked_forfeit, false) AS marked_forfeit,
               coalesce(sf.legacy_one_zero, false) AS legacy_one_zero,
               coalesce(af.has_forfeit_audit, false) AS has_forfeit_audit,
               coalesce(sf.scored_map_names, ARRAY[]::text[]) AS scored_map_names,
               dps.s3_keys AS s3_keys
        FROM matches_matches m
        JOIN leagues_matchday md ON md.id = m.match_day_id
        JOIN leagues_seasons s ON s.id = md.season_id
        LEFT JOIN teams_teams home_team ON home_team.id = m.home_id
        LEFT JOIN players_tiers home_tier ON home_tier.id = home_team.tier_id
        LEFT JOIN teams_teams away_team ON away_team.id = m.away_id
        LEFT JOIN players_tiers away_tier ON away_tier.id = away_team.tier_id
        LEFT JOIN stat_flags sf ON sf.match_id = m.id
        LEFT JOIN audit_flags af ON af.match_id = m.id
        LEFT JOIN matches_demoprocessingstatus dps ON dps.match_id = m.id
        WHERE s.number = $1
        ORDER BY md.scheduled_date, m.id
        "#,
    )
    .bind(season)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

#[derive(Debug, FromRow, Clone)]
struct CombineMatchRow {
    match_id: i64,
    demo_url: Option<String>,
    tier: Option<String>,
    match_date: String,
}

/// Fetches a season's combine matches (matches_combinematches), shaped as
/// CoreMatch so the rest of the reparse pipeline (discover_demos,
/// process_match, the Ledger) can be reused unchanged. Combines have no
/// BO3/round-repair concept: every combine is a single map, so is_bo3 is
/// always false, map_count is always 1, and the forfeit/audit flags (which
/// only gate the round-repair path, never taken for combines) are always
/// false.
///
/// matches_combinematches has no season foreign key the way matches_matches
/// has via leagues_matchday -> leagues_seasons, so season scoping can't be a
/// SQL join. CSC's demo archival tooling embeds the season as an `sNN/` path
/// segment in demo_url (verified against prod-mirrored data: every archived
/// combine and league match demo_url carries this segment), so that's what
/// this query filters on. The regex requires a trailing `/` after the season
/// number so season 1 cannot match season 10-19's `s1N/` prefix.
///
/// tier_id is nullable on matches_combinematches (unlike a league match's
/// tier, which is always derivable from its teams), so this is an inner join:
/// a combine without a tier can't be reparsed (full_import_request's `tier`
/// is a required add-match field) and would otherwise download/extract an
/// archive only to fail at apply time. Excluding it here surfaces the gap as
/// a missing row in the season's discovered match count rather than a
/// mid-run failure.
async fn combine_season_matches(pool: &PgPool, season: i32) -> Result<Vec<CoreMatch>> {
    let rows = sqlx::query_as::<_, CombineMatchRow>(
        r#"
        SELECT mm.id AS match_id,
               mm.demo_url,
               pt.name AS tier,
               to_char(coalesce(mm.game_finished_at, mm.scheduled_date) AT TIME ZONE 'UTC',
                       'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS match_date
        FROM matches_combinematches mm
        JOIN players_tiers pt ON pt.id = mm.tier_id
        WHERE mm.cancelled = false
          AND mm.game_finished = true
          AND mm.demo_url IS NOT NULL
          AND mm.demo_url ~ ('/s' || $1::text || '/')
        ORDER BY mm.scheduled_date, mm.id
        "#,
    )
    .bind(season)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| CoreMatch {
            match_id: row.match_id,
            is_bo3: false,
            demo_url: row.demo_url,
            map_count: 1,
            played_map_numbers: vec![1],
            match_day: String::new(),
            match_date: row.match_date,
            tier: row.tier,
            marked_forfeit: false,
            legacy_one_zero: false,
            has_forfeit_audit: false,
            // Combines have no matches_demoprocessingstatus row (that table
            // is keyed to matches_matches, not matches_combinematches), so
            // there is no s3_keys array to consult; combines always go
            // through the legacy single-demo_url path, consistent with
            // being single-map.
            scored_map_names: Vec::new(),
            s3_keys: None,
        })
        .collect())
}

fn validate_archive_url(raw: &str) -> Result<Url> {
    validate_archive_url_labeled(raw, "demo_url")
}

fn validate_archive_url_labeled(raw: &str, label: &str) -> Result<Url> {
    let url = Url::parse(raw).with_context(|| format!("invalid {label}"))?;
    let host = url.host_str();
    let backblaze = host == Some(B2_HOST) && url.path().starts_with(B2_PATH_PREFIX);
    let legacy_digital_ocean = host.is_some_and(|value| LEGACY_DO_HOSTS.contains(&value));
    if url.scheme() != "https"
        || (!backblaze && !legacy_digital_ocean)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
    {
        bail!("{label} is not an allowlisted CSC archive URL");
    }
    if !(url.path().to_ascii_lowercase().ends_with(".7z")
        || url.path().to_ascii_lowercase().ends_with(".zip"))
    {
        bail!("{label} is not a .7z/.zip archive");
    }
    Ok(url)
}

/// CDN host that CSC-Core's `apps/matches/demo_artifacts.py::build_demo_url()`
/// prefixes `matches_demoprocessingstatus.s3_keys` entries with. It is
/// already present in `LEGACY_DO_HOSTS`.
const S3_KEYS_CDN_HOST: &str = "cscdemos.nyc3.cdn.digitaloceanspaces.com";

/// Parses the `matches_demoprocessingstatus.s3_keys` JSONB column into an
/// ordered list of object keys. `None`/JSON `null` and an empty array both
/// mean "no per-map artifacts recorded" and yield an empty vec (the caller
/// falls back to the legacy `demo_url` path for those). Any other shape
/// (non-array, or an array containing a non-string element) is treated as
/// corrupt data and returns an error rather than silently dropping entries,
/// since a dropped entry would reintroduce exactly the kind of silent
/// map-loss this code exists to fix.
fn parse_s3_keys(value: Option<&Value>) -> Result<Vec<String>> {
    match value {
        None => Ok(Vec::new()),
        Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) => items
            .iter()
            .enumerate()
            .map(|(index, item)| {
                item.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| anyhow!("s3_keys[{index}] is not a string: {item}"))
            })
            .collect(),
        Some(other) => bail!("s3_keys is not a JSON array: {other}"),
    }
}

/// Builds and validates the CDN URL for an `s3_keys` entry. Most entries are
/// bare object paths (e.g. `s20/M10/....dem.zip`) with no scheme, which get
/// joined onto the DigitalOcean CDN host. But CSC-Core's Match admin lets
/// tech/admin ops directly edit a match's per-map demo URLs to correct a bad
/// entry (`apps/matches/admin.py::MatchAdminForm.clean_map_demo_urls`), and
/// its save path (`apps/matches/demo_artifacts.py::s3_key_from_url`) stores
/// whatever the admin pasted back verbatim whenever it isn't already
/// DO-CDN-prefixed — including a migrated Backblaze URL, or any other
/// allowlisted CSC archive host. Rejecting every absolute entry outright
/// would fail URL conversion for exactly the corrected matches this s3_keys
/// path exists to fix, silently falling back to the legacy demo_url and
/// recreating the original truncation. So an entry containing a scheme is
/// instead run through the same host/scheme/extension allowlist as
/// `demo_url` itself (`validate_archive_url_labeled`) rather than being
/// joined — that still refuses to redirect the download off an allowlisted
/// CSC archive host, it just also accepts a non-DO one. A bare leading-slash
/// value (neither a scheme-qualified URL nor a joinable relative key) stays
/// rejected.
fn s3_key_to_url(key: &str) -> Result<Url> {
    if key.contains("://") {
        return validate_archive_url_labeled(key, "s3_keys entry");
    }
    if key.starts_with('/') {
        bail!("s3_keys entry is not a bare object key or an absolute URL: {key}");
    }
    let raw = format!("https://{S3_KEYS_CDN_HOST}/{key}");
    validate_archive_url_labeled(&raw, "s3_keys entry")
}

/// Validates `s3_keys` against Core's `scored_map_names` (the map names of
/// the maps Core recorded as actually played, in map-number order) on both
/// count and order:
///
/// - **Count**: a length mismatch either way is treated as terminal rather
///   than guessed at. Too many keys is the known duplicate-upload shape
///   (e.g. a mid-match restart re-uploading a map, match 9077); too few
///   would silently mislabel later maps under earlier positions.
/// - **Order**: count alone cannot catch a same-length reshuffle (match
///   9275's own real shape: keys `[anubis, anubis, nuke]` against played
///   maps `[anubis, nuke, nuke]` — 3 and 3, but position 1 disagrees). Each
///   key must contain its position's map name; a mismatch here is exactly
///   the class of silent per-map mis-assignment this s3_keys path exists to
///   prevent, so it is also terminal rather than best-effort.
fn validate_s3_key_order(s3_keys: &[String], core_match: &CoreMatch) -> Result<()> {
    if s3_keys.len() != core_match.scored_map_names.len() {
        bail!(
            "s3_keys has {} entries but Core records {} scored map(s) ({:?})",
            s3_keys.len(),
            core_match.scored_map_names.len(),
            core_match.scored_map_names,
        );
    }
    for (index, (key, map_name)) in s3_keys
        .iter()
        .zip(core_match.scored_map_names.iter())
        .enumerate()
    {
        // map_name is a nullable column coalesced to "" in the query; an
        // empty needle would make `contains` vacuously true and silently
        // disable the order check for this position, so treat it as a
        // mismatch rather than a pass.
        if map_name.is_empty()
            || !key
                .to_ascii_lowercase()
                .contains(&map_name.to_ascii_lowercase())
        {
            bail!(
                "s3_keys[{index}] ({key}) does not contain Core's map name at that position ({map_name:?})",
            );
        }
    }
    Ok(())
}

async fn download_archive(
    client: &Client,
    url: &Url,
    path: &Path,
    max_bytes: u64,
) -> Result<String> {
    let partial = path.with_extension("partial");
    let response = client.get(url.clone()).send().await?;
    if response.status() != StatusCode::OK {
        bail!("archive download returned {}", response.status());
    }
    if let Some(length) = response.content_length() {
        if length > max_bytes {
            bail!(
                "archive Content-Length {} exceeds limit {}",
                length,
                max_bytes
            );
        }
    }
    let mut file = File::create(&partial).await?;
    let mut stream = response.bytes_stream();
    let mut hash = Sha256::new();
    let mut downloaded = 0_u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        downloaded = downloaded
            .checked_add(chunk.len() as u64)
            .ok_or_else(|| anyhow!("download size overflow"))?;
        if downloaded > max_bytes {
            bail!("archive exceeded download size limit");
        }
        hash.update(&chunk);
        file.write_all(&chunk).await?;
    }
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(&partial, path).await?;
    Ok(hex::encode(hash.finalize()))
}

fn safe_member_path(member: &str) -> bool {
    let path = Path::new(member);
    !path.is_absolute()
        && !path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
}

fn inspect_archive(archive: &Path, max_members: usize, max_expanded_bytes: u64) -> Result<()> {
    let listing = Command::new("timeout")
        .args(["--kill-after=30s", "5m", "7z", "l", "-slt"])
        .arg(archive)
        .output()
        .context("failed to launch timeout/7z")?;
    if !listing.status.success() {
        bail!("7z archive listing failed");
    }
    let text = String::from_utf8_lossy(&listing.stdout);
    let mut in_members = false;
    let mut member_count = 0_usize;
    let mut expanded_bytes = 0_u64;
    for line in text.lines() {
        if line.starts_with("----------") {
            in_members = true;
            continue;
        }
        if !in_members {
            continue;
        }
        if let Some(member) = line.strip_prefix("Path = ") {
            member_count += 1;
            if member_count > max_members {
                bail!("archive member count exceeds limit {max_members}");
            }
            if !safe_member_path(member) {
                bail!("archive contains unsafe member path {member:?}");
            }
        }
        if let Some(size) = line.strip_prefix("Size = ") {
            expanded_bytes = expanded_bytes
                .checked_add(size.parse::<u64>().context("invalid archive member size")?)
                .ok_or_else(|| anyhow!("expanded archive size overflow"))?;
            if expanded_bytes > max_expanded_bytes {
                bail!("expanded archive size exceeds configured limit");
            }
        }
        if line.starts_with("Symbolic Link = ")
            || line.starts_with("Hard Link = ")
            || line.starts_with("Attributes = L")
        {
            bail!("archive contains a symbolic link");
        }
    }
    let test = Command::new("timeout")
        .args(["--kill-after=30s", "30m", "7z", "t"])
        .arg(archive)
        .output()
        .context("failed to launch timeout/7z")?;
    if !test.status.success() {
        bail!(
            "7z archive test failed or timed out: {}",
            String::from_utf8_lossy(&test.stderr).trim()
        );
    }
    Ok(())
}

fn extract_archive(archive: &Path, destination: &Path) -> Result<()> {
    fs::create_dir_all(destination)?;
    let output = Command::new("timeout")
        .args(["--kill-after=30s", "30m", "7z", "x", "-y"])
        .arg(format!("-o{}", destination.display()))
        .arg(archive)
        .output()?;
    if !output.status.success() {
        bail!(
            "7z extraction failed or timed out: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let canonical_destination = fs::canonicalize(destination)?;
    for entry in WalkDir::new(destination).follow_links(false) {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink() || (!metadata.is_file() && !metadata.is_dir()) {
            bail!(
                "extraction produced a link or non-regular entry: {}",
                entry.path().display()
            );
        }
        if !fs::canonicalize(entry.path())?.starts_with(&canonical_destination) {
            bail!("extraction escaped its isolated destination");
        }
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hash = Sha256::new();
    std::io::copy(&mut file, &mut hash)?;
    Ok(hex::encode(hash.finalize()))
}

fn checksum_matched_cached_archive(
    match_root: &Path,
    current_attempt: &Path,
    extension: &str,
    expected_checksum: Option<&str>,
) -> Result<Option<PathBuf>> {
    if expected_checksum.is_none() {
        return Ok(None);
    }
    let expected_name = format!("archive.{extension}");
    for entry in WalkDir::new(match_root).min_depth(2).max_depth(2) {
        let entry = entry?;
        if !entry.file_type().is_file()
            || entry.file_name().to_string_lossy() != expected_name
            || entry.path().starts_with(current_attempt)
        {
            continue;
        }
        let checksum_matches = expected_checksum
            .map(|expected| sha256_file(entry.path()).map(|actual| actual == expected))
            .transpose()?
            .unwrap_or(false);
        if checksum_matches {
            return Ok(Some(entry.path().to_path_buf()));
        }
    }
    Ok(None)
}

fn discover_demos(
    extracted: &Path,
    core_match: &CoreMatch,
    season_match_ids: &HashSet<i64>,
) -> Result<Vec<DemoCandidate>> {
    let embedded_match = Regex::new(r"-mid([0-9]+)-")?;
    let any_suffix = Regex::new(r"-mid([0-9]+)-([0-9]+)(?:_|-)")?;
    let mut paths = Vec::new();
    for entry in WalkDir::new(extracted)
        .follow_links(false)
        .sort_by_file_name()
    {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        let extension = entry
            .path()
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        if !extension.eq_ignore_ascii_case("dem") {
            continue;
        }
        paths.push(entry.path().to_path_buf());
    }
    if paths.is_empty() {
        bail!("archive contains no .dem files (recursive search included demo/ and demos/)");
    }

    if core_match.is_bo3 {
        let suffixed = paths
            .iter()
            .filter(|path| {
                path.file_name()
                    .and_then(|value| value.to_str())
                    .is_some_and(|filename| any_suffix.is_match(filename))
            })
            .count();
        if suffixed != paths.len() {
            if suffixed != 0 {
                bail!("BO3 archive mixes suffixed and unnamed demos; map attribution is ambiguous");
            }
            if paths.len() as i64 != core_match.map_count
                || paths.len() != core_match.played_map_numbers.len()
            {
                bail!(
                    "cannot use Core map order for {} unnamed demos when Core records {} distinct played maps",
                    paths.len(),
                    core_match.map_count,
                );
            }
        }
    }

    let mut demos = Vec::new();
    for (index, path) in paths.into_iter().enumerate() {
        let filename = path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| anyhow!("demo filename is not UTF-8"))?;
        let embedded_id = embedded_match
            .captures(filename)
            .and_then(|captures| captures.get(1))
            .map(|value| value.as_str().parse::<i64>())
            .transpose()?;
        let displaced_match_id = embedded_id.filter(|value| *value != core_match.match_id);
        if displaced_match_id.is_some_and(|value| season_match_ids.contains(&value)) {
            bail!(
                "archive for Core match {} contains demo {} belonging to Core match {} in the same season",
                core_match.match_id,
                filename,
                displaced_match_id.unwrap(),
            );
        }
        let suffix = any_suffix.captures(filename).and_then(|captures| {
            Some((
                captures.get(1)?.as_str().parse::<i64>().ok()?,
                captures.get(2)?.as_str(),
            ))
        });
        let (stats_match_id, identity_source) = if !core_match.is_bo3 {
            (
                core_match.match_id.to_string(),
                if displaced_match_id.is_some() {
                    "core_id_normalized"
                } else {
                    "core_bo1"
                }
                .to_owned(),
            )
        } else if let Some((embedded_id, map_suffix)) = suffix {
            // The archive URL is attached to this exact Core match. Preserve
            // the historical map suffix. A displaced ID is accepted only
            // when it does not identify another Core match in this season.
            (
                format!("{}_{}", core_match.match_id, map_suffix),
                if embedded_id == core_match.match_id {
                    "exact_filename"
                } else {
                    "core_id_normalized"
                }
                .to_owned(),
            )
        } else {
            let map_number = core_match
                .played_map_numbers
                .get(index)
                .copied()
                .ok_or_else(|| {
                    anyhow!(
                        "cannot map unnamed BO3 demo {} to a Core map number",
                        filename
                    )
                })?;
            (
                format!("{}_{}", core_match.match_id, map_number),
                "core_map_order".to_owned(),
            )
        };
        demos.push(DemoCandidate {
            checksum: sha256_file(&path)?,
            relative_path: path.strip_prefix(extracted)?.to_string_lossy().to_string(),
            path,
            stats_match_id,
            identity_source,
            displaced_match_id,
        });
    }
    Ok(demos)
}

/// Discovers the single demo inside one `s3_keys`-derived per-map archive.
/// Map order here comes from the archive's position in the `s3_keys` array
/// (`map_index`, zero-based), not from any digit in the filename — the
/// per-map zip filenames embed a `-{N}_` index that is known to repeat
/// across different maps in some matches (Core-planning#242 item 2) and
/// must not be used for ordering.
///
/// `stats_match_id` is built as `{match_id}_{map_index}`, matching
/// CSC-Stats' own zero-based per-map numbering convention (confirmed
/// against real data: e.g. Core match 8336's live-ingested maps are stored
/// as `8336_0`/`8336_1`, not `8336_1`/`8336_2`) — it is *not* Core's
/// 1-based `matches_matchstats.map_number`. `core_map_name` is carried
/// separately, for human-readable messages only.
///
/// This function intentionally does not run `discover_demos`'s BO3
/// suffix/`core_map_order` logic; it keeps only the cross-match
/// displaced-id guard, which is orthogonal to the unreliable digit and
/// still catches an archive containing another season match's demo.
fn discover_single_map_demo(
    extracted: &Path,
    core_match: &CoreMatch,
    map_index: usize,
    core_map_name: &str,
    season_match_ids: &HashSet<i64>,
) -> Result<DemoCandidate> {
    let embedded_match = Regex::new(r"-mid([0-9]+)-")?;
    let mut paths = Vec::new();
    for entry in WalkDir::new(extracted)
        .follow_links(false)
        .sort_by_file_name()
    {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        let extension = entry
            .path()
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        if extension.eq_ignore_ascii_case("dem") {
            paths.push(entry.path().to_path_buf());
        }
    }
    let path = match paths.len() {
        0 => bail!("s3_keys archive for Core map {core_map_name} contains no .dem files"),
        1 => paths.remove(0),
        count => bail!(
            "s3_keys archive for Core map {core_map_name} contains {count} .dem files; expected exactly 1"
        ),
    };
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("demo filename is not UTF-8"))?;
    let embedded_id = embedded_match
        .captures(filename)
        .and_then(|captures| captures.get(1))
        .map(|value| value.as_str().parse::<i64>())
        .transpose()?;
    let displaced_match_id = embedded_id.filter(|value| *value != core_match.match_id);
    if displaced_match_id.is_some_and(|value| season_match_ids.contains(&value)) {
        bail!(
            "s3_keys archive for Core match {} map {} contains demo {} belonging to Core match {} in the same season",
            core_match.match_id,
            core_map_name,
            filename,
            displaced_match_id.unwrap(),
        );
    }
    Ok(DemoCandidate {
        checksum: sha256_file(&path)?,
        relative_path: path.strip_prefix(extracted)?.to_string_lossy().to_string(),
        path,
        stats_match_id: format!("{}_{}", core_match.match_id, map_index),
        identity_source: "s3_keys_array_order".to_owned(),
        displaced_match_id,
    })
}

fn api_path(args: &BackfillArgs, demo: &Path) -> Result<String> {
    let relative = demo
        .strip_prefix(&args.workspace)
        .context("demo path is not beneath --workspace")?;
    Ok(args
        .api_path_root
        .join(relative)
        .to_string_lossy()
        .to_string())
}

async fn repair_request(
    client: &Client,
    args: &BackfillArgs,
    token: &str,
    core_match: &CoreMatch,
    demo: &DemoCandidate,
    dry_run: bool,
    archive_checksum: &str,
    archive_object_key: &str,
    reviewed: Option<&Value>,
    inventory_checksum: Option<&str>,
) -> Result<Value> {
    let stats_url = env::var("STATS_API_URL").context("STATS_API_URL is required")?;
    let parser_version = args
        .parser_version
        .as_deref()
        .context("--parser-version is required for round-repair mode")?;
    let mut body = json!({
        "path": api_path(args, &demo.path)?,
        "statsMatchId": demo.stats_match_id,
        "matchDate": core_match.match_date,
        "dryRun": dry_run,
        "parserVersion": parser_version,
        "source": {
            "archiveChecksum": archive_checksum,
            "objectKey": archive_object_key,
            "candidateFilename": demo.relative_path,
            "inventoryChecksum": inventory_checksum,
        }
    });
    if !dry_run {
        let reviewed = reviewed.ok_or_else(|| anyhow!("apply requires dry-run evidence"))?;
        let stored = reviewed
            .get("storedFingerprintHash")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("dry-run response omitted storedFingerprintHash"))?;
        let subtree = reviewed
            .get("currentSubtreeHash")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("dry-run response omitted currentSubtreeHash"))?;
        let parser_output = reviewed
            .get("parserOutputChecksum")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("dry-run response omitted parserOutputChecksum"))?;
        let parsed_subtree = reviewed
            .get("parsedSubtreeHash")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("dry-run response omitted parsedSubtreeHash"))?;
        let idempotency_key = hex::encode(Sha256::digest(format!(
            "v3\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}",
            demo.stats_match_id,
            demo.checksum,
            parser_version,
            core_match.match_date,
            stored,
            subtree,
            parser_output,
            parsed_subtree,
            inventory_checksum.unwrap_or_default(),
        )));
        body["expectedDemoChecksum"] = json!(demo.checksum);
        body["expectedParserOutputChecksum"] = json!(parser_output);
        body["expectedParsedSubtreeHash"] = json!(parsed_subtree);
        body["expectedStoredFingerprintHash"] = json!(stored);
        body["expectedCurrentSubtreeHash"] = json!(subtree);
        body["idempotencyKey"] = json!(idempotency_key);
    }
    let response = client
        .post(format!(
            "{}/api/repair-round-stats",
            stats_url.trim_end_matches('/')
        ))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let value: Value = response
        .json()
        .await
        .context("Stats repair endpoint returned non-JSON")?;
    if !status.is_success() {
        bail!("Stats repair endpoint returned {status}: {value}");
    }
    Ok(value)
}

/// Computes the add-match `matchId`/`matchType` for a discovered demo. For
/// combines this applies the `combines-{id}` prefix that CSC-Stats'
/// `handle-add-match.ts` expects to distinguish combine imports from league
/// match imports (the same convention main.rs's single-file import path
/// already applies), and a "Combine" matchType distinct from
/// Regulation/Playoff.
fn full_import_identity(
    args: &BackfillArgs,
    core_match: &CoreMatch,
    stats_match_id: &str,
) -> (String, &'static str) {
    if args.combines {
        (format!("combines-{stats_match_id}"), "Combine")
    } else {
        (
            stats_match_id.to_owned(),
            if core_match.is_bo3 {
                "Playoff"
            } else {
                "Regulation"
            },
        )
    }
}

async fn full_import_request(
    client: &Client,
    args: &BackfillArgs,
    token: &str,
    core_match: &CoreMatch,
    demo: &DemoCandidate,
    create_only: bool,
) -> Result<Value> {
    let stats_url = env::var("STATS_API_URL").context("STATS_API_URL is required")?;
    let tier = core_match
        .tier
        .as_deref()
        .ok_or_else(|| anyhow!("Core match {} has no tier", core_match.match_id))?;
    let (match_id, match_type) = full_import_identity(args, core_match, &demo.stats_match_id);
    let body = json!({
        "path": api_path(args, &demo.path)?,
        "matchId": match_id,
        "matchDay": core_match.match_day,
        "matchType": match_type,
        "matchDate": core_match.match_date,
        "season": args.season,
        "tier": tier,
        "traceId": format!(
            "historical-recovery-s{}-core{}-stats{}",
            args.season, core_match.match_id, match_id
        ),
        "createOnly": create_only,
        "fixCoreScores": false,
        "fixTeamNames": false,
    });
    let response = client
        .post(format!("{}/api/add-match", stats_url.trim_end_matches('/')))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await?;
    let status = response.status();
    let response_text = response
        .text()
        .await
        .context("failed to read Stats full-import response")?;
    parse_full_import_response(status, &response_text)
}

fn remove_isolated_directory(root: &Path, path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    let canonical_root = fs::canonicalize(root)?;
    let canonical_path = fs::canonicalize(path)?;
    if canonical_path == canonical_root || !canonical_path.starts_with(&canonical_root) {
        bail!("refusing to remove directory outside configured root");
    }
    fs::remove_dir_all(&canonical_path)?;
    let mut parent = canonical_path.parent().map(Path::to_path_buf);
    while let Some(candidate) = parent {
        if candidate == canonical_root {
            break;
        }
        parent = candidate.parent().map(Path::to_path_buf);
        match fs::remove_dir(&candidate) {
            Ok(()) => {}
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::NotFound | ErrorKind::DirectoryNotEmpty
                ) =>
            {
                break;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// `--full-reparse` path for a match that has a canonical per-map `s3_keys`
/// array that has already passed `validate_s3_key_order`. Downloads and
/// extracts each key as its own archive (array position is the map number
/// — see `discover_single_map_demo`), then feeds every discovered demo
/// through the same add-match reparse call the legacy single-archive
/// full-reparse path uses.
///
/// Any failure (a download 404 — seen on match 9275 and, for an otherwise
/// clean and ordered `s3_keys` array, on season-19 match 8334 — or anything
/// in `inspect_archive`/`extract_archive`/`discover_single_map_demo`) is
/// propagated as an `Err` rather than continuing with a partial set of
/// maps, since reparsing a subset is exactly the bug this path exists to
/// fix. The caller (`process_match`) catches that `Err` and falls back to
/// the legacy `demo_url` path for the match instead of failing it outright,
/// since a validated s3_keys array can still point at artifacts that no
/// longer resolve while the older bundled archive still does.
async fn process_full_reparse_s3_keys(
    args: &BackfillArgs,
    client: &Client,
    token: &str,
    ledger: &mut Ledger,
    core_match: &CoreMatch,
    season_match_ids: &HashSet<i64>,
    s3_keys: &[String],
) -> Result<()> {
    let match_root = args
        .workspace
        .join(format!("s{}", args.season))
        .join(core_match.match_id.to_string());
    fs::create_dir_all(&match_root)?;
    let attempt_name = format!(
        "attempt-{}-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
        std::process::id(),
    );
    let match_workspace = match_root.join(attempt_name);
    fs::create_dir(&match_workspace)?;
    let mut attempt_workspace =
        AttemptWorkspace::new(&args.workspace, match_workspace.clone(), args.keep_all);

    let mut demos = Vec::new();
    let mut archive_evidence = Vec::new();
    for (map_index, key) in s3_keys.iter().enumerate() {
        // validate_s3_key_order already proved core_match.scored_map_names
        // has the same length as s3_keys and that this key contains this
        // position's map name.
        let map_name = &core_match.scored_map_names[map_index];
        // stats_match_id (built inside discover_single_map_demo) uses
        // map_index — CSC-Stats' own zero-based per-map numbering, matching
        // its live-ingest convention (e.g. Core match 8336's maps are
        // stored as 8336_0/8336_1, not 8336_1/8336_2).
        let stats_match_id = format!("{}_{}", core_match.match_id, map_index);
        let url = s3_key_to_url(key)
            .with_context(|| format!("map {map_index} ({map_name}, s3_keys entry {key})"))?;
        let extension = if url.path().to_ascii_lowercase().ends_with(".zip") {
            "zip"
        } else {
            "7z"
        };
        let map_workspace = match_workspace.join(format!("map-{map_index}"));
        fs::create_dir(&map_workspace)?;
        let archive_path = map_workspace.join(format!("archive.{extension}"));
        ledger.append(event(
            args,
            core_match.match_id,
            "downloading",
            Some(stats_match_id.clone()),
            Some(key.clone()),
            None,
        ))?;
        let archive_checksum = download_archive(
            client,
            &url,
            &archive_path,
            args.max_archive_gib.saturating_mul(1024 * 1024 * 1024),
        )
        .await
        .with_context(|| format!("downloading map {map_index} ({map_name}, {key})"))?;
        ledger.append(event(
            args,
            core_match.match_id,
            "archive_cached",
            Some(stats_match_id.clone()),
            Some(archive_path.to_string_lossy().to_string()),
            Some(json!({
                "archiveChecksum": archive_checksum,
                "objectKey": key,
                "mapIndex": map_index,
                "coreMapName": map_name,
            })),
        ))?;
        inspect_archive(
            &archive_path,
            args.max_archive_members,
            args.max_extracted_gib.saturating_mul(1024 * 1024 * 1024),
        )?;
        let extracted = map_workspace.join("extracted");
        extract_archive(&archive_path, &extracted)?;
        let demo = discover_single_map_demo(
            &extracted,
            core_match,
            map_index,
            map_name,
            season_match_ids,
        )?;
        debug_assert_eq!(demo.stats_match_id, stats_match_id);
        archive_evidence.push(json!({
            "mapIndex": map_index,
            "coreMapName": map_name,
            "objectKey": key,
            "archiveChecksum": archive_checksum,
        }));
        demos.push(demo);
    }

    if demos.is_empty() {
        ledger.append(event(
            args,
            core_match.match_id,
            "skipped_not_repairable",
            None,
            Some("no demos discovered across s3_keys archives".to_owned()),
            None,
        ))?;
        attempt_workspace.finish(args.keep_successful || args.keep_all)?;
        return Ok(());
    }

    for demo in &demos {
        if args.writes() {
            let response =
                full_import_request(client, args, token, core_match, demo, false).await?;
            ledger.append(event(
                args,
                core_match.match_id,
                "full_reparse_applied",
                Some(demo.stats_match_id.clone()),
                None,
                Some(json!({
                    "demo": demo.relative_path,
                    "demoChecksum": demo.checksum,
                    "identitySource": demo.identity_source,
                    "displacedMatchId": demo.displaced_match_id,
                    "result": response,
                })),
            ))?;
        } else {
            ledger.append(event(
                args,
                core_match.match_id,
                "full_reparse_planned",
                Some(demo.stats_match_id.clone()),
                None,
                Some(json!({
                    "demo": demo.relative_path,
                    "demoChecksum": demo.checksum,
                    "identitySource": demo.identity_source,
                    "displacedMatchId": demo.displaced_match_id,
                })),
            ))?;
        }
    }

    ledger.append(event(
        args,
        core_match.match_id,
        "match_complete",
        None,
        Some(format!(
            "{} map(s) {}",
            demos.len(),
            if args.writes() {
                "reparsed"
            } else {
                "validated for full reparse"
            }
        )),
        Some(json!({
            "archives": archive_evidence,
            "targets": demos.iter().map(|d| d.stats_match_id.clone()).collect::<Vec<_>>(),
        })),
    ))?;
    attempt_workspace.finish(args.keep_successful || args.keep_all)?;
    Ok(())
}

async fn process_match(
    args: &BackfillArgs,
    client: &Client,
    token: &str,
    ledger: &mut Ledger,
    core_match: &CoreMatch,
    season_match_ids: &HashSet<i64>,
    reviewed_inventory: Option<&ReviewedInventory>,
    cached_source_inventory: Option<&CachedSourceInventory>,
) -> Result<()> {
    if core_match.marked_forfeit || core_match.legacy_one_zero || core_match.has_forfeit_audit {
        verify_reviewed_terminal(reviewed_inventory, core_match.match_id, "skipped_forfeit")?;
        ledger.append(event(
            args,
            core_match.match_id,
            "skipped_forfeit",
            None,
            Some("Core score/forfeit history marks this as a forfeit".to_owned()),
            None,
        ))?;
        return Ok(());
    }
    // --full-reparse only, and BO3 only: prefer the canonical per-map
    // s3_keys array over the legacy single-map demo_url when it is present
    // *and* validates cleanly against Core's scored maps. This is gated to
    // full-reparse because round-repair's reviewed-ledger/idempotency-key
    // machinery (ReviewedInventory.archive_checksums, repair_request's v3
    // idempotency key) is keyed on exactly one archive per match; extending
    // it to N archives would need a reviewed-ledger schema change and
    // invalidate every previously-reviewed ledger. full-reparse has no such
    // attestation flow (add-match reparses are safe to rerun), so it can
    // take the multi-archive path safely.
    //
    // It is also gated to is_bo3: CSC-Core populates a one-entry s3_keys
    // array for BO1s too, but discover_single_map_demo always builds
    // stats_match_id as `{match_id}_{map_index}` — the BO3 suffix
    // convention. Taking this path for a BO1 would import under the wrong
    // (suffixed) stats match id, creating a new record while leaving the
    // real unsuffixed BO1 record stale. BO1s were never truncated by the
    // original bug (a single demo_url already covers their one map), so
    // there is nothing for this path to fix there — the legacy demo_url
    // path below already handles BO1s correctly.
    //
    // s3_keys is not reliably a *complete* per-map array in practice: most
    // season-19 BO3s have a DemoProcessingStatus row whose s3_keys holds
    // just one key (a single per-map upload alongside the real bundled
    // archive at demo_url) even though 2-3 maps were played, and a handful
    // of season-20 matches have a corrupt count or order (matches 9077 and
    // 9275 — see validate_s3_key_order). Any of those must not turn into a
    // worse outcome than before this code existed, so a validation failure
    // (including a malformed s3_keys value itself) logs a clear
    // (non-terminal) ledger event and falls through to the legacy demo_url
    // path below, same as a match with no s3_keys at all.
    if args.full_reparse && core_match.is_bo3 {
        let s3_keys = match parse_s3_keys(core_match.s3_keys.as_ref()) {
            Ok(keys) => keys,
            Err(error) => {
                ledger.append(event(
                    args,
                    core_match.match_id,
                    "s3_keys_mismatch",
                    None,
                    Some(format!(
                        "s3_keys is malformed: {error:#}; falling back to the legacy demo_url path for this match"
                    )),
                    None,
                ))?;
                Vec::new()
            }
        };
        if !s3_keys.is_empty() {
            match validate_s3_key_order(&s3_keys, core_match) {
                Ok(()) => {
                    // A validated-but-then-unreachable s3_keys archive
                    // (e.g. a 404 from the archival-migration issue seen on
                    // match 9275, or any other download/extract/discover
                    // failure) also falls back rather than failing the
                    // whole match: some season-19 matches with a complete,
                    // ordered s3_keys array (e.g. match 8334) have per-map
                    // zips that 404 while their bundled demo_url archive
                    // still serves fine, so treating this the same as a
                    // validation mismatch avoids regressing a match that
                    // reparses correctly today.
                    match process_full_reparse_s3_keys(
                        args,
                        client,
                        token,
                        ledger,
                        core_match,
                        season_match_ids,
                        &s3_keys,
                    )
                    .await
                    {
                        Ok(()) => return Ok(()),
                        Err(error) => {
                            ledger.append(event(
                                args,
                                core_match.match_id,
                                "s3_keys_mismatch",
                                None,
                                Some(format!(
                                    "s3_keys archive(s) unusable ({error:#}); falling back to the legacy demo_url path for this match"
                                )),
                                None,
                            ))?;
                        }
                    }
                }
                Err(error) => {
                    ledger.append(event(
                        args,
                        core_match.match_id,
                        "s3_keys_mismatch",
                        None,
                        Some(format!(
                            "{error:#}; falling back to the legacy demo_url path for this match"
                        )),
                        Some(json!({
                            "s3KeysCount": s3_keys.len(),
                            "coreScoredMapNames": core_match.scored_map_names,
                            "s3Keys": s3_keys,
                        })),
                    ))?;
                }
            }
        }
    }
    let Some(raw_url) = &core_match.demo_url else {
        verify_reviewed_terminal(reviewed_inventory, core_match.match_id, "artifact_missing")?;
        ledger.append(event(
            args,
            core_match.match_id,
            "artifact_missing",
            None,
            Some("Core match has no demo_url".to_owned()),
            None,
        ))?;
        return Ok(());
    };
    let url = match validate_archive_url(raw_url) {
        Ok(url) => url,
        Err(error) => {
            verify_reviewed_terminal(
                reviewed_inventory,
                core_match.match_id,
                "artifact_unsupported",
            )?;
            ledger.append(event(
                args,
                core_match.match_id,
                "artifact_unsupported",
                None,
                Some(error.to_string()),
                None,
            ))?;
            return Ok(());
        }
    };
    // matches_combinematches.id and matches_matches.id are independent
    // sequences and can collide numerically; the "combines-" prefix keeps
    // per-match workspace/archive-cache directories from colliding when a
    // combine and a league match in the same season share a raw id.
    let match_root = args
        .workspace
        .join(format!("s{}", args.season))
        .join(if args.combines {
            format!("combines-{}", core_match.match_id)
        } else {
            core_match.match_id.to_string()
        });
    fs::create_dir_all(&match_root)?;
    let attempt_name = format!(
        "attempt-{}-{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
        std::process::id(),
    );
    let match_workspace = match_root.join(attempt_name);
    fs::create_dir(&match_workspace)?;
    let mut attempt_workspace =
        AttemptWorkspace::new(&args.workspace, match_workspace.clone(), args.keep_all);
    let extension = if url.path().to_ascii_lowercase().ends_with(".zip") {
        "zip"
    } else {
        "7z"
    };
    let expected_archive_checksum = reviewed_inventory
        .and_then(|inventory| {
            inventory
                .archive_checksums
                .get(&core_match.match_id)
                .map(String::as_str)
        })
        .or_else(|| {
            cached_source_inventory.and_then(|inventory| {
                inventory
                    .archive_checksums
                    .get(&core_match.match_id)
                    .map(String::as_str)
            })
        });
    let cached_archive = checksum_matched_cached_archive(
        &match_root,
        &match_workspace,
        extension,
        expected_archive_checksum,
    )?;
    let (archive_path, archive_checksum) = if let Some(cached_archive) = cached_archive {
        let checksum = sha256_file(&cached_archive)?;
        let evidence = if let Some(source) = cached_source_inventory {
            json!({
                "archiveChecksum": checksum,
                "sourceInventoryChecksum": source.checksum,
            })
        } else {
            json!({ "archiveChecksum": checksum })
        };
        ledger.append(event(
            args,
            core_match.match_id,
            "using_cached_archive",
            None,
            Some(cached_archive.to_string_lossy().to_string()),
            Some(evidence),
        ))?;
        (cached_archive, checksum)
    } else {
        let archive_path = match_workspace.join(format!("archive.{extension}"));
        ledger.append(event(
            args,
            core_match.match_id,
            "downloading",
            None,
            None,
            None,
        ))?;
        let checksum = download_archive(
            client,
            &url,
            &archive_path,
            args.max_archive_gib.saturating_mul(1024 * 1024 * 1024),
        )
        .await?;
        ledger.append(event(
            args,
            core_match.match_id,
            "archive_cached",
            None,
            Some(archive_path.to_string_lossy().to_string()),
            Some(json!({
                "archiveChecksum": checksum,
                "objectKey": url.path(),
            })),
        ))?;
        (archive_path, checksum)
    };
    if let Some(reviewed) = reviewed_inventory {
        if reviewed
            .archive_checksums
            .get(&core_match.match_id)
            .map(String::as_str)
            != Some(archive_checksum.as_str())
        {
            bail!("archive checksum differs from reviewed inventory");
        }
    }
    inspect_archive(
        &archive_path,
        args.max_archive_members,
        args.max_extracted_gib.saturating_mul(1024 * 1024 * 1024),
    )?;
    let extracted = match_workspace.join("extracted");
    extract_archive(&archive_path, &extracted)?;
    let demos = discover_demos(&extracted, core_match, season_match_ids)?;

    if core_match.map_count > 0 && (demos.len() as i64) < core_match.map_count {
        ledger.append(event(
            args,
            core_match.match_id,
            "partial_archive",
            None,
            Some(format!(
                "archive contains {} demos while Core records {} played maps; processing available demos",
                demos.len(), core_match.map_count
            )),
            Some(json!({
                "availableDemos": demos.len(),
                "corePlayedMaps": core_match.map_count,
                "coreMapNumbers": core_match.played_map_numbers,
            })),
        ))?;
    }

    if args.full_reparse || args.combines {
        if demos.is_empty() {
            ledger.append(event(
                args,
                core_match.match_id,
                "skipped_not_repairable",
                None,
                Some("no demos discovered in archive".to_owned()),
                Some(json!({ "archiveChecksum": archive_checksum })),
            ))?;
            attempt_workspace.finish(args.keep_successful || args.keep_all)?;
            return Ok(());
        }
        for demo in &demos {
            if args.writes() {
                let response =
                    full_import_request(client, args, token, core_match, demo, false).await?;
                ledger.append(event(
                    args,
                    core_match.match_id,
                    "full_reparse_applied",
                    Some(demo.stats_match_id.clone()),
                    None,
                    Some(json!({
                        "demo": demo.relative_path,
                        "demoChecksum": demo.checksum,
                        "identitySource": demo.identity_source,
                        "displacedMatchId": demo.displaced_match_id,
                        "result": response,
                    })),
                ))?;
            } else {
                ledger.append(event(
                    args,
                    core_match.match_id,
                    "full_reparse_planned",
                    Some(demo.stats_match_id.clone()),
                    None,
                    Some(json!({
                        "demo": demo.relative_path,
                        "demoChecksum": demo.checksum,
                        "identitySource": demo.identity_source,
                        "displacedMatchId": demo.displaced_match_id,
                    })),
                ))?;
            }
        }
        ledger.append(event(
            args,
            core_match.match_id,
            "match_complete",
            None,
            Some(format!(
                "{} map(s) {}",
                demos.len(),
                if args.writes() {
                    "reparsed"
                } else {
                    "validated for full reparse"
                }
            )),
            Some(json!({
                "archiveChecksum": archive_checksum,
                "targets": demos.iter().map(|d| d.stats_match_id.clone()).collect::<Vec<_>>(),
            })),
        ))?;
        attempt_workspace.finish(args.keep_successful || args.keep_all)?;
        return Ok(());
    }

    let mut validations = Vec::new();
    for demo in demos {
        let response = repair_request(
            client,
            args,
            token,
            core_match,
            &demo,
            true,
            &archive_checksum,
            url.path(),
            None,
            None,
        )
        .await?;
        ledger.append(event(
            args,
            core_match.match_id,
            "demo_validated",
            Some(demo.stats_match_id.clone()),
            None,
            Some(json!({
                "demo": demo.relative_path,
                "demoChecksum": demo.checksum,
                "identitySource": demo.identity_source,
                "displacedMatchId": demo.displaced_match_id,
                "coreMatch": {
                    "matchId": core_match.match_id,
                    "season": args.season,
                    "matchDay": core_match.match_day,
                    "tier": core_match.tier,
                    "isBo3": core_match.is_bo3,
                },
                "result": response,
            })),
        ))?;
        validations.push(Validation {
            candidate: demo,
            response,
        });
    }

    let mut ready_by_target: HashMap<String, Vec<&Validation>> = HashMap::new();
    let mut importable_by_target: HashMap<String, Vec<&Validation>> = HashMap::new();
    for validation in &validations {
        let classification = validation
            .response
            .get("classification")
            .and_then(Value::as_str);
        match classification {
            Some("ready") => ready_by_target
                .entry(validation.candidate.stats_match_id.clone())
                .or_default()
                .push(validation),
            Some("no_matching_candidate") => importable_by_target
                .entry(validation.candidate.stats_match_id.clone())
                .or_default()
                .push(validation),
            _ => {}
        }
    }
    let targets: HashSet<_> = validations
        .iter()
        .map(|item| item.candidate.stats_match_id.clone())
        .collect();
    let mut ordered_targets = targets.iter().cloned().collect::<Vec<_>>();
    ordered_targets.sort();
    if let Some(reviewed) = reviewed_inventory {
        let current = validations
            .iter()
            .filter(|item| {
                item.response.get("classification").and_then(Value::as_str) == Some("ready")
            })
            .map(|item| {
                (
                    item.candidate.stats_match_id.clone(),
                    item.candidate.checksum.clone(),
                )
            })
            .collect::<HashSet<_>>();
        let reviewed_ready = reviewed
            .ready_sets
            .get(&core_match.match_id)
            .cloned()
            .unwrap_or_default();
        if reviewed_ready != current {
            bail!("current ready candidate set differs from reviewed inventory");
        }
        let current_importable = validations
            .iter()
            .filter(|item| {
                item.response.get("classification").and_then(Value::as_str)
                    == Some("no_matching_candidate")
            })
            .map(|item| {
                (
                    item.candidate.stats_match_id.clone(),
                    item.candidate.checksum.clone(),
                )
            })
            .collect::<HashSet<_>>();
        let reviewed_importable = reviewed
            .importable_sets
            .get(&core_match.match_id)
            .cloned()
            .unwrap_or_default();
        if reviewed_importable != current_importable {
            bail!("current missing-match import set differs from reviewed inventory");
        }
    }
    let mut skipped_targets = Vec::new();
    for target in &ordered_targets {
        let ready_count = ready_by_target.get(target).map(Vec::len).unwrap_or(0);
        let import_count = importable_by_target.get(target).map(Vec::len).unwrap_or(0);
        match ready_count + import_count {
            1 => {}
            0 => {
                let candidates = validations
                    .iter()
                    .filter(|item| item.candidate.stats_match_id == *target)
                    .collect::<Vec<_>>();
                let mut classifications = candidates
                    .iter()
                    .filter_map(|item| {
                        item.response
                            .get("classification")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    })
                    .collect::<Vec<_>>();
                classifications.sort();
                classifications.dedup();
                if candidates.is_empty()
                    || !candidates.iter().all(|item| {
                        is_clean_non_repairable(
                            item.response.get("classification").and_then(Value::as_str),
                        )
                    })
                {
                    bail!("Stats map {target} has no ready or clean non-repairable verdict");
                }
                skipped_targets.push(json!({
                    "statsMatchId": target,
                    "classifications": classifications,
                }));
            }
            count => {
                bail!("{count} demos fingerprint the same Stats map {target}; source is ambiguous")
            }
        }
    }

    if ready_by_target.is_empty() && importable_by_target.is_empty() {
        verify_reviewed_terminal(
            reviewed_inventory,
            core_match.match_id,
            "skipped_not_repairable",
        )?;
        ledger.append(event(
            args,
            core_match.match_id,
            "skipped_not_repairable",
            None,
            Some("all discovered maps received clean non-repairable verdicts".to_owned()),
            Some(json!({
                "archiveChecksum": archive_checksum,
                "targets": skipped_targets,
            })),
        ))?;
        attempt_workspace.finish(args.keep_successful || args.keep_all)?;
        return Ok(());
    }

    if args.writes() {
        for target in &ordered_targets {
            let Some(validations) = ready_by_target.get(target) else {
                continue;
            };
            let validation = validations[0];
            let apply_evidence = if let Some(inventory) = reviewed_inventory {
                let reviewed = inventory
                    .ready
                    .get(&(
                        core_match.match_id,
                        validation.candidate.stats_match_id.clone(),
                        validation.candidate.checksum.clone(),
                    ))
                    .ok_or_else(|| {
                        anyhow!(
                            "ready candidate {} is absent from the reviewed dry-run inventory",
                            validation.candidate.stats_match_id,
                        )
                    })?;
                for field in [
                    "sourceChecksum",
                    "parserOutputChecksum",
                    "parserVersion",
                    "matchDate",
                    "storedFingerprintHash",
                    "parsedSubtreeHash",
                ] {
                    if reviewed.get(field) != validation.response.get(field) {
                        bail!("current validation differs from reviewed inventory at {field}");
                    }
                }
                let current_subtree = validation.response.get("currentSubtreeHash");
                if current_subtree != reviewed.get("currentSubtreeHash")
                    && current_subtree != reviewed.get("parsedSubtreeHash")
                {
                    bail!("current subtree is neither the reviewed before-state nor verified repaired state");
                }
                reviewed
            } else {
                &validation.response
            };
            let response = repair_request(
                client,
                args,
                token,
                core_match,
                &validation.candidate,
                false,
                &archive_checksum,
                url.path(),
                Some(apply_evidence),
                reviewed_inventory.map(|inventory| inventory.checksum.as_str()),
            )
            .await?;
            let classification = response
                .get("classification")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            if !matches!(classification, "repaired" | "already_verified") {
                bail!("apply returned non-terminal classification {classification}");
            }
            ledger.append(event(
                args,
                core_match.match_id,
                "demo_repaired",
                Some(validation.candidate.stats_match_id.clone()),
                None,
                Some(response),
            ))?;
        }
        for target in &ordered_targets {
            let Some(validations) = importable_by_target.get(target) else {
                continue;
            };
            let validation = validations[0];
            if let Some(inventory) = reviewed_inventory {
                let reviewed = inventory
                    .importable
                    .get(&(
                        core_match.match_id,
                        validation.candidate.stats_match_id.clone(),
                        validation.candidate.checksum.clone(),
                    ))
                    .ok_or_else(|| {
                        anyhow!(
                        "missing-match candidate {} is absent from the reviewed dry-run inventory",
                        validation.candidate.stats_match_id,
                    )
                    })?;
                verify_reviewed_import(reviewed, &validation.response)?;
            }
            let response =
                full_import_request(client, args, token, core_match, &validation.candidate, true)
                    .await?;
            ledger.append(event(
                args,
                core_match.match_id,
                "demo_imported",
                Some(validation.candidate.stats_match_id.clone()),
                None,
                Some(response),
            ))?;
        }
    }

    ledger.append(event(
        args,
        core_match.match_id,
        "match_complete",
        None,
        Some(format!(
            "{} repair candidate(s) {}, {} missing map(s) {}, {} map(s) skipped as not repairable",
            ready_by_target.len(),
            if args.writes() {
                "repaired"
            } else {
                "validated"
            },
            importable_by_target.len(),
            if args.writes() {
                "imported"
            } else {
                "validated for create-only import"
            },
            skipped_targets.len()
        )),
        Some(json!({
            "archiveChecksum": archive_checksum,
            "skippedTargets": skipped_targets,
            "repairTargets": ready_by_target.keys().collect::<Vec<_>>(),
            "importTargets": importable_by_target.keys().collect::<Vec<_>>(),
        })),
    ))?;
    attempt_workspace.finish(args.keep_successful || args.keep_all)?;
    Ok(())
}

pub async fn run(args: BackfillArgs) -> Result<()> {
    if args.season <= 0 {
        bail!("--season must be positive");
    }
    if args.writes() && args.confirm_season != Some(args.season) {
        bail!(
            "--apply, --direct-apply, --full-reparse, and --combines require --confirm-season {}",
            args.season
        );
    }
    if !args.writes() && args.confirm_season.is_some() {
        bail!(
            "--confirm-season is only valid with --apply, --direct-apply, --full-reparse, or --combines"
        );
    }
    let reviewed_inventory = ReviewedInventory::load(&args)?;
    let cached_source_inventory = CachedSourceInventory::load(&args)?;
    if args.max_archive_gib == 0 || args.max_extracted_gib == 0 || args.max_archive_members == 0 {
        bail!("archive size/member limits must be positive");
    }
    Command::new("7z")
        .arg("i")
        .output()
        .context("7z is required")?;
    fs::create_dir_all(&args.workspace)?;
    let _workspace_lock = WorkspaceLock::acquire(&args.workspace)?;
    let ledger_path = args.ledger.clone().unwrap_or_else(|| {
        args.workspace
            .join(format!("season-{}-{}.jsonl", args.season, args.mode()))
    });
    if let Some(reviewed_path) = &args.reviewed_ledger {
        if fs::canonicalize(reviewed_path)? == canonical_output_path(&ledger_path)? {
            bail!("--ledger must not overwrite the immutable --reviewed-ledger");
        }
    }
    if let Some(source_path) = &args.cached_source_ledger {
        if fs::canonicalize(source_path)? == canonical_output_path(&ledger_path)? {
            bail!("--ledger must not overwrite the immutable --cached-source-ledger");
        }
    }
    let mut ledger = Ledger::open(ledger_path)?;
    let token = env::var("STATS_REPAIR_TOKEN").context("STATS_REPAIR_TOKEN is required")?;
    let database_url = env::var("DATABASE_URL").context("DATABASE_URL (Core DB) is required")?;
    let pool = PgPool::connect(&database_url).await?;
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(30 * 60))
        // Allowlisted archive URLs and the internal Stats endpoint are direct.
        // Refusing redirects prevents a trusted URL from redirecting a repair
        // run to an unreviewed host.
        .redirect(reqwest::redirect::Policy::none())
        .user_agent("csc-stats-historical-round-repair/1")
        .build()?;
    let mode = args.mode();
    let selected: HashSet<i64> = args.match_id.iter().copied().collect();
    let matches = if args.combines {
        combine_season_matches(&pool, args.season).await?
    } else {
        season_matches(&pool, args.season).await?
    };
    let available: HashSet<i64> = matches.iter().map(|item| item.match_id).collect();
    let mut missing_selected = selected.difference(&available).copied().collect::<Vec<_>>();
    missing_selected.sort_unstable();
    if !missing_selected.is_empty() {
        bail!(
            "--match-id values are not in season {}: {:?}",
            args.season,
            missing_selected
        );
    }
    if let Some(inventory) = &reviewed_inventory {
        // Must match the same is_selected() filter the processing loop below
        // uses, not the full unfiltered `available` set — otherwise a
        // filtered dry run (e.g. --bo3-only with no --match-id) never
        // produces terminal ledger entries for the matches it skipped, and
        // this check would then demand review coverage the dry run never
        // could have produced, failing every apply run before it starts.
        let required_review: HashSet<i64> = matches
            .iter()
            .filter(|item| is_selected(item, args.bo3_only, &selected))
            .map(|item| item.match_id)
            .collect();
        let missing_review = required_review
            .difference(&inventory.terminal_matches)
            .copied()
            .collect::<Vec<_>>();
        if !missing_review.is_empty() {
            bail!(
                "reviewed dry-run inventory is incomplete: {} season match(es) lack a terminal classification",
                missing_review.len()
            );
        }
    }
    println!(
        "Season {}: {} Core matches found{}; mode={mode}; concurrency=1",
        args.season,
        matches.len(),
        if args.bo3_only {
            format!(
                " ({} BO3)",
                matches.iter().filter(|item| item.is_bo3).count()
            )
        } else {
            String::new()
        }
    );
    let mut processed = 0_usize;
    let mut failures = 0_usize;
    for core_match in matches {
        if !is_selected(&core_match, args.bo3_only, &selected) {
            continue;
        }
        if ledger.is_complete(args.season, mode, core_match.match_id) {
            println!("[match {}] resumed: already complete", core_match.match_id);
            continue;
        }
        if args.limit.is_some_and(|limit| processed >= limit) {
            break;
        }
        processed += 1;
        ledger.append(event(
            &args,
            core_match.match_id,
            "match_started",
            None,
            None,
            None,
        ))?;
        if let Err(error) = process_match(
            &args,
            &client,
            &token,
            &mut ledger,
            &core_match,
            &available,
            reviewed_inventory.as_ref(),
            cached_source_inventory.as_ref(),
        )
        .await
        {
            failures += 1;
            ledger.append(event(
                &args,
                core_match.match_id,
                "match_failed",
                None,
                Some(format!("{error:#}")),
                None,
            ))?;
        }
        if args.pause_seconds > 0 {
            sleep(Duration::from_secs(args.pause_seconds)).await;
        }
    }
    println!(
        "Season {} finished: processed={}, failed={}, mode={mode}",
        args.season, processed, failures
    );
    if failures > 0 {
        bail!("{} match(es) failed; see the JSONL ledger", failures);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{Args as ClapArgs, Command, FromArgMatches};

    fn test_path(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "stats-importer-{label}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn ledger_event(status: &str) -> LedgerEvent {
        LedgerEvent {
            schema_version: 1,
            timestamp_unix: 1,
            season: 18,
            mode: "dry-run".to_owned(),
            match_id: 123,
            status: status.to_owned(),
            stats_match_id: None,
            message: None,
            evidence: None,
        }
    }

    fn core_match(id: i64, is_bo3: bool) -> CoreMatch {
        CoreMatch {
            match_id: id,
            is_bo3,
            demo_url: None,
            map_count: 1,
            played_map_numbers: vec![1],
            match_day: "M01".to_owned(),
            match_date: "2023-02-17T03:00:00.000000Z".to_owned(),
            tier: Some("Elite".to_owned()),
            marked_forfeit: false,
            legacy_one_zero: false,
            has_forfeit_audit: false,
            scored_map_names: Vec::new(),
            s3_keys: None,
        }
    }

    fn backfill_args(workspace: &Path) -> BackfillArgs {
        BackfillArgs {
            season: 18,
            apply: false,
            direct_apply: false,
            full_reparse: false,
            combines: false,
            confirm_season: None,
            parser_version: Some("test-parser".to_owned()),
            workspace: workspace.to_path_buf(),
            api_path_root: workspace.to_path_buf(),
            ledger: None,
            reviewed_ledger: None,
            reviewed_ledger_sha256: None,
            cached_source_ledger: None,
            cached_source_ledger_sha256: None,
            pause_seconds: 0,
            limit: None,
            match_id: Vec::new(),
            bo3_only: false,
            keep_successful: false,
            keep_all: false,
            max_archive_gib: 8,
            max_extracted_gib: 32,
            max_archive_members: 100,
        }
    }

    #[test]
    fn direct_apply_is_explicit_and_supports_checksum_bound_cache_reuse() {
        let command = BackfillArgs::augment_args(Command::new("backfill"));
        let matches = command
            .try_get_matches_from([
                "backfill",
                "--season",
                "12",
                "--direct-apply",
                "--confirm-season",
                "12",
                "--parser-version",
                "test-parser",
                "--api-path-root",
                "/round-repair-work",
                "--cached-source-ledger",
                "/tmp/source.jsonl",
                "--cached-source-ledger-sha256",
                "checksum",
            ])
            .unwrap();
        let args = BackfillArgs::from_arg_matches(&matches).unwrap();
        assert!(args.writes());
        assert_eq!(args.mode(), "direct-apply");
        assert!(args.cached_source_ledger.is_some());
    }

    #[test]
    fn direct_apply_conflicts_with_reviewed_apply() {
        let command = BackfillArgs::augment_args(Command::new("backfill"));
        assert!(command
            .try_get_matches_from([
                "backfill",
                "--season",
                "12",
                "--apply",
                "--direct-apply",
                "--confirm-season",
                "12",
                "--parser-version",
                "test-parser",
                "--api-path-root",
                "/round-repair-work",
            ])
            .is_err());
    }

    #[test]
    fn full_reparse_does_not_require_parser_version_and_defaults_to_dry_run() {
        let command = BackfillArgs::augment_args(Command::new("backfill"));
        let matches = command
            .try_get_matches_from([
                "backfill",
                "--season",
                "12",
                "--full-reparse",
                "--api-path-root",
                "/round-repair-work",
            ])
            .unwrap();
        let args = BackfillArgs::from_arg_matches(&matches).unwrap();
        assert!(args.parser_version.is_none());
        assert!(!args.writes());
        assert_eq!(args.mode(), "full-reparse-dry-run");
    }

    #[test]
    fn full_reparse_writes_only_once_confirm_season_is_supplied() {
        let command = BackfillArgs::augment_args(Command::new("backfill"));
        let matches = command
            .try_get_matches_from([
                "backfill",
                "--season",
                "12",
                "--full-reparse",
                "--confirm-season",
                "12",
                "--api-path-root",
                "/round-repair-work",
            ])
            .unwrap();
        let args = BackfillArgs::from_arg_matches(&matches).unwrap();
        assert!(args.writes());
        assert_eq!(args.mode(), "full-reparse");
    }

    #[test]
    fn full_reparse_conflicts_with_round_repair_modes() {
        let command = BackfillArgs::augment_args(Command::new("backfill"));
        assert!(command
            .clone()
            .try_get_matches_from([
                "backfill",
                "--season",
                "12",
                "--full-reparse",
                "--apply",
                "--api-path-root",
                "/round-repair-work",
            ])
            .is_err());
        assert!(command
            .try_get_matches_from([
                "backfill",
                "--season",
                "12",
                "--full-reparse",
                "--direct-apply",
                "--api-path-root",
                "/round-repair-work",
            ])
            .is_err());
    }

    #[test]
    fn combines_does_not_require_parser_version_and_defaults_to_dry_run() {
        let command = BackfillArgs::augment_args(Command::new("backfill"));
        let matches = command
            .try_get_matches_from([
                "backfill",
                "--season",
                "12",
                "--combines",
                "--api-path-root",
                "/round-repair-work",
            ])
            .unwrap();
        let args = BackfillArgs::from_arg_matches(&matches).unwrap();
        assert!(args.parser_version.is_none());
        assert!(!args.writes());
        assert_eq!(args.mode(), "combines-full-reparse-dry-run");
    }

    #[test]
    fn combines_writes_only_once_confirm_season_is_supplied() {
        let command = BackfillArgs::augment_args(Command::new("backfill"));
        let matches = command
            .try_get_matches_from([
                "backfill",
                "--season",
                "12",
                "--combines",
                "--confirm-season",
                "12",
                "--api-path-root",
                "/round-repair-work",
            ])
            .unwrap();
        let args = BackfillArgs::from_arg_matches(&matches).unwrap();
        assert!(args.writes());
        assert_eq!(args.mode(), "combines-full-reparse");
    }

    #[test]
    fn combines_conflicts_with_full_reparse_and_round_repair_modes() {
        let command = BackfillArgs::augment_args(Command::new("backfill"));
        assert!(command
            .clone()
            .try_get_matches_from([
                "backfill",
                "--season",
                "12",
                "--combines",
                "--full-reparse",
                "--api-path-root",
                "/round-repair-work",
            ])
            .is_err());
        assert!(command
            .clone()
            .try_get_matches_from([
                "backfill",
                "--season",
                "12",
                "--combines",
                "--apply",
                "--api-path-root",
                "/round-repair-work",
            ])
            .is_err());
        assert!(command
            .try_get_matches_from([
                "backfill",
                "--season",
                "12",
                "--combines",
                "--direct-apply",
                "--api-path-root",
                "/round-repair-work",
            ])
            .is_err());
    }

    #[test]
    fn combines_conflicts_with_bo3_only() {
        // combine_season_matches() always synthesizes is_bo3 = false, so
        // --bo3-only --combines would silently filter out every combine and
        // produce an empty, confusing run. Reject the combination at the
        // CLI level instead.
        let command = BackfillArgs::augment_args(Command::new("backfill"));
        assert!(command
            .clone()
            .try_get_matches_from([
                "backfill",
                "--season",
                "12",
                "--combines",
                "--bo3-only",
                "--api-path-root",
                "/round-repair-work",
            ])
            .is_err());
        assert!(command
            .try_get_matches_from([
                "backfill",
                "--season",
                "12",
                "--bo3-only",
                "--combines",
                "--api-path-root",
                "/round-repair-work",
            ])
            .is_err());
    }

    #[test]
    fn combines_ledger_mode_is_distinct_from_league_full_reparse_mode() {
        // The ledger dedups on (season, mode, match_id), and
        // matches_combinematches.id / matches_matches.id are independent
        // sequences that can numerically collide within the same season.
        // Distinct mode strings keep a combine run from being mistaken for
        // (or masking) a league-match run that happens to share an id.
        let root = test_path("combines-mode");
        let mut combines_args = backfill_args(&root);
        combines_args.combines = true;
        combines_args.full_reparse = false;
        let mut league_args = backfill_args(&root);
        league_args.full_reparse = true;
        assert_ne!(combines_args.mode(), league_args.mode());
        assert_eq!(combines_args.mode(), "combines-full-reparse-dry-run");
        assert_eq!(league_args.mode(), "full-reparse-dry-run");
    }

    #[test]
    fn full_import_identity_prefixes_combine_match_ids_and_uses_combine_match_type() {
        let root = test_path("full-import-identity");
        let mut combines_args = backfill_args(&root);
        combines_args.combines = true;
        let combine_core_match = core_match(8440, false);
        let (match_id, match_type) =
            full_import_identity(&combines_args, &combine_core_match, "8440");
        assert_eq!(match_id, "combines-8440");
        assert_eq!(match_type, "Combine");

        // Even if a combine's CoreMatch were mistakenly marked is_bo3 (it
        // never is in practice: combines are always single-map), --combines
        // should still win and report "Combine", not "Playoff".
        let miscoded = core_match(8440, true);
        let (_, match_type) = full_import_identity(&combines_args, &miscoded, "8440");
        assert_eq!(match_type, "Combine");

        let league_args = backfill_args(&root);
        let bo1 = core_match(500, false);
        let (match_id, match_type) = full_import_identity(&league_args, &bo1, "500");
        assert_eq!(match_id, "500");
        assert_eq!(match_type, "Regulation");

        let bo3 = core_match(501, true);
        let (match_id, match_type) = full_import_identity(&league_args, &bo3, "501_1");
        assert_eq!(match_id, "501_1");
        assert_eq!(match_type, "Playoff");
    }

    #[test]
    fn is_selected_bo3_only_skips_bo1s() {
        let bo1 = core_match(1, false);
        let bo3 = core_match(2, true);
        let empty = HashSet::new();
        assert!(!is_selected(&bo1, true, &empty));
        assert!(is_selected(&bo3, true, &empty));
        // Without --bo3-only, both are selected.
        assert!(is_selected(&bo1, false, &empty));
        assert!(is_selected(&bo3, false, &empty));
    }

    #[test]
    fn is_selected_combines_bo3_only_with_match_id() {
        let bo3_a = core_match(10, true);
        let bo3_b = core_match(11, true);
        let bo1 = core_match(12, false);
        let selected = HashSet::from([10, 12]);
        // --bo3-only AND --match-id are both active: a match must satisfy
        // both, so the BO1 in the --match-id set is still excluded.
        assert!(is_selected(&bo3_a, true, &selected));
        assert!(!is_selected(&bo3_b, true, &selected));
        assert!(!is_selected(&bo1, true, &selected));
    }

    #[test]
    fn is_selected_match_id_alone_is_unaffected_by_bo3_only_default() {
        let bo1 = core_match(5, false);
        let selected = HashSet::from([5]);
        assert!(is_selected(&bo1, false, &selected));
        assert!(!is_selected(&bo1, false, &HashSet::from([6])));
    }

    #[test]
    fn bo3_only_is_a_match_selection_filter_not_a_mode() {
        // --bo3-only must compose with every mode (dry-run, --full-reparse,
        // --apply, --direct-apply) rather than conflicting with any of
        // them, the same way --match-id does.
        let command = BackfillArgs::augment_args(Command::new("backfill"));
        let matches = command
            .clone()
            .try_get_matches_from([
                "backfill",
                "--season",
                "12",
                "--bo3-only",
                "--full-reparse",
                "--confirm-season",
                "12",
                "--api-path-root",
                "/round-repair-work",
            ])
            .unwrap();
        let args = BackfillArgs::from_arg_matches(&matches).unwrap();
        assert!(args.bo3_only);
        assert!(args.writes());

        let matches = command
            .try_get_matches_from([
                "backfill",
                "--season",
                "12",
                "--bo3-only",
                "--direct-apply",
                "--confirm-season",
                "12",
                "--parser-version",
                "test-parser",
                "--api-path-root",
                "/round-repair-work",
            ])
            .unwrap();
        let args = BackfillArgs::from_arg_matches(&matches).unwrap();
        assert!(args.bo3_only);
        assert!(args.writes());
    }

    #[test]
    fn bo3_only_defaults_to_false_and_processes_every_match() {
        let command = BackfillArgs::augment_args(Command::new("backfill"));
        let matches = command
            .try_get_matches_from([
                "backfill",
                "--season",
                "12",
                "--full-reparse",
                "--api-path-root",
                "/round-repair-work",
            ])
            .unwrap();
        let args = BackfillArgs::from_arg_matches(&matches).unwrap();
        assert!(!args.bo3_only);
    }

    #[test]
    fn cached_source_inventory_requires_the_ledger_digest_and_consistent_checksums() {
        let root = test_path("cached-source-inventory");
        fs::create_dir_all(&root).unwrap();
        let path = root.join("source.jsonl");
        let mut complete = ledger_event("match_complete");
        complete.evidence = Some(json!({ "archiveChecksum": "approved" }));
        let mut failed = LedgerEvent {
            match_id: 456,
            status: "archive_cached".to_owned(),
            ..ledger_event("archive_cached")
        };
        failed.evidence = Some(json!({ "archiveChecksum": "failed-but-retained" }));
        let content = format!(
            "{}\n{}\n",
            serde_json::to_string(&complete).unwrap(),
            serde_json::to_string(&failed).unwrap()
        );
        fs::write(&path, &content).unwrap();
        let digest = hex::encode(Sha256::digest(content.as_bytes()));
        let mut args = backfill_args(&root);
        args.cached_source_ledger = Some(path);
        args.cached_source_ledger_sha256 = Some(digest);

        let inventory = CachedSourceInventory::load(&args).unwrap().unwrap();
        assert_eq!(
            inventory.archive_checksums.get(&123).map(String::as_str),
            Some("approved")
        );
        assert_eq!(
            inventory.archive_checksums.get(&456).map(String::as_str),
            Some("failed-but-retained")
        );

        let mut conflicting = failed.clone();
        conflicting.evidence = Some(json!({ "archiveChecksum": "different" }));
        let conflicting_content = format!(
            "{}\n{}\n",
            serde_json::to_string(&failed).unwrap(),
            serde_json::to_string(&conflicting).unwrap()
        );
        fs::write(
            args.cached_source_ledger.as_ref().unwrap(),
            &conflicting_content,
        )
        .unwrap();
        args.cached_source_ledger_sha256 =
            Some(hex::encode(Sha256::digest(conflicting_content.as_bytes())));
        assert!(CachedSourceInventory::load(&args)
            .unwrap_err()
            .to_string()
            .contains("conflicting archive checksums"));

        args.cached_source_ledger_sha256 = Some("0".repeat(64));
        assert!(CachedSourceInventory::load(&args)
            .unwrap_err()
            .to_string()
            .contains("SHA-256 mismatch"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn member_path_rejects_traversal_and_absolute_paths() {
        assert!(safe_member_path("demos/match.dem"));
        assert!(safe_member_path("demo/nested/match.dem"));
        assert!(!safe_member_path("../match.dem"));
        assert!(!safe_member_path("/tmp/match.dem"));
    }

    #[test]
    fn archive_url_allowlist_covers_backblaze_and_legacy_csc_spaces_only() {
        assert!(validate_archive_url(
            "https://f005.backblazeb2.com/file/csc-demo-archive/s18/M01/match.7z"
        )
        .is_ok());
        assert!(validate_archive_url(
            "https://cscdemos.nyc3.digitaloceanspaces.com/s20/M01/match.7z"
        )
        .is_ok());
        assert!(validate_archive_url(
            "https://cscdemos.nyc3.cdn.digitaloceanspaces.com/s20/M01/match.zip"
        )
        .is_ok());
        assert!(
            validate_archive_url("https://attacker.nyc3.digitaloceanspaces.com/match.7z").is_err()
        );
        assert!(validate_archive_url(
            "https://f005.backblazeb2.com.attacker.example/file/csc-demo-archive/match.7z"
        )
        .is_err());
        assert!(validate_archive_url(
            "https://user@f005.backblazeb2.com/file/csc-demo-archive/match.7z"
        )
        .is_err());
    }

    #[test]
    fn parse_s3_keys_treats_null_and_empty_as_absent() {
        assert_eq!(parse_s3_keys(None).unwrap(), Vec::<String>::new());
        assert_eq!(
            parse_s3_keys(Some(&Value::Null)).unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(
            parse_s3_keys(Some(&json!([]))).unwrap(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn parse_s3_keys_preserves_array_order() {
        let value = json!(["s20/M10/map-a.dem.zip", "s20/M10/map-b.dem.zip"]);
        assert_eq!(
            parse_s3_keys(Some(&value)).unwrap(),
            vec![
                "s20/M10/map-a.dem.zip".to_owned(),
                "s20/M10/map-b.dem.zip".to_owned(),
            ]
        );
    }

    #[test]
    fn parse_s3_keys_rejects_non_array_and_non_string_elements() {
        assert!(parse_s3_keys(Some(&json!("not-an-array"))).is_err());
        assert!(parse_s3_keys(Some(&json!([1, 2]))).is_err());
        assert!(parse_s3_keys(Some(&json!(["ok", null]))).is_err());
    }

    #[test]
    fn s3_key_to_url_builds_the_do_cdn_url_for_a_bare_key() {
        let url =
            s3_key_to_url("s20/M10/s20-M10-Demons-vs-Foo-mid9077-0_de_anubis.dem.zip").unwrap();
        assert_eq!(
            url.as_str(),
            "https://cscdemos.nyc3.cdn.digitaloceanspaces.com/s20/M10/s20-M10-Demons-vs-Foo-mid9077-0_de_anubis.dem.zip"
        );
    }

    #[test]
    fn s3_key_to_url_accepts_an_allowlisted_absolute_url_from_an_admin_correction() {
        // CSC-Core's Match admin lets ops paste a corrected demo URL directly
        // into s3_keys (e.g. a migrated Backblaze URL); it's stored verbatim,
        // not re-relativized to a DO CDN key. This must not be rejected —
        // that would fail the s3_keys path and fall back to the legacy
        // demo_url, recreating the truncation this path exists to fix.
        let url = s3_key_to_url(
            "https://f005.backblazeb2.com/file/csc-demo-archive/s20/M10/corrected.dem.zip",
        )
        .unwrap();
        assert_eq!(
            url.as_str(),
            "https://f005.backblazeb2.com/file/csc-demo-archive/s20/M10/corrected.dem.zip"
        );
    }

    #[test]
    fn s3_key_to_url_rejects_absolute_urls_off_the_csc_archive_allowlist() {
        assert!(s3_key_to_url("https://attacker.example/key.dem.zip").is_err());
    }

    #[test]
    fn s3_key_to_url_rejects_a_bare_leading_slash_value() {
        assert!(s3_key_to_url("/absolute/key.dem.zip").is_err());
    }

    #[test]
    fn s3_key_to_url_rejects_a_non_archive_extension() {
        assert!(s3_key_to_url("s20/M10/not-an-archive.txt").is_err());
    }

    #[test]
    fn validate_s3_key_order_accepts_matching_length_and_map_names() {
        let mut core = core_match(9223, true);
        core.scored_map_names = vec!["de_nuke".to_owned(), "de_anubis".to_owned()];
        let keys = vec![
            "s20/M13/....-mid9223-0_de_nuke-....dem.zip".to_owned(),
            "s20/M13/....-mid9223-1_de_anubis-....dem.zip".to_owned(),
        ];
        assert!(validate_s3_key_order(&keys, &core).is_ok());
    }

    #[test]
    fn validate_s3_key_order_rejects_extra_keys_like_the_9077_duplicate_upload() {
        // Match 9077's real shape: 4 s3_keys entries (a mid-match-restart
        // duplicate de_inferno upload) but only 3 scored maps.
        let mut core = core_match(9077, true);
        core.scored_map_names = vec![
            "de_anubis".to_owned(),
            "de_inferno".to_owned(),
            "de_ancient".to_owned(),
        ];
        let keys = vec![
            "..._de_anubis_...".to_owned(),
            "..._de_inferno_a_...".to_owned(),
            "..._de_inferno_b_...".to_owned(),
            "..._de_ancient_...".to_owned(),
        ];
        assert!(validate_s3_key_order(&keys, &core).is_err());
    }

    #[test]
    fn validate_s3_key_order_rejects_fewer_keys_than_scored_maps() {
        let mut core = core_match(9275, true);
        core.scored_map_names = vec![
            "de_anubis".to_owned(),
            "de_nuke".to_owned(),
            "de_nuke".to_owned(),
        ];
        let keys = vec!["..._de_anubis_...".to_owned(), "..._de_nuke_...".to_owned()];
        assert!(validate_s3_key_order(&keys, &core).is_err());
    }

    #[test]
    fn validate_s3_key_order_rejects_a_same_length_reshuffle() {
        // Match 9275's own real shape: keys are [anubis, anubis, nuke] but
        // the scored maps are [anubis, nuke, nuke] — same count (3 and 3),
        // different order. Position 1 disagrees (anubis vs nuke) and must
        // fail closed rather than writing the wrong map into 9275_1.
        let mut core = core_match(9275, true);
        core.scored_map_names = vec![
            "de_anubis".to_owned(),
            "de_nuke".to_owned(),
            "de_nuke".to_owned(),
        ];
        let keys = vec![
            "..._de_anubis_2026-07-24_...".to_owned(),
            "..._de_anubis_2026-07-25_...".to_owned(),
            "..._de_nuke_2026-07-25_...".to_owned(),
        ];
        assert!(validate_s3_key_order(&keys, &core).is_err());
    }

    #[test]
    fn validate_s3_key_order_is_case_insensitive() {
        let mut core = core_match(1, true);
        core.scored_map_names = vec!["de_Anubis".to_owned()];
        let keys = vec!["...DE_ANUBIS...".to_owned()];
        assert!(validate_s3_key_order(&keys, &core).is_ok());
    }

    #[test]
    fn discover_single_map_demo_ignores_the_unreliable_filename_digit() {
        // Two archives whose filenames both embed the same "-0_" digit
        // (the Core-planning#242 item 2 bug) must still be told apart by
        // the caller-supplied map_index (array position), not the digit.
        let root = test_path("s3-keys-single-map");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            root.join("s20-M10-Demons-vs-Foo-mid9077-0_de_anubis.dem"),
            b"demo",
        )
        .unwrap();
        let demo = discover_single_map_demo(
            &root,
            &core_match(9077, true),
            1,
            "de_anubis",
            &HashSet::from([9077]),
        )
        .unwrap();
        // stats_match_id uses the zero-based map_index (CSC-Stats'
        // convention), not any value derived from the map name.
        assert_eq!(demo.stats_match_id, "9077_1");
        assert_eq!(demo.identity_source, "s3_keys_array_order");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn discover_single_map_demo_rejects_zero_or_multiple_dems() {
        let root = test_path("s3-keys-empty");
        fs::create_dir_all(&root).unwrap();
        assert!(discover_single_map_demo(
            &root,
            &core_match(9077, true),
            0,
            "de_anubis",
            &HashSet::from([9077])
        )
        .is_err());
        fs::write(root.join("a.dem"), b"1").unwrap();
        fs::write(root.join("b.dem"), b"2").unwrap();
        assert!(discover_single_map_demo(
            &root,
            &core_match(9077, true),
            0,
            "de_anubis",
            &HashSet::from([9077])
        )
        .is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn discover_single_map_demo_still_fails_closed_on_a_displaced_match_id() {
        let root = test_path("s3-keys-displaced");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("s20-mid555-0_de_anubis.dem"), b"demo").unwrap();
        assert!(discover_single_map_demo(
            &root,
            &core_match(9077, true),
            0,
            "de_anubis",
            &HashSet::from([9077, 555]),
        )
        .is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn discovers_root_demo_and_demos_subdirectories_recursively() {
        let root = std::env::temp_dir().join(format!("stats-importer-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("demo/deeper")).unwrap();
        fs::create_dir_all(root.join("demos")).unwrap();
        fs::write(root.join("s11-mid123-0_root.dem"), b"a").unwrap();
        fs::write(root.join("demo/deeper/s11-mid123-1_nested.DEM"), b"b").unwrap();
        fs::write(root.join("demos/ignore.txt"), b"c").unwrap();
        let mut found =
            discover_demos(&root, &core_match(123, true), &HashSet::from([123])).unwrap();
        found.sort_by(|a, b| a.stats_match_id.cmp(&b.stats_match_id));
        assert_eq!(
            found
                .iter()
                .map(|d| d.stats_match_id.as_str())
                .collect::<Vec<_>>(),
            ["123_0", "123_1"]
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn bo1_ignores_historical_filename_suffix() {
        let root =
            std::env::temp_dir().join(format!("stats-importer-bo1-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("s11-mid456-7_map.dem"), b"demo").unwrap();
        let found = discover_demos(&root, &core_match(456, false), &HashSet::from([456])).unwrap();
        assert_eq!(found[0].stats_match_id, "456");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn bo3_normalizes_a_stale_embedded_match_id_from_core_metadata() {
        let root = test_path("normalize-mid");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("s12-mid999-2_mirage.dem"), b"demo").unwrap();
        let found = discover_demos(&root, &core_match(456, true), &HashSet::from([456])).unwrap();
        assert_eq!(found[0].stats_match_id, "456_2");
        assert_eq!(found[0].identity_source, "core_id_normalized");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn bo3_uses_core_map_order_when_the_filename_has_no_identity() {
        let root = test_path("normalize-unnamed");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("first.dem"), b"a").unwrap();
        fs::write(root.join("second.dem"), b"b").unwrap();
        let mut core = core_match(456, true);
        core.played_map_numbers = vec![1, 3];
        core.map_count = 2;
        let found = discover_demos(&root, &core, &HashSet::from([456])).unwrap();
        assert_eq!(found[0].stats_match_id, "456_1");
        assert_eq!(found[1].stats_match_id, "456_3");
        assert!(found
            .iter()
            .all(|demo| demo.identity_source == "core_map_order"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn foreign_embedded_core_match_in_the_same_season_fails_closed() {
        for is_bo3 in [false, true] {
            let root = test_path(if is_bo3 { "foreign-bo3" } else { "foreign-bo1" });
            fs::create_dir_all(&root).unwrap();
            fs::write(root.join("s12-mid999-1_mirage.dem"), b"demo").unwrap();
            let error = discover_demos(&root, &core_match(456, is_bo3), &HashSet::from([456, 999]))
                .unwrap_err();
            assert!(error.to_string().contains("belonging to Core match 999"));
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn core_map_order_rejects_partial_and_mixed_archives() {
        let partial_root = test_path("partial-unnamed");
        fs::create_dir_all(&partial_root).unwrap();
        fs::write(partial_root.join("first.dem"), b"a").unwrap();
        let mut core = core_match(456, true);
        core.played_map_numbers = vec![1, 2];
        core.map_count = 2;
        let partial = discover_demos(&partial_root, &core, &HashSet::from([456])).unwrap_err();
        assert!(partial.to_string().contains("cannot use Core map order"));
        fs::remove_dir_all(partial_root).unwrap();

        let mixed_root = test_path("mixed-naming");
        fs::create_dir_all(&mixed_root).unwrap();
        fs::write(mixed_root.join("s12-mid456-1_mirage.dem"), b"a").unwrap();
        fs::write(mixed_root.join("second.dem"), b"b").unwrap();
        let mixed = discover_demos(&mixed_root, &core, &HashSet::from([456])).unwrap_err();
        assert!(mixed.to_string().contains("mixes suffixed and unnamed"));
        fs::remove_dir_all(mixed_root).unwrap();
    }

    #[test]
    fn cache_reuse_requires_an_exact_archive_checksum() {
        let root = test_path("cached-archive");
        let old_attempt = root.join("attempt-old");
        let current_attempt = root.join("attempt-current");
        fs::create_dir_all(&old_attempt).unwrap();
        fs::create_dir_all(&current_attempt).unwrap();
        let archive = old_attempt.join("archive.7z");
        fs::write(&archive, b"approved archive").unwrap();
        let checksum = sha256_file(&archive).unwrap();

        assert_eq!(
            checksum_matched_cached_archive(&root, &current_attempt, "7z", Some(&checksum))
                .unwrap(),
            Some(archive)
        );
        assert!(checksum_matched_cached_archive(
            &root,
            &current_attempt,
            "7z",
            Some(&"0".repeat(64)),
        )
        .unwrap()
        .is_none());
        assert!(
            checksum_matched_cached_archive(&root, &current_attempt, "7z", None)
                .unwrap()
                .is_none()
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn attempt_workspace_is_deleted_on_drop_and_empty_parents_are_pruned() {
        let root = test_path("attempt-cleanup");
        let attempt = root.join("s18/123/attempt-1");
        fs::create_dir_all(attempt.join("extracted")).unwrap();
        fs::write(attempt.join("archive.7z"), b"archive").unwrap();
        fs::write(attempt.join("extracted/match.dem"), b"demo").unwrap();
        {
            let _workspace = AttemptWorkspace::new(&root, attempt.clone(), false);
        }
        assert!(!attempt.exists());
        assert!(!root.join("s18/123").exists());
        assert!(root.exists());
        fs::remove_dir(root).unwrap();
    }

    #[test]
    fn successful_workspace_is_retained_only_when_explicitly_requested() {
        let root = test_path("attempt-retain");
        let attempt = root.join("s18/123/attempt-1");
        fs::create_dir_all(&attempt).unwrap();
        fs::write(attempt.join("archive.7z"), b"archive").unwrap();
        {
            let mut workspace = AttemptWorkspace::new(&root, attempt.clone(), false);
            workspace.finish(true).unwrap();
        }
        assert!(attempt.join("archive.7z").is_file());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn failed_workspace_is_retained_when_keep_all_is_requested() {
        let root = test_path("attempt-retain-all");
        let attempt = root.join("s18/123/attempt-1");
        fs::create_dir_all(&attempt).unwrap();
        fs::write(attempt.join("archive.7z"), b"archive").unwrap();
        {
            let _workspace = AttemptWorkspace::new(&root, attempt.clone(), true);
        }
        assert!(attempt.join("archive.7z").is_file());
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn clean_non_repairable_verdicts_are_explicit() {
        for classification in ["ingest_incomplete", "fingerprint_mismatch", "ambiguous"] {
            assert!(is_clean_non_repairable(Some(classification)));
        }
        assert!(!is_clean_non_repairable(Some("parse_failed")));
        assert!(!is_clean_non_repairable(Some("no_matching_candidate")));
        assert!(!is_clean_non_repairable(Some("ready")));
        assert!(!is_clean_non_repairable(None));
    }

    #[test]
    fn missing_match_apply_is_bound_to_reviewed_parser_evidence() {
        let reviewed = json!({
            "sourceChecksum": "source-a",
            "parserOutputChecksum": "parser-a",
            "parserVersion": "worker-v1",
            "parsedSubtreeHash": "subtree-a",
        });
        assert!(verify_reviewed_import(&reviewed, &reviewed).is_ok());

        let mut changed = reviewed.clone();
        changed["parserVersion"] = json!("worker-v2");
        assert!(verify_reviewed_import(&reviewed, &changed)
            .unwrap_err()
            .to_string()
            .contains("parserVersion"));

        let legacy = json!({ "sourceChecksum": "source-a" });
        assert!(verify_reviewed_import(&legacy, &reviewed)
            .unwrap_err()
            .to_string()
            .contains("omitted parserOutputChecksum"));
    }

    #[test]
    fn full_import_error_preserves_status_for_non_json_bodies() {
        let error =
            parse_full_import_response(StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error")
                .unwrap_err()
                .to_string();
        assert!(error.contains("500 Internal Server Error"));
        assert!(error.contains("Internal Server Error"));
    }

    #[test]
    fn ledger_discards_only_an_incomplete_trailing_record() {
        let path = test_path("trailing-ledger");
        {
            let mut ledger = Ledger::open(path.clone()).unwrap();
            ledger
                .append(ledger_event("skipped_not_repairable"))
                .unwrap();
        }
        let valid_len = fs::metadata(&path).unwrap().len();
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(br#"{"schema_version":1,"timestamp_unix"#)
                .unwrap();
            file.sync_all().unwrap();
        }
        let ledger = Ledger::open(path.clone()).unwrap();
        assert!(ledger.is_complete(18, "dry-run", 123));
        assert_eq!(fs::metadata(&path).unwrap().len(), valid_len);
        drop(ledger);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn ledger_preserves_a_complete_record_missing_only_its_newline() {
        let path = test_path("missing-newline-ledger");
        fs::write(
            &path,
            serde_json::to_vec(&ledger_event("match_complete")).unwrap(),
        )
        .unwrap();
        let ledger = Ledger::open(path.clone()).unwrap();
        assert!(ledger.is_complete(18, "dry-run", 123));
        assert!(fs::read(&path).unwrap().ends_with(b"\n"));
        drop(ledger);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn ledger_rejects_newline_terminated_corruption() {
        let path = test_path("interior-corrupt-ledger");
        let mut bytes = serde_json::to_vec(&ledger_event("match_complete")).unwrap();
        bytes.extend_from_slice(b"\n{not-json}\n");
        fs::write(&path, bytes).unwrap();
        assert!(Ledger::open(path.clone()).is_err());
        fs::remove_file(path).unwrap();
    }
}
