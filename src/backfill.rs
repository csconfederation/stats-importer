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
    #[arg(long)]
    apply: bool,

    /// Required with --apply to make an accidental write invocation obvious.
    #[arg(long, requires = "apply")]
    confirm_season: Option<i32>,

    /// Version/profile configured by CSC-Stats for the pinned parser.
    #[arg(long, env = "STATS_REPAIR_PARSER_VERSION")]
    parser_version: String,

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
    #[arg(
        long,
        requires = "cached_source_ledger",
        conflicts_with = "apply"
    )]
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
    tier: Option<String>,
    marked_forfeit: bool,
    legacy_one_zero: bool,
    has_forfeit_audit: bool,
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
                format!("invalid cached source ledger JSON at line {}", line_number + 1)
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
        Ok(Some(Self { checksum, archive_checksums }))
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
        mode: if args.apply { "apply" } else { "dry-run" }.to_owned(),
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
                 array_agg(DISTINCT ms.map_number ORDER BY ms.map_number) AS played_map_numbers
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
               coalesce(home_tier.name, away_tier.name) AS tier,
               coalesce(sf.marked_forfeit, false) AS marked_forfeit,
               coalesce(sf.legacy_one_zero, false) AS legacy_one_zero,
               coalesce(af.has_forfeit_audit, false) AS has_forfeit_audit
        FROM matches_matches m
        JOIN leagues_matchday md ON md.id = m.match_day_id
        JOIN leagues_seasons s ON s.id = md.season_id
        LEFT JOIN teams_teams home_team ON home_team.id = m.home_id
        LEFT JOIN players_tiers home_tier ON home_tier.id = home_team.tier_id
        LEFT JOIN teams_teams away_team ON away_team.id = m.away_id
        LEFT JOIN players_tiers away_tier ON away_tier.id = away_team.tier_id
        LEFT JOIN stat_flags sf ON sf.match_id = m.id
        LEFT JOIN audit_flags af ON af.match_id = m.id
        WHERE s.number = $1
        ORDER BY md.scheduled_date, m.id
        "#,
    )
    .bind(season)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

fn validate_archive_url(raw: &str) -> Result<Url> {
    let url = Url::parse(raw).context("invalid demo_url")?;
    let host = url.host_str();
    let backblaze = host == Some(B2_HOST) && url.path().starts_with(B2_PATH_PREFIX);
    let legacy_digital_ocean = host.is_some_and(|value| LEGACY_DO_HOSTS.contains(&value));
    if url.scheme() != "https"
        || (!backblaze && !legacy_digital_ocean)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
    {
        bail!("demo_url is not an allowlisted CSC archive URL");
    }
    if !(url.path().to_ascii_lowercase().ends_with(".7z")
        || url.path().to_ascii_lowercase().ends_with(".zip"))
    {
        bail!("demo_url is not a .7z/.zip archive");
    }
    Ok(url)
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
    demo: &DemoCandidate,
    dry_run: bool,
    archive_checksum: &str,
    archive_object_key: &str,
    reviewed: Option<&Value>,
    inventory_checksum: Option<&str>,
) -> Result<Value> {
    let stats_url = env::var("STATS_API_URL").context("STATS_API_URL is required")?;
    let mut body = json!({
        "path": api_path(args, &demo.path)?,
        "statsMatchId": demo.stats_match_id,
        "dryRun": dry_run,
        "parserVersion": args.parser_version,
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
            "v2\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}",
            demo.stats_match_id,
            demo.checksum,
            args.parser_version,
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

async fn full_import_request(
    client: &Client,
    args: &BackfillArgs,
    token: &str,
    core_match: &CoreMatch,
    demo: &DemoCandidate,
) -> Result<Value> {
    let stats_url = env::var("STATS_API_URL").context("STATS_API_URL is required")?;
    let tier = core_match
        .tier
        .as_deref()
        .ok_or_else(|| anyhow!("Core match {} has no team tier", core_match.match_id))?;
    let body = json!({
        "path": api_path(args, &demo.path)?,
        "matchId": demo.stats_match_id,
        "matchDay": core_match.match_day,
        "matchType": if core_match.is_bo3 { "Playoff" } else { "Regulation" },
        "season": args.season,
        "tier": tier,
        "traceId": format!(
            "historical-recovery-s{}-core{}-stats{}",
            args.season, core_match.match_id, demo.stats_match_id
        ),
        "createOnly": true,
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
        ledger.append(event(
            args,
            core_match.match_id,
            "using_cached_archive",
            None,
            Some(cached_archive.to_string_lossy().to_string()),
            Some(json!({
                "archiveChecksum": checksum,
                "sourceInventoryChecksum": cached_source_inventory
                    .map(|item| item.checksum.as_str()),
            })),
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

    let mut validations = Vec::new();
    for demo in demos {
        let response = repair_request(
            client,
            args,
            token,
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

    if args.apply {
        for target in &ordered_targets {
            let Some(validations) = ready_by_target.get(target) else {
                continue;
            };
            let validation = validations[0];
            let reviewed = reviewed_inventory
                .and_then(|inventory| {
                    inventory.ready.get(&(
                        core_match.match_id,
                        validation.candidate.stats_match_id.clone(),
                        validation.candidate.checksum.clone(),
                    ))
                })
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
            let response = repair_request(
                client,
                args,
                token,
                &validation.candidate,
                false,
                &archive_checksum,
                url.path(),
                Some(reviewed),
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
            let reviewed = reviewed_inventory
                .and_then(|inventory| {
                    inventory.importable.get(&(
                        core_match.match_id,
                        validation.candidate.stats_match_id.clone(),
                        validation.candidate.checksum.clone(),
                    ))
                })
                .ok_or_else(|| {
                    anyhow!(
                        "missing-match candidate {} is absent from the reviewed dry-run inventory",
                        validation.candidate.stats_match_id,
                    )
                })?;
            verify_reviewed_import(reviewed, &validation.response)?;
            let response =
                full_import_request(client, args, token, core_match, &validation.candidate).await?;
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
            if args.apply { "repaired" } else { "validated" },
            importable_by_target.len(),
            if args.apply {
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
    if args.apply && args.confirm_season != Some(args.season) {
        bail!("--apply requires --confirm-season {}", args.season);
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
        args.workspace.join(format!(
            "season-{}-{}.jsonl",
            args.season,
            if args.apply { "apply" } else { "dry-run" }
        ))
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
    let mode = if args.apply { "apply" } else { "dry-run" };
    let selected: HashSet<i64> = args.match_id.iter().copied().collect();
    let matches = season_matches(&pool, args.season).await?;
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
        let required_review = if selected.is_empty() {
            &available
        } else {
            &selected
        };
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
        "Season {}: {} Core matches found; mode={mode}; concurrency=1",
        args.season,
        matches.len()
    );
    let mut processed = 0_usize;
    let mut failures = 0_usize;
    for core_match in matches {
        if !selected.is_empty() && !selected.contains(&core_match.match_id) {
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
            tier: Some("Elite".to_owned()),
            marked_forfeit: false,
            legacy_one_zero: false,
            has_forfeit_audit: false,
        }
    }

    fn backfill_args(workspace: &Path) -> BackfillArgs {
        BackfillArgs {
            season: 18,
            apply: false,
            confirm_season: None,
            parser_version: "test-parser".to_owned(),
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
            keep_successful: false,
            keep_all: false,
            max_archive_gib: 8,
            max_extracted_gib: 32,
            max_archive_members: 100,
        }
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
        fs::write(args.cached_source_ledger.as_ref().unwrap(), &conflicting_content).unwrap();
        args.cached_source_ledger_sha256 = Some(hex::encode(Sha256::digest(conflicting_content.as_bytes())));
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
        assert!(
            checksum_matched_cached_archive(&root, &current_attempt, "7z", Some(&"0".repeat(64)),)
                .unwrap()
                .is_none()
        );
        assert!(checksum_matched_cached_archive(&root, &current_attempt, "7z", None)
            .unwrap()
            .is_none());
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
