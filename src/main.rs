use std::{
    env,
    fs::{self},
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use regex::Regex;
use serde::Serialize;
use sqlx::{FromRow, PgPool};

mod backfill;
mod forfeit;

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,

    /// demos directory
    #[arg(short, long)]
    directory: Option<String>,

    /// override tier (optional)
    #[arg(short, long)]
    tier: Option<String>,

    /// override season (optional)
    #[arg(short, long)]
    season: Option<u8>,

    /// override match_day (optional)
    #[arg(short, long)]
    match_day: Option<String>,

    /// fix core match stats scores (optional)
    #[arg(long)]
    fix_core_scores: Option<bool>,

    /// fix demo team names (optional)
    #[arg(long)]
    fix_team_names: Option<bool>,

    /// Treat legacy single-file imports as combine/FA Colo matches.
    #[arg(long, conflicts_with = "league")]
    combine: bool,

    /// Treat legacy single-file imports as league matches.
    #[arg(long, conflicts_with = "combine")]
    league: bool,
}

#[derive(Subcommand, Debug, Clone)]
enum Command {
    /// Inventory or repair historical round-player stats for one Core season.
    Backfill(backfill::BackfillArgs),
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    let args = Args::parse();
    if let Some(Command::Backfill(backfill_args)) = &args.command {
        if let Err(error) = backfill::run(backfill_args.clone()).await {
            eprintln!("backfill failed: {error:#}");
            std::process::exit(1);
        }
        return;
    }

    let Some(directory) = &args.directory else {
        eprintln!(
            "--directory is required for legacy import mode (or use the backfill subcommand)"
        );
        std::process::exit(2);
    };
    let dir = Path::new(directory);
    if !dir.is_dir() {
        println!("'{}' is not a directory", directory);
        return;
    }
    println!("Importing from {:?}", dir.as_os_str());
    let mut paths = Vec::new();
    for entry in dir.read_dir().expect("read_dir call failed") {
        if let Ok(entry) = entry {
            if entry.path().is_dir() {
                continue;
            }
            paths.push(entry.path());
        }
    }
    println!("Found {} .dem files", paths.len());

    let pool = PgPool::connect(&env::var("DATABASE_URL").expect("missing DATABASE_URL"))
        .await
        .unwrap();
    fs::create_dir_all(format!("{}/_completed", dir.as_os_str().to_str().unwrap())).unwrap();
    fs::create_dir_all(format!("{}/_skipped", dir.as_os_str().to_str().unwrap())).unwrap();
    for path in paths {
        let filename = &path.file_name().unwrap().to_str().unwrap();
        println!("Processing {}...", &filename);
        match handle_file(filename, &path, args.clone(), &pool).await {
            Ok(filename) => {
                let exsiting = dir.join(&filename);
                let p = dir.join("_completed").join(&filename);
                println!("moving to: {}", p.display());
                fs::rename(&exsiting, p).unwrap();
                println!("Processed {} successfully", filename);
            }
            Err(err) => {
                println!("Skipping {}, error: {}", filename, err);
                let filename = filename.replace(".zip", "");
                let p = dir.join("_skipped").join(&filename);
                let exsiting = dir.join(&filename);
                println!("moving to: {}", p.display());
                fs::rename(&exsiting, p).unwrap();
            }
        }
    }
}

async fn handle_file(filename: &str, path: &PathBuf, args: Args, pool: &PgPool) -> Result<String> {
    let match_id_re = Regex::new(r"-mid([0-9]*)-").expect("regex is busted");
    let match_info: MatchInfo = match match_id_re.captures(filename) {
        Some(captures) => {
            let mid = captures.get(0).unwrap().as_str();
            let id = mid.replace("-", "").replace("mid", "").parse::<i64>()?;
            get_core_match(id, pool, filename, &args).await?
        }
        None => {
            println!("Cannot parse match id from filename, using args...");
            let is_combine = legacy_file_is_combine(&args, filename);
            let Some(season) = args.season else {
                return Err(anyhow!("--season arg not provided, skipping..."));
            };
            let tier = if is_combine {
                None
            } else {
                Some(
                    args.tier
                        .ok_or_else(|| anyhow!("--tier arg not provided, skipping..."))?,
                )
            };
            let Some(match_day) = args.match_day else {
                return Err(anyhow!("--match_day arg not provided, skipping..."));
            };
            MatchInfo {
                match_id: filename.to_string(),
                tier,
                season: i32::from(season),
                match_day,
                is_series: false,
                core_regulation_id: None,
                match_date: None,
                match_type: if is_combine {
                    "Combine".to_owned()
                } else {
                    "Regulation".to_owned()
                },
            }
        }
    };

    let file_path = String::from(path.as_path().to_str().unwrap());
    let req_root_dir = env::var("REQUEST_ROOT_DIR");
    let req_path = match req_root_dir {
        Ok(root_dir) => format!("{}/{}", root_dir, filename),
        Err(_) => file_path,
    };

    let mut core_map_number = 1;
    let map_num_str = if match_info.is_series {
        let map_number_re = Regex::new(r"-mid([0-9]*)-[0-9]").expect("regex is busted");
        let map_number = match map_number_re.captures(filename) {
            Some(captures) => {
                let c = captures.get(0).unwrap().as_str();
                let num_char = c.chars().last().unwrap();
                let map_num = num_char.to_string().parse::<i32>()?;
                map_num
            }
            None => {
                return Err(anyhow!(
                    "cannot parse series map number from filename, skipping..."
                ));
            }
        };
        // The filename suffix is Stats' zero-based map index; Core's map_number is 1-based.
        core_map_number = map_number + 1;
        format!("_{}", map_number)
    } else {
        String::new()
    };
    if let Some(core_id) = match_info.core_regulation_id {
        let guard = forfeit::fetch_match_guard(pool, core_id).await?;
        if let Some(reason) = direct_import_skip(&guard, core_map_number) {
            return Err(anyhow!(
                "skipped_forfeit: Core match {core_id} map {core_map_number}: {}",
                reason.describe()
            ));
        }
    }
    let body = StatsRequestBody {
        path: req_path,
        match_id: format!("{}{}", match_info.match_id, map_num_str),
        season: match_info.season,
        tier: match_info.tier,
        match_day: match_info.match_day,
        match_type: match_info.match_type,
        match_date: match_info.match_date,
        fix_core_scores: args.fix_core_scores.unwrap_or(false),
        fix_team_names: args.fix_team_names.unwrap_or(false),
    };
    let client = reqwest::Client::new();
    let url = format!(
        "{}/api/add-match",
        env::var("STATS_API_URL").expect("STATS_API_URL expected")
    );
    let resp = client.post(url).json(&body).send().await?;
    if resp.status() != 200 {
        return Err(anyhow!("{}", resp.status()));
    }
    let filename = filename.replace(".dem", "").replace(".zip", "");
    Ok(String::from(format!("{}.dem", &filename)))
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StatsRequestBody {
    path: String,
    match_id: String,
    season: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    tier: Option<String>,
    match_day: String,
    match_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    match_date: Option<String>,
    fix_core_scores: bool,
    fix_team_names: bool,
}
#[derive(Debug, Clone)]
struct MatchInfo {
    match_id: String,
    season: i32,
    // None for Combine/FA Colo (#705); Stats takes no tier for them.
    tier: Option<String>,
    match_day: String,
    is_series: bool,
    // Core id of a regulation match; the forfeit/void guard only applies to these.
    core_regulation_id: Option<i64>,
    match_date: Option<String>,
    match_type: String,
}

#[derive(Debug, FromRow, Clone)]
struct CoreMatch {
    id: i64,
    match_type: String,
    is_playoff: bool,
    is_bo3: bool,
    season: i32,
    tier: Option<String>,
    match_day: Option<String>,
    match_date: Option<String>,
    demo_url: Option<String>,
}

async fn get_core_match(id: i64, pool: &PgPool, filename: &str, args: &Args) -> Result<MatchInfo> {
    // Legacy Combine ids (pre-merge filenames) can equal a regulation id, so both rows can match.
    let candidates = sqlx::query_as::<_, CoreMatch>(
        r#"
        select mm.id,
               mm.match_type,
               mm.is_playoff,
               mm.is_bo3,
               ls.number as season,
               case when mm.match_type = 'REGULATION' then ht_tier.name else mm_tier.name end as tier,
               lm.number as match_day,
               to_char(case when mm.match_type = 'REGULATION'
                            then coalesce(mm.completed_at, mm.scheduled_date)
                            when mm.match_type in ('COMBINE', 'FA_COLO')
                            then coalesce(mm.game_finished_at, mm.scheduled_date)
                            else mm.scheduled_date
                       end AT TIME ZONE 'UTC',
                       'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as match_date,
               mm.demo_url
            from matches_matches mm
                join leagues_seasons ls on ls.id = mm.season_id
                left join leagues_matchday lm on lm.id = mm.match_day_id
                left join teams_teams ht on ht.id = mm.home_id
                left join players_tiers ht_tier on ht_tier.id = ht.tier_id
                left join players_tiers mm_tier on mm_tier.id = mm.tier_id
        where mm.id = $1 or mm.legacy_combine_id = $1;
    "#,
    )
    .bind(id)
    .fetch_all(pool)
    .await?;

    let (league, combine) = split_candidates(id, &candidates)?;
    let selected = match select_match_source(id, league, combine, filename, args)? {
        MatchSource::League => league.expect("selected league match must exist"),
        MatchSource::Combine => combine.expect("selected combine match must exist"),
    };
    match_info(selected)
}

/// Why a `--directory` file's map must not import played stats (map-scoped, so
/// a BO3's other maps still import).
fn direct_import_skip(
    guard: &forfeit::MapGuard,
    core_map_number: i32,
) -> Option<forfeit::SkipReason> {
    guard.skip_reason(core_map_number)
}

fn is_combine_type(match_type: &str) -> bool {
    matches!(match_type, "COMBINE" | "FA_COLO")
}

/// Splits the lookup rows into the non-Combine row and the Combine/FA Colo row.
fn split_candidates(
    id: i64,
    candidates: &[CoreMatch],
) -> Result<(Option<&CoreMatch>, Option<&CoreMatch>)> {
    let (combines, others): (Vec<_>, Vec<_>) = candidates
        .iter()
        .partition(|row| is_combine_type(&row.match_type));
    if combines.len() > 1 || others.len() > 1 {
        return Err(anyhow!(
            "Core match id {id} is ambiguous: {} Combine and {} other rows",
            combines.len(),
            others.len()
        ));
    }
    Ok((others.first().copied(), combines.first().copied()))
}

fn stats_match_type(match_type: &str, is_playoff: bool) -> Result<&'static str> {
    match match_type {
        "REGULATION" if is_playoff => Ok("Playoff"),
        "REGULATION" => Ok("Regulation"),
        "COMBINE" => Ok("Combine"),
        "FA_COLO" => Ok("FAColo"),
        other => Err(anyhow!(
            "Core match type {other} is not imported into Stats"
        )),
    }
}

fn match_info(row: &CoreMatch) -> Result<MatchInfo> {
    let id = row.id;
    let match_type = stats_match_type(&row.match_type, row.is_playoff)
        .map_err(|error| anyhow!("Core match {id}: {error}"))?;
    let tier = if is_combine_type(&row.match_type) {
        None
    } else {
        Some(
            row.tier
                .clone()
                .ok_or_else(|| anyhow!("Core match {id} has no tier"))?,
        )
    };
    let match_day = if is_combine_type(&row.match_type) {
        String::new()
    } else {
        row.match_day
            .clone()
            .ok_or_else(|| anyhow!("Core match {id} has no match day"))?
    };
    Ok(MatchInfo {
        match_id: id.to_string(),
        season: row.season,
        tier,
        match_day,
        is_series: row.is_bo3,
        core_regulation_id: (!is_combine_type(&row.match_type)).then_some(row.id),
        match_date: row.match_date.clone(),
        match_type: match_type.to_owned(),
    })
}

#[derive(Debug, PartialEq, Eq)]
enum MatchSource {
    League,
    Combine,
}

fn select_match_source(
    id: i64,
    league: Option<&CoreMatch>,
    combine: Option<&CoreMatch>,
    filename: &str,
    args: &Args,
) -> Result<MatchSource> {
    if args.league {
        return league
            .map(|_| MatchSource::League)
            .ok_or_else(|| anyhow!("Core league match id {id} was not found"));
    }
    if args.combine {
        return combine
            .map(|_| MatchSource::Combine)
            .ok_or_else(|| anyhow!("Core combine match id {id} was not found"));
    }

    match (league, combine) {
        (Some(_), None) => Ok(MatchSource::League),
        (None, None) => Err(anyhow!("Core match id {id} was not found")),
        (None, Some(combine)) => {
            // A post-merge Combine id is unique; only a legacy id hit needs filename evidence.
            if combine.id == id
                || demo_url_matches_filename(combine.demo_url.as_deref(), filename)
                || filename_indicates_combine(filename)
            {
                Ok(MatchSource::Combine)
            } else {
                Err(anyhow!(
                    "Core match id {id} was not found as a league match; use --combine to select the colliding combine/FA Colo row"
                ))
            }
        }
        (Some(league), Some(combine)) => {
            let league_matches = demo_url_matches_filename(league.demo_url.as_deref(), filename);
            let combine_matches = demo_url_matches_filename(combine.demo_url.as_deref(), filename);
            match (league_matches, combine_matches) {
                (true, false) => Ok(MatchSource::League),
                (false, true) => Ok(MatchSource::Combine),
                _ if filename_indicates_combine(filename) => Ok(MatchSource::Combine),
                _ => Ok(MatchSource::League),
            }
        }
    }
}

fn demo_url_matches_filename(demo_url: Option<&str>, filename: &str) -> bool {
    let filename = normalized_demo_basename(filename);
    demo_url
        .and_then(|url| url.rsplit('/').next())
        .map(normalized_demo_basename)
        .map(|name| name == filename)
        .unwrap_or(false)
}

fn normalized_demo_basename(value: &str) -> String {
    let mut normalized = value
        .split(['?', '#'])
        .next()
        .unwrap_or(value)
        .to_ascii_lowercase();
    loop {
        let Some(stripped) = [".7z", ".zip", ".dem"]
            .iter()
            .find_map(|suffix| normalized.strip_suffix(suffix))
        else {
            return normalized;
        };
        normalized = stripped.to_owned();
    }
}

fn filename_indicates_combine(filename: &str) -> bool {
    let filename = filename.to_ascii_lowercase();
    filename.contains("combine") || filename.contains("fa-colo") || filename.contains("fa_colo")
}

fn legacy_file_is_combine(args: &Args, filename: &str) -> bool {
    args.combine || (!args.league && filename_indicates_combine(filename))
}

#[cfg(test)]
mod tests {
    use super::{
        demo_url_matches_filename, direct_import_skip, forfeit, legacy_file_is_combine, match_info,
        select_match_source, split_candidates, stats_match_type, Args, CoreMatch, MatchSource,
    };

    fn args() -> Args {
        Args {
            command: None,
            directory: None,
            tier: None,
            season: None,
            match_day: None,
            fix_core_scores: None,
            fix_team_names: None,
            combine: false,
            league: false,
        }
    }

    fn league(demo_url: Option<&str>) -> CoreMatch {
        CoreMatch {
            id: 8088,
            match_type: "REGULATION".to_owned(),
            is_playoff: false,
            is_bo3: false,
            season: 20,
            tier: Some("Premier".to_owned()),
            match_day: Some("M10".to_owned()),
            match_date: None,
            demo_url: demo_url.map(str::to_owned),
        }
    }

    /// A merged FA Colo row whose legacy id 8088 collides with the regulation match.
    fn combine(demo_url: Option<&str>) -> CoreMatch {
        CoreMatch {
            id: 19773,
            match_type: "FA_COLO".to_owned(),
            is_playoff: false,
            is_bo3: false,
            season: 20,
            tier: Some("Premier".to_owned()),
            match_day: None,
            match_date: None,
            demo_url: demo_url.map(str::to_owned),
        }
    }

    #[test]
    fn archive_extensions_are_trimmed_symmetrically_and_names_compare_exactly() {
        assert!(demo_url_matches_filename(
            Some("https://cscdemos.nyc3.cdn.digitaloceanspaces.com/s20/M10/s20-M10-Demons-vs-Foo-mid8088-0_de_anubis.dem.zip"),
            "s20-M10-Demons-vs-Foo-mid8088-0_de_anubis.dem.7z"
        ));
        assert!(!demo_url_matches_filename(
            Some("https://example.invalid/s20/prefix-s20-mid8088-map1.7z"),
            "s20-mid8088-map1.dem"
        ));
    }

    #[test]
    fn persisted_urls_resolve_a_realistic_colliding_id() {
        // Core id 8005 exists in both tables with these production-shaped URLs.
        let league = league(Some(
            "https://f005.backblazeb2.com/file/csc-demo-archive/s19/M07/s19-M07-PhoFighters-vs-Nightshades-mid8005.7z",
        ));
        let combine = combine(Some(
            "https://cscdemos.nyc3.cdn.digitaloceanspaces.com/s20/Combines/05-03/combine-contender-mid8005-0_de_nuke-2026-05-04_05-56-55.dem.zip",
        ));
        let args = args();

        assert_eq!(
            select_match_source(
                8005,
                Some(&league),
                Some(&combine),
                "s19-M07-PhoFighters-vs-Nightshades-mid8005.dem",
                &args
            )
            .unwrap(),
            MatchSource::League
        );
        assert_eq!(
            select_match_source(
                8005,
                Some(&league),
                Some(&combine),
                "combine-contender-mid8005-0_de_nuke-2026-05-04_05-56-55.dem.7z",
                &args
            )
            .unwrap(),
            MatchSource::Combine
        );
    }

    #[test]
    fn collision_without_url_evidence_uses_filename_then_defaults_to_league() {
        let league = league(None);
        let combine = combine(None);
        let args = args();

        assert_eq!(
            select_match_source(
                8088,
                Some(&league),
                Some(&combine),
                "manual-combine-mid8088.dem",
                &args
            )
            .unwrap(),
            MatchSource::Combine
        );
        assert_eq!(
            select_match_source(
                8088,
                Some(&league),
                Some(&combine),
                "renamed-mid8088.dem",
                &args
            )
            .unwrap(),
            MatchSource::League
        );
    }

    #[test]
    fn combine_only_row_requires_positive_evidence_or_explicit_flag() {
        let combine = combine(None);
        let mut args = args();
        assert!(select_match_source(
            8088,
            None,
            Some(&combine),
            "historical-league-mid8088.dem",
            &args
        )
        .is_err());

        args.combine = true;
        assert_eq!(
            select_match_source(8088, None, Some(&combine), "renamed-mid8088.dem", &args).unwrap(),
            MatchSource::Combine
        );
    }

    #[test]
    fn no_mid_fallback_preserves_combine_typing_and_honors_overrides() {
        let mut args = args();
        assert!(legacy_file_is_combine(
            &args,
            "season20-combine-contender.dem"
        ));

        args.league = true;
        assert!(!legacy_file_is_combine(
            &args,
            "season20-combine-contender.dem"
        ));

        args.league = false;
        args.combine = true;
        assert!(legacy_file_is_combine(&args, "renamed-recording.dem"));
    }

    #[test]
    fn stats_match_type_maps_core_types() {
        assert_eq!(stats_match_type("REGULATION", false).unwrap(), "Regulation");
        assert_eq!(stats_match_type("REGULATION", true).unwrap(), "Playoff");
        assert_eq!(stats_match_type("COMBINE", false).unwrap(), "Combine");
        assert_eq!(stats_match_type("FA_COLO", false).unwrap(), "FAColo");
        assert!(stats_match_type("SCRIM", false).is_err());
        assert!(stats_match_type("PRESEASON", false).is_err());
    }

    #[test]
    fn two_candidates_split_and_disambiguate_by_filename() {
        let league = league(Some(
            "https://f005.backblazeb2.com/file/csc-demo-archive/s19/M07/s19-M07-PhoFighters-vs-Nightshades-mid8088.7z",
        ));
        let rows = [combine(None), league];
        let (league, combine) = split_candidates(8088, &rows).unwrap();
        let args = args();

        let pick =
            |filename| match select_match_source(8088, league, combine, filename, &args).unwrap() {
                MatchSource::League => league.unwrap(),
                MatchSource::Combine => combine.unwrap(),
            };
        let regulation =
            match_info(pick("s19-M07-PhoFighters-vs-Nightshades-mid8088.dem")).unwrap();
        assert_eq!(regulation.match_id, "8088");
        assert_eq!(regulation.match_type, "Regulation");
        assert_eq!(regulation.match_day, "M10");

        let fa_colo = match_info(pick("fa-colo-Premier-mid8088-0_de_nuke.dem")).unwrap();
        // A legacy `mid8088` file posts the row's own Core id, never the legacy id.
        assert_eq!(fa_colo.match_id, "19773");
        assert_eq!(fa_colo.tier, None);
        assert_eq!(fa_colo.match_type, "FAColo");
        assert_eq!(fa_colo.match_day, "");
    }

    #[test]
    fn two_rows_of_the_same_kind_are_rejected() {
        assert!(split_candidates(8088, &[combine(None), combine(None)]).is_err());
        assert!(split_candidates(8088, &[league(None), league(None)]).is_err());
    }

    #[test]
    fn post_merge_combine_id_needs_no_filename_evidence() {
        let mut combine = combine(None);
        combine.id = 20001;
        assert_eq!(
            select_match_source(20001, None, Some(&combine), "renamed-mid20001.dem", &args())
                .unwrap(),
            MatchSource::Combine
        );
        assert_eq!(match_info(&combine).unwrap().match_id, "20001");
    }

    #[test]
    fn combine_sends_no_tier_but_regulation_keeps_it() {
        let body = |info: super::MatchInfo| {
            serde_json::to_value(super::StatsRequestBody {
                path: String::new(),
                match_id: info.match_id,
                season: info.season,
                tier: info.tier,
                match_day: info.match_day,
                match_type: info.match_type,
                match_date: None,
                fix_core_scores: false,
                fix_team_names: false,
            })
            .unwrap()
        };
        // The Combine row still carries a tier (old Core data).
        let combine = body(match_info(&combine(None)).unwrap());
        assert!(combine.get("tier").is_none());
        assert_eq!(combine["matchId"], "19773");
        let regulation = body(match_info(&league(None)).unwrap());
        assert_eq!(regulation["tier"], "Premier");
    }

    #[test]
    fn scrim_rows_are_not_imported() {
        let mut scrim = league(None);
        scrim.match_type = "SCRIM".to_owned();
        assert!(match_info(&scrim).is_err());
    }

    fn guard(rows: &[forfeit::MapRow]) -> forfeit::MapGuard {
        forfeit::MapGuard::from_rows(rows)
    }

    #[test]
    fn directory_import_allows_adjusted_zero_thirteen_and_bo3_thirteen_zero() {
        let adjusted = guard(&[forfeit::test_rows::played(6034, 1, 0, 13)]);
        assert_eq!(direct_import_skip(&adjusted, 1), None);
        let bo3 = guard(&[
            forfeit::test_rows::played(9001, 1, 13, 0),
            forfeit::test_rows::played(9001, 2, 7, 13),
        ]);
        assert_eq!(direct_import_skip(&bo3, 1), None);
        assert_eq!(direct_import_skip(&bo3, 2), None);
    }

    #[test]
    fn directory_import_skips_only_the_forfeited_map() {
        let mut forfeited = forfeit::test_rows::played(9001, 1, 0, 13);
        forfeited.is_forfeit = true;
        let bo3 = guard(&[forfeited, forfeit::test_rows::played(9001, 2, 13, 5)]);
        assert!(direct_import_skip(&bo3, 1).is_some());
        assert_eq!(direct_import_skip(&bo3, 2), None);
        let legacy = guard(&[forfeit::test_rows::played(9003, 1, 0, 1)]);
        assert_eq!(
            direct_import_skip(&legacy, 1),
            Some(forfeit::SkipReason::LegacyOneZero)
        );
    }

    #[test]
    fn directory_import_does_not_skip_a_reversed_forfeit() {
        let mut reversed = forfeit::test_rows::played(9001, 1, 0, 0);
        reversed.audit_new_score = Some("0 - 0".to_owned());
        assert_eq!(direct_import_skip(&guard(&[reversed]), 1), None);
    }

    #[test]
    fn directory_import_skips_reviewed_void_maps() {
        let void = guard(&[forfeit::test_rows::played(7339, 1, 0, 0)]);
        assert_eq!(
            direct_import_skip(&void, 1),
            Some(forfeit::SkipReason::ReviewedVoid)
        );
    }
}
