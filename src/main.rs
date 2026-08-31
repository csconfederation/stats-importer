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
            let mut info = get_core_match(id, pool, filename, &args).await?;
            info.match_id = if info.is_combine {
                Some(format!("combines-{}", info.match_id.unwrap()))
            } else {
                info.match_id
            };
            info
        }
        None => {
            println!("Cannot parse match id from filename, using args...");
            let Some(season) = args.season else {
                return Err(anyhow!("--season arg not provided, skipping..."));
            };
            let Some(tier) = args.tier else {
                return Err(anyhow!("--tier arg not provided, skipping..."));
            };
            let Some(match_day) = args.match_day else {
                return Err(anyhow!("--match_day arg not provided, skipping..."));
            };
            MatchInfo {
                match_id: Some(filename.to_string()),
                tier,
                season: i32::from(season),
                match_day,
                is_series: false,
                match_date: None,
                is_combine: false,
                match_type: "Regulation".to_owned(),
                demo_url: None,
            }
        }
    };

    let file_path = String::from(path.as_path().to_str().unwrap());
    let req_root_dir = env::var("REQUEST_ROOT_DIR");
    let req_path = match req_root_dir {
        Ok(root_dir) => format!("{}/{}", root_dir, filename),
        Err(_) => file_path,
    };

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
        format!("_{}", map_number)
    } else {
        String::new()
    };
    let body = StatsRequestBody {
        path: req_path,
        match_id: format!("{}{}", match_info.match_id.unwrap(), map_num_str),
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
    tier: String,
    match_day: String,
    match_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    match_date: Option<String>,
    fix_core_scores: bool,
    fix_team_names: bool,
}
#[derive(Debug, FromRow, Clone)]
struct MatchInfo {
    match_id: Option<String>,
    season: i32,
    tier: String,
    match_day: String,
    is_series: bool,
    match_date: Option<String>,
    is_combine: bool,
    match_type: String,
    demo_url: Option<String>,
}

#[derive(Debug, FromRow, Clone)]
struct CombineMatchInfo {
    match_id: Option<String>,
    season: Option<i32>,
    tier: String,
    match_date: Option<String>,
    match_type: String,
    demo_url: Option<String>,
}

async fn get_core_match(id: i64, pool: &PgPool, filename: &str, args: &Args) -> Result<MatchInfo> {
    let league_match = sqlx::query_as::<_, MatchInfo>(
        r#"
        select mm.id::varchar as match_id,
               ls.number as season,
               pt.name as tier,
               is_bo3 as is_series,
               lm.number as match_day,
               to_char(coalesce(mm.completed_at, mm.scheduled_date) AT TIME ZONE 'UTC',
                       'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as match_date,
               false as is_combine,
               case when is_bo3 then 'Playoff' else 'Regulation' end::text as match_type,
               mm.demo_url
            from matches_matches mm
                join leagues_matchday lm on lm.id = mm.match_day_id
                join leagues_seasons ls on ls.id = lm.season_id
                join teams_teams ht on mm.home_id = ht.id
                join teams_teams at on mm.away_id = at.id
                join players_tiers pt on ht.tier_id = pt.id
                join matches_matchlobby ml on ml.id = mm.lobby_id
        where mm.id = $1;
    "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    let combine_match = sqlx::query_as::<_, CombineMatchInfo>(
        r#"
        select mm.id::varchar as match_id,
               ls.number as season,
               pt.name as tier,
               to_char(coalesce(mm.game_finished_at, mm.scheduled_date) AT TIME ZONE 'UTC',
                       'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') as match_date,
               case mm.queue_mode
                 when 'fa_colo' then 'FAColo'
                 else 'Combine'
               end::text as match_type,
               mm.demo_url
            from matches_combinematches mm
                join players_tiers pt on mm.tier_id = pt.id
                left join leagues_seasons ls on ls.id = mm.season_id
        where mm.id = $1;
    "#,
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;

    let combine_match = combine_match
        .map(|combine| {
            let season = combine.season.or(args.season.map(i32::from)).ok_or_else(|| {
                anyhow!(
                    "Core combine match {id} has no persisted season; provide --season for this legacy row"
                )
            })?;
            Ok::<MatchInfo, anyhow::Error>(MatchInfo {
                match_id: combine.match_id,
                season,
                tier: combine.tier,
                match_day: String::new(),
                is_series: false,
                match_date: combine.match_date,
                is_combine: true,
                match_type: combine.match_type,
                demo_url: combine.demo_url,
            })
        })
        .transpose()?;

    match (league_match, combine_match) {
        (Some(league), None) => Ok(league),
        (None, Some(combine)) => Ok(combine),
        (None, None) => Err(anyhow!("Core match id {id} was not found")),
        (Some(league), Some(combine)) => {
            // The two Core tables have independent id sequences. When an id
            // exists in both, use Core's persisted demo URL to resolve the
            // local file; the filename itself is never a match-type signal.
            let league_matches = demo_url_matches_filename(league.demo_url.as_deref(), filename);
            let combine_matches = demo_url_matches_filename(combine.demo_url.as_deref(), filename);
            match (league_matches, combine_matches) {
                (true, false) => Ok(league),
                (false, true) => Ok(combine),
                _ => Err(anyhow!(
                    "Core match id {id} exists in both league and combine tables and demo_url does not identify {filename} uniquely"
                )),
            }
        }
    }
}

fn demo_url_matches_filename(demo_url: Option<&str>, filename: &str) -> bool {
    let filename = filename
        .trim_end_matches(".zip")
        .trim_end_matches(".dem")
        .to_ascii_lowercase();
    demo_url
        .and_then(|url| url.rsplit('/').next())
        .map(|name| {
            name.trim_end_matches(".7z")
                .trim_end_matches(".zip")
                .trim_end_matches(".dem")
                .to_ascii_lowercase()
                .contains(&filename)
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::demo_url_matches_filename;

    #[test]
    fn persisted_demo_url_resolves_a_colliding_core_id() {
        assert!(demo_url_matches_filename(
            Some("https://example.invalid/s20/FA-Colo-s20-mid42-map1.7z"),
            "FA-Colo-s20-mid42-map1.dem"
        ));
        assert!(!demo_url_matches_filename(
            Some("https://example.invalid/s20/combine-s20-mid42-map1.7z"),
            "FA-Colo-s20-mid42-map1.dem"
        ));
    }
}
