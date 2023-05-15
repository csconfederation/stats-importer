use std::{
    env,
    fs::{self},
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Result};
use clap::Parser;
use regex::Regex;
use serde::Serialize;
use sqlx::{FromRow, PgPool};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// demos directory
    #[arg(short, long)]
    directory: String,

    /// override tier (optional)
    #[arg(short, long)]
    tier: Option<String>,

    /// override season (optional)
    #[arg(short, long)]
    season: Option<u8>,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().expect(".env not found");
    let args = Args::parse();
    let dir = Path::new(&args.directory);
    if !dir.is_dir() {
        println!("'{}' is not a directory", args.directory);
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
            let mut info = get_core_match(id, pool, filename.contains("combine"), &args).await?;
            info.match_id = if filename.contains("combine") {
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
            MatchInfo {
                match_id: Some(filename.clone().to_string()),
                tier,
                season: i32::from(season),
                is_series: false,
            }
        }
    };

    let file_path = String::from(path.as_path().to_str().unwrap());
    let req_root_dir = env::var("REQUEST_ROOT_DIR");
    let req_path = match req_root_dir {
        Ok(root_dir) => format!("{}/{}", root_dir, filename),
        Err(_) => file_path,
    };
    let client = reqwest::Client::new();
    let url = format!(
        "{}/api/add-match",
        env::var("STATS_API_URL").expect("STATS_API_URL expected")
    );

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
    };
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
}
#[derive(Debug, FromRow, Clone)]
struct MatchInfo {
    match_id: Option<String>,
    season: i32,
    tier: String,
    is_series: bool,
}
#[derive(Debug, FromRow, Clone)]
struct CombineMatch {
    match_id: Option<String>,
    tier: String,
}

async fn get_core_match(
    id: i64,
    pool: &PgPool,
    is_combine: bool,
    args: &Args,
) -> Result<MatchInfo> {
    if !is_combine {
        Ok(sqlx::query_as!(
            MatchInfo,
            "
        select mm.id::varchar as match_id, ls.number as season, pt.name as tier, is_bo3 as is_series
            from matches_matches mm
                join leagues_matchday lm on lm.id = mm.match_day_id
                join leagues_seasons ls on ls.id = lm.season_id
                join teams_teams tt on mm.home_id = tt.id
                join players_tiers pt on tt.tier_id = pt.id
        where mm.id = $1;
    ",
            id
        )
        .fetch_one(pool)
        .await?)
    } else {
        let m = sqlx::query_as!(
            CombineMatch,
            "
        select mm.id::varchar as match_id, pt.name as tier
            from matches_combinematches mm
                join players_tiers pt on mm.tier_id = pt.id
        where mm.id = $1;
    ",
            id
        )
        .fetch_one(pool)
        .await?;
        let Some(season) = args.season else {
            return Err(anyhow!("--season not provided for combine match"));
        };
        Ok(MatchInfo {
            match_id: m.match_id,
            season: season.try_into().unwrap(),
            tier: m.tier,
            is_series: false,
        })
    }
}
