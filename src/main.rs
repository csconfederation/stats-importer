use std::{
    env,
    fs::{self, File},
    io,
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
                println!("{}", exsiting.display());
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
            get_core_match(id, pool).await?
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
                tier: tier,
                season: i32::from(season),
            }
        }
    };

    let mut file_path = String::from(path.as_path().to_str().unwrap());

    let is_zip = path.extension().unwrap() == "zip";
    if is_zip {
        file_path = unzip_file(path.clone(), &args.directory)?;
    }
    let req_root_dir = env::var("REQUEST_ROOT_DIR");
    let req_path = match req_root_dir {
        Ok(root_dir) => format!("{}/{}", root_dir, filename),
        Err(_) => file_path,
    };
    println!("{}", &req_path);
    let client = reqwest::Client::new();
    let url = format!(
        "{}/api/add-match",
        env::var("STATS_API_URL").expect("STATS_API_URL expected")
    );
    let body = StatsRequestBody {
        path: req_path,
        match_id: match_info.match_id.unwrap(),
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
#[derive(Debug, FromRow)]
struct MatchInfo {
    match_id: Option<String>,
    season: i32,
    tier: String,
}

async fn get_core_match(id: i64, pool: &PgPool) -> Result<MatchInfo> {
    Ok(sqlx::query_as!(
        MatchInfo,
        "
        select mm.id::varchar as match_id, ls.number as season, pt.name as tier
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
}

fn unzip_file(path: PathBuf, dir: &String) -> Result<String> {
    let file = File::open(&path).unwrap();
    let mut archive = zip::ZipArchive::new(file).unwrap();
    let mut file = archive.by_index(0).unwrap();
    let outpath = match file.enclosed_name() {
        Some(path) => path.to_owned(),
        None => return Err(anyhow!("Zip contains no file".to_owned())),
    };

    // println!("{:#?}", &path.as_os_str());
    let new_path = format!("{}/{}", dir, &outpath.display());
    let mut outfile = fs::File::create(&new_path).unwrap();
    io::copy(&mut file, &mut outfile)?;
    fs::remove_file(path)?;

    // Get and Set permissions
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        if let Some(mode) = file.unix_mode() {
            fs::set_permissions(&new_path, fs::Permissions::from_mode(mode)).unwrap();
        }
    }
    Ok(String::from(new_path))
}
