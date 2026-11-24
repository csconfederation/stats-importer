//! Core's `matches_demoprocessingstatus.stats_demos`: the recordings Stats gets,
//! one per played map in map order, `[{"map", "demo_path", "s3_key"}]`. A map
//! with no recording keeps its position with a null `demo_path`, so list index
//! `i` is Core map `i + 1` and Stats id suffix `_i` (Core's `stats_key`). Null
//! on rows that predate the field; those read `s3_keys` as the map list.

use anyhow::{anyhow, bail, Result};
use serde::Deserialize;
use serde_json::Value;
use sqlx::PgPool;

use crate::forfeit::MapGuard;

#[derive(Debug, Clone, Deserialize)]
pub struct StatsDemo {
    pub map: Option<String>,
    pub demo_path: Option<String>,
    pub s3_key: Option<String>,
}

/// One recorded map's archive, resolved to its Core map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MapArtifact {
    /// Core's 1-based `map_number`: the forfeit/void guard and the Stats id.
    pub core_map_number: i32,
    pub key: String,
    pub map_name: String,
}

impl MapArtifact {
    /// Stats id suffix: Stats reads `{match}_{n}` as Core map `n + 1`
    /// (CSC-Stats `manual-score.ts`), so it is never the key's list position.
    pub fn stats_index(&self) -> i32 {
        self.core_map_number - 1
    }
}

/// `None` for NULL or an empty list (Core treats both as absent).
pub fn parse(value: Option<&Value>) -> Result<Option<Vec<StatsDemo>>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => {
            let entries: Vec<StatsDemo> = serde_json::from_value(value.clone())
                .map_err(|error| anyhow!("stats_demos is malformed: {error}"))?;
            Ok((!entries.is_empty()).then_some(entries))
        }
    }
}

/// Core's `uploaded_stats_demos`: usable once every recorded map has a key.
pub fn all_keyed(entries: &[StatsDemo]) -> bool {
    entries
        .iter()
        .filter(|entry| entry.demo_path.is_some())
        .all(|entry| entry.s3_key.as_deref().is_some_and(|key| !key.is_empty()))
}

/// Resolves each keyed entry to its Core map by list position and checks that
/// the archive key names that map, so a position that disagrees with Core's map
/// fails closed instead of importing the wrong map.
pub fn artifacts(entries: &[StatsDemo], guard: &MapGuard) -> Result<Vec<MapArtifact>> {
    let mut resolved = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        let Some(key) = entry.s3_key.as_deref().filter(|key| !key.is_empty()) else {
            continue;
        };
        let core_map_number = i32::try_from(index + 1)?;
        let map_name = guard
            .map_name(core_map_number)
            .or(entry.map.as_deref().filter(|name| !name.is_empty()))
            .ok_or_else(|| {
                anyhow!("stats_demos[{index}] ({key}): Core map {core_map_number} has no map name")
            })?;
        if !key
            .to_ascii_lowercase()
            .contains(&map_name.to_ascii_lowercase())
        {
            bail!(
                "stats_demos[{index}] ({key}) does not contain Core map {core_map_number}'s name ({map_name:?})"
            );
        }
        resolved.push(MapArtifact {
            core_map_number,
            key: key.to_owned(),
            map_name: map_name.to_owned(),
        });
    }
    Ok(resolved)
}

/// A file name or path's basename, lowercased, without query/fragment or
/// `.7z`/`.zip`/`.dem` suffixes.
pub fn normalized_demo_basename(value: &str) -> String {
    let value = value.rsplit('/').next().unwrap_or(value);
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

/// Core's map for one BO3 recording. The `-N_` digit is unreliable (recordings
/// can share `-0_`, Core-planning #242), and Core lists recordings in path order
/// when it can't resolve the veto order, so neither is trusted over Core's map
/// names. In order:
/// - the one Core map whose `map_name` the filename contains, unless a real map
///   has no name (it may be the same map);
/// - if the match has a forfeit/void map, nothing else: reject;
/// - the `stats_demos` entry naming this file, else `fallback` (the digit or
///   archive position); when the filename names several Core maps (one map
///   played twice), the result must be one of them.
///
/// The result decides both the forfeit guard and the Stats id suffix, so
/// anything else is rejected.
pub fn resolve_core_map_number(
    filename: &str,
    fallback: Option<i32>,
    entries: &[StatsDemo],
    guard: &MapGuard,
) -> Result<i32> {
    let named = guard.map_numbers_named_in(filename);
    let unnamed_beside_skip = guard.has_skipped_maps() && guard.has_unnamed_real_map();
    if let [number] = named.as_slice() {
        if !unnamed_beside_skip {
            return Ok(*number);
        }
    }
    if guard.has_skipped_maps() {
        bail!(
            "{filename} names Core map(s) {named:?}, which can't prove its map, and the match \
             has forfeit/void maps"
        );
    }
    let name = normalized_demo_basename(filename);
    let recorded: Vec<usize> = entries
        .iter()
        .enumerate()
        .filter(|(_, entry)| {
            [&entry.demo_path, &entry.s3_key]
                .into_iter()
                .flatten()
                .any(|path| normalized_demo_basename(path) == name)
        })
        .map(|(index, _)| index)
        .collect();
    let map = match recorded.as_slice() {
        [index] => i32::try_from(index + 1)?,
        [] => fallback.ok_or_else(|| anyhow!("{filename} has no Core map number"))?,
        many => bail!(
            "{filename} matches {} stats_demos entries ({many:?})",
            many.len()
        ),
    };
    if !named.is_empty() && !named.contains(&map) {
        bail!("{filename} resolves to Core map {map} but names Core maps {named:?}");
    }
    Ok(map)
}

pub async fn fetch(pool: &PgPool, match_id: i64) -> Result<Option<Vec<StatsDemo>>> {
    let value = sqlx::query_scalar::<_, Option<Value>>(
        "SELECT stats_demos FROM matches_demoprocessingstatus WHERE match_id = $1",
    )
    .bind(match_id)
    .fetch_optional(pool)
    .await?
    .flatten();
    parse(value.as_ref())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::forfeit::{test_rows::played, MapRow};

    fn named(match_id: i64, map: i32, name: &str) -> MapRow {
        MapRow {
            map_name: Some(name.to_owned()),
            ..played(match_id, map, 13, 5)
        }
    }

    #[test]
    fn null_and_empty_are_absent_and_malformed_is_an_error() {
        assert!(parse(None).unwrap().is_none());
        assert!(parse(Some(&Value::Null)).unwrap().is_none());
        assert!(parse(Some(&json!([]))).unwrap().is_none());
        assert!(parse(Some(&json!("nope"))).is_err());
    }

    #[test]
    fn a_missing_recording_keeps_its_position() {
        let entries = parse(Some(&json!([
            {"map": null, "demo_path": null, "s3_key": null},
            {"map": "de_nuke", "demo_path": "/d/a.dem", "s3_key": "s20/a_de_nuke.dem.zip"},
            {"map": "de_inferno", "demo_path": "/d/b.dem", "s3_key": "s20/b_de_inferno.dem.zip"},
        ])))
        .unwrap()
        .unwrap();
        assert!(all_keyed(&entries));
        let guard = MapGuard::from_rows(&[
            named(9001, 1, "de_anubis"),
            named(9001, 2, "de_nuke"),
            named(9001, 3, "de_inferno"),
        ]);
        let resolved = artifacts(&entries, &guard).unwrap();
        let positions: Vec<_> = resolved
            .iter()
            .map(|artifact| (artifact.stats_index(), artifact.core_map_number))
            .collect();
        assert_eq!(positions, vec![(1, 2), (2, 3)]);
    }

    #[test]
    fn an_unkeyed_recording_makes_the_list_unusable() {
        let entries = parse(Some(&json!([
            {"map": "de_nuke", "demo_path": "/d/a.dem", "s3_key": null},
        ])))
        .unwrap()
        .unwrap();
        assert!(!all_keyed(&entries));
    }

    #[test]
    fn a_unique_name_proves_nothing_beside_an_unnamed_forfeited_map() {
        // Map 1's forfeited recording may also be Nuke.
        let mut forfeited = played(9001, 1, 0, 13);
        forfeited.is_forfeit = true;
        let guard = MapGuard::from_rows(&[forfeited.clone(), named(9001, 2, "de_nuke")]);
        let filename = "s20-mid9001-0_de_nuke.dem";
        assert!(resolve_core_map_number(filename, Some(1), &[], &guard).is_err());
        forfeited.map_name = Some("de_anubis".to_owned());
        let guard = MapGuard::from_rows(&[forfeited, named(9001, 2, "de_nuke")]);
        assert_eq!(
            resolve_core_map_number(filename, Some(1), &[], &guard).unwrap(),
            2
        );
    }

    #[test]
    fn a_key_for_the_wrong_map_fails_closed() {
        let entries = parse(Some(&json!([
            {"map": "de_nuke", "demo_path": "/d/a.dem", "s3_key": "s20/a_de_nuke.dem.zip"},
        ])))
        .unwrap()
        .unwrap();
        let guard = MapGuard::from_rows(&[named(9001, 1, "de_anubis")]);
        assert!(artifacts(&entries, &guard).is_err());
        assert!(artifacts(&entries, &MapGuard::default()).is_ok());
    }
}
