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
    /// Stats id suffix (`{match}_{stats_index}`).
    pub stats_index: usize,
    /// Core's 1-based `map_number`, for the forfeit/void guard.
    pub core_map_number: i32,
    pub key: String,
    pub map_name: String,
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
            stats_index: index,
            core_map_number,
            key: key.to_owned(),
            map_name: map_name.to_owned(),
        });
    }
    Ok(resolved)
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
            .map(|artifact| (artifact.stats_index, artifact.core_map_number))
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
