//! A BO3's existing Stats ids (`{match_id}_{n}`, Core map `n + 1`). Earlier
//! imports keyed some series by position (1-based, or `_0`/`_1` for maps 1, 3).
//! Stats replaces only the id it is sent, so a canonical import beside such a
//! row leaves a duplicate map; those rows need a rekey first.

use anyhow::{bail, Context, Result};
use reqwest::Client;
use serde_json::{json, Value};

use crate::forfeit::MapGuard;

const QUERY: &str = "query($prefix: String!) { \
    findManyMatch(where: { matchId: { startsWith: $prefix } }) { matchId mapName } }";

/// One existing Stats match row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatsMatch {
    pub match_id: String,
    pub map_name: String,
}

/// Stats' matches whose id starts `{match_id}_`, from its GraphQL endpoint.
pub async fn fetch(client: &Client, stats_url: &str, match_id: i64) -> Result<Vec<StatsMatch>> {
    let response = client
        .post(format!("{}/graphql", stats_url.trim_end_matches('/')))
        .json(&json!({ "query": QUERY, "variables": { "prefix": format!("{match_id}_") } }))
        .send()
        .await
        .context("Stats GraphQL request failed")?;
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .context("Stats GraphQL returned non-JSON")?;
    parse(status.is_success(), &body)
}

fn parse(success: bool, body: &Value) -> Result<Vec<StatsMatch>> {
    if !success || body.get("errors").is_some_and(|errors| !errors.is_null()) {
        bail!("Stats GraphQL findManyMatch failed: {body}");
    }
    let Some(rows) = body
        .pointer("/data/findManyMatch")
        .and_then(Value::as_array)
    else {
        bail!("Stats GraphQL findManyMatch returned no list: {body}");
    };
    rows.iter()
        .map(|row| {
            let field = |name: &str| {
                row.get(name)
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .with_context(|| format!("Stats match row has no {name}: {row}"))
            };
            Ok(StatsMatch {
                match_id: field("matchId")?,
                map_name: field("mapName")?,
            })
        })
        .collect()
}

/// Existing ids of this match that hold no Core map other than `writing`: the
/// map (`n + 1`) is not real (see `MapGuard::is_real_map`), or Stats recorded a
/// different map there than Core. The name check catches a played map stored
/// under a forfeited map's key. Ids of other matches that share the prefix
/// (`startsWith` treats `_` as a wildcard) are ignored.
pub fn stale(
    match_id: i64,
    existing: &[StatsMatch],
    guard: &MapGuard,
    writing: Option<i32>,
) -> Vec<String> {
    let prefix = format!("{match_id}_");
    let mut stale: Vec<String> = existing
        .iter()
        .filter(|row| {
            let Some(suffix) = row.match_id.strip_prefix(&prefix) else {
                return false;
            };
            if suffix.is_empty() || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
                return false;
            }
            let map = suffix
                .parse::<i32>()
                .map_or(i32::MAX, |n| n.saturating_add(1));
            // Stats writes Core's map_name from its own mapName, so they compare exactly.
            Some(map) != writing
                && (!guard.is_real_map(map)
                    || guard
                        .map_name(map)
                        .is_some_and(|name| !name.eq_ignore_ascii_case(&row.map_name)))
        })
        .map(|row| row.match_id.clone())
        .collect();
    stale.sort();
    stale
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forfeit::{test_rows::played, MapRow};

    fn ids(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    /// Stats rows `(matchId, mapName)`.
    fn rows(values: &[(&str, &str)]) -> Vec<StatsMatch> {
        values
            .iter()
            .map(|(id, map)| StatsMatch {
                match_id: (*id).to_owned(),
                map_name: (*map).to_owned(),
            })
            .collect()
    }

    /// Unnamed Stats rows: only the id decides.
    fn unnamed(values: &[&str]) -> Vec<StatsMatch> {
        rows(&values.iter().map(|id| (*id, "de_x")).collect::<Vec<_>>())
    }

    fn placeholder(match_id: i64, map: i32) -> MapRow {
        MapRow {
            has_winner: false,
            ..played(match_id, map, 0, 0)
        }
    }

    fn named(row: MapRow, name: &str) -> MapRow {
        MapRow {
            map_name: Some(name.to_owned()),
            ..row
        }
    }

    #[test]
    fn a_reversed_forfeit_with_canonical_ids_is_not_stale() {
        // Map 1's forfeit was reversed through Core (0-0, no winner, audited).
        let mut reversed = placeholder(9001, 1);
        reversed.has_audit = true;
        let guard = MapGuard::from_rows(&[reversed, played(9001, 2, 13, 5), placeholder(9001, 3)]);
        assert!(stale(9001, &unnamed(&["9001_0", "9001_1"]), &guard, None).is_empty());
        // An unaudited hand reversal reads the same: a 0-0 map before a played one.
        let guard = MapGuard::from_rows(&[
            placeholder(9001, 1),
            played(9001, 2, 13, 5),
            placeholder(9001, 3),
        ]);
        assert!(stale(9001, &unnamed(&["9001_0", "9001_1"]), &guard, None).is_empty());
    }

    #[test]
    fn a_one_based_legacy_sweep_is_stale() {
        let guard = MapGuard::from_rows(&[
            played(9001, 1, 13, 5),
            played(9001, 2, 13, 7),
            placeholder(9001, 3),
        ]);
        assert_eq!(
            stale(9001, &unnamed(&["9001_1", "9001_2"]), &guard, None),
            ids(&["9001_2"])
        );
        assert!(stale(9001, &unnamed(&["9001_0", "9001_1"]), &guard, None).is_empty());
    }

    #[test]
    fn a_positional_gap_series_is_stale() {
        let guard = MapGuard::from_rows(&[played(9001, 1, 13, 5), played(9001, 3, 13, 7)]);
        assert_eq!(
            stale(9001, &unnamed(&["9001_0", "9001_1"]), &guard, Some(3)),
            ids(&["9001_1"])
        );
        assert!(stale(9001, &unnamed(&["9001_0", "9001_2"]), &guard, None).is_empty());
    }

    #[test]
    fn a_played_map_stored_under_a_forfeited_maps_key_is_stale() {
        // Map 1 forfeited unrecorded; positional ids put Nuke at `_0`, Inferno at `_1`.
        let mut forfeited = named(played(9001, 1, 0, 13), "de_anubis");
        forfeited.is_forfeit = true;
        let guard = MapGuard::from_rows(&[
            forfeited,
            named(played(9001, 2, 13, 5), "de_nuke"),
            named(played(9001, 3, 9, 13), "de_inferno"),
        ]);
        let legacy = rows(&[("9001_0", "de_nuke"), ("9001_1", "de_inferno")]);
        assert_eq!(
            stale(9001, &legacy, &guard, None),
            ids(&["9001_0", "9001_1"])
        );
        let canonical = rows(&[("9001_1", "DE_NUKE"), ("9001_2", "de_inferno")]);
        assert!(stale(9001, &canonical, &guard, None).is_empty());
        // Played, then forfeited: Stats keeps the forfeited map's own row.
        let corrected = rows(&[("9001_0", "de_anubis")]);
        assert!(stale(9001, &corrected, &guard, None).is_empty());
    }

    #[test]
    fn a_later_map_stored_under_an_unscored_middle_maps_key_is_stale() {
        // Map 2 reads 0-0 (unaudited); legacy `_1` holds map 3.
        let guard = MapGuard::from_rows(&[
            named(played(9001, 1, 13, 5), "de_dust2"),
            named(placeholder(9001, 2), "de_nuke"),
            named(played(9001, 3, 9, 13), "de_inferno"),
        ]);
        let legacy = rows(&[("9001_0", "de_dust2"), ("9001_1", "de_inferno")]);
        assert_eq!(stale(9001, &legacy, &guard, None), ids(&["9001_1"]));
    }

    #[test]
    fn the_map_being_written_and_other_matches_are_not_stale() {
        let guard = MapGuard::from_rows(&[played(9001, 1, 13, 5), placeholder(9001, 2)]);
        assert!(stale(9001, &unnamed(&["9001_1"]), &guard, Some(2)).is_empty());
        assert!(stale(
            9001,
            &unnamed(&["90011_0", "9001_x", "9001_"]),
            &guard,
            None
        )
        .is_empty());
        assert_eq!(
            stale(9001, &unnamed(&["9001_99999999999"]), &guard, None),
            ids(&["9001_99999999999"])
        );
    }

    #[test]
    fn graphql_errors_and_missing_data_fail_closed() {
        let ok =
            json!({ "data": { "findManyMatch": [{ "matchId": "9001_0", "mapName": "de_nuke" }] } });
        assert_eq!(parse(true, &ok).unwrap(), rows(&[("9001_0", "de_nuke")]));
        assert!(parse(false, &ok).is_err());
        assert!(parse(
            true,
            &json!({ "errors": [{ "message": "boom" }], "data": null })
        )
        .is_err());
        assert!(parse(true, &json!({ "data": null })).is_err());
        assert!(parse(
            true,
            &json!({ "data": { "findManyMatch": [{ "matchId": "9001_0" }] } })
        )
        .is_err());
    }
}
