//! A BO3's existing Stats ids (`{match_id}_{n}`, Core map `n + 1`). Earlier
//! imports keyed some series by position (1-based, or `_0`/`_1` for maps 1, 3).
//! Stats replaces only the id it is sent, so a canonical import beside such a
//! row leaves a duplicate map; those rows need a rekey first.

use anyhow::{bail, Context, Result};
use reqwest::Client;
use serde_json::{json, Value};

use crate::forfeit::MapGuard;

const QUERY: &str = "query($prefix: String!) { \
    findManyMatch(where: { matchId: { startsWith: $prefix } }) { matchId } }";

/// Stats' match ids starting `{match_id}_`, from its GraphQL endpoint.
pub async fn fetch(client: &Client, stats_url: &str, match_id: i64) -> Result<Vec<String>> {
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

fn parse(success: bool, body: &Value) -> Result<Vec<String>> {
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
            row.get("matchId")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .with_context(|| format!("Stats match row has no matchId: {row}"))
        })
        .collect()
}

/// Existing ids of this match that name no real Core map (see
/// `MapGuard::is_real_map`) other than `writing`. Ids of other matches that
/// share the prefix (`startsWith` treats `_` as a wildcard) are ignored.
pub fn stale(
    match_id: i64,
    existing: &[String],
    guard: &MapGuard,
    writing: Option<i32>,
) -> Vec<String> {
    let prefix = format!("{match_id}_");
    let mut stale: Vec<String> = existing
        .iter()
        .filter(|id| {
            let Some(suffix) = id.strip_prefix(&prefix) else {
                return false;
            };
            if suffix.is_empty() || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
                return false;
            }
            let map = suffix
                .parse::<i32>()
                .map_or(i32::MAX, |n| n.saturating_add(1));
            Some(map) != writing && !guard.is_real_map(map)
        })
        .cloned()
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

    fn placeholder(match_id: i64, map: i32) -> MapRow {
        MapRow {
            has_winner: false,
            ..played(match_id, map, 0, 0)
        }
    }

    #[test]
    fn a_reversed_forfeit_with_canonical_ids_is_not_stale() {
        // Map 1's forfeit was reversed through Core (0-0, no winner, audited).
        let mut reversed = placeholder(9001, 1);
        reversed.has_audit = true;
        let guard = MapGuard::from_rows(&[reversed, played(9001, 2, 13, 5), placeholder(9001, 3)]);
        assert!(stale(9001, &ids(&["9001_0", "9001_1"]), &guard, None).is_empty());
        // An unaudited hand reversal reads the same: a 0-0 map before a played one.
        let guard = MapGuard::from_rows(&[
            placeholder(9001, 1),
            played(9001, 2, 13, 5),
            placeholder(9001, 3),
        ]);
        assert!(stale(9001, &ids(&["9001_0", "9001_1"]), &guard, None).is_empty());
    }

    #[test]
    fn a_one_based_legacy_sweep_is_stale() {
        let guard = MapGuard::from_rows(&[
            played(9001, 1, 13, 5),
            played(9001, 2, 13, 7),
            placeholder(9001, 3),
        ]);
        assert_eq!(
            stale(9001, &ids(&["9001_1", "9001_2"]), &guard, None),
            ids(&["9001_2"])
        );
        assert!(stale(9001, &ids(&["9001_0", "9001_1"]), &guard, None).is_empty());
    }

    #[test]
    fn a_positional_gap_series_is_stale() {
        let guard = MapGuard::from_rows(&[played(9001, 1, 13, 5), played(9001, 3, 13, 7)]);
        assert_eq!(
            stale(9001, &ids(&["9001_0", "9001_1"]), &guard, Some(3)),
            ids(&["9001_1"])
        );
        assert!(stale(9001, &ids(&["9001_0", "9001_2"]), &guard, None).is_empty());
    }

    #[test]
    fn the_map_being_written_and_other_matches_are_not_stale() {
        let guard = MapGuard::from_rows(&[played(9001, 1, 13, 5), placeholder(9001, 2)]);
        assert!(stale(9001, &ids(&["9001_1"]), &guard, Some(2)).is_empty());
        assert!(stale(9001, &ids(&["90011_0", "9001_x", "9001_"]), &guard, None).is_empty());
        assert_eq!(
            stale(9001, &ids(&["9001_99999999999"]), &guard, None),
            ids(&["9001_99999999999"])
        );
    }

    #[test]
    fn graphql_errors_and_missing_data_fail_closed() {
        let ok = json!({ "data": { "findManyMatch": [{ "matchId": "9001_0" }] } });
        assert_eq!(parse(true, &ok).unwrap(), ids(&["9001_0"]));
        assert!(parse(false, &ok).is_err());
        assert!(parse(
            true,
            &json!({ "errors": [{ "message": "boom" }], "data": null })
        )
        .is_err());
        assert!(parse(true, &json!({ "data": null })).is_err());
        assert!(parse(true, &json!({ "data": { "findManyMatch": [{}] } })).is_err());
    }
}
