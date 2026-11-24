//! Map-scoped forfeit/void guard shared by `backfill` and `--directory` imports.
//!
//! Decides per `(match_id, map_number)`, never per series: one forfeited map in
//! a BO3 must not suppress a played map. Score protection (Core's
//! `score_entered_manually`, D65) is a separate question from "may this map
//! import played stats" (D66), so adjusted non-forfeit maps such as S17 `6034`
//! (official `0-13`) and legitimate `13-0` maps are never classified here.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::Result;
use sqlx::{FromRow, PgPool};

/// D66: forfeit or void maps that keep no played stats and that Core does not
/// flag `is_forfeit` (S18 `0-1`/`0-0` admin edits). Keyed `(Core match id,
/// map_number)`; every demo is `-0_` i.e. map 1. The four S20 forfeits
/// (8668, 8763, 8811, 8926) are `is_forfeit = true` in Core.
const REVIEWED_VOID_MAPS: [(i64, i32); 5] = [(6841, 1), (6881, 1), (6929, 1), (7086, 1), (7339, 1)];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// `matches_matchstats.is_forfeit`.
    Forfeit,
    /// Legacy `1-0` / `0-1` map score (disciplinary forfeit, pre-flag data).
    LegacyOneZero,
    /// Reviewed D66 void map.
    ReviewedVoid,
}

impl SkipReason {
    pub fn describe(self) -> &'static str {
        match self {
            Self::Forfeit => "Core marks the map is_forfeit",
            Self::LegacyOneZero => "map score is a legacy 1-0/0-1 forfeit",
            Self::ReviewedVoid => "reviewed D66 void map",
        }
    }
}

/// Canonical (lowest-id) `matches_matchstats` row for one map, plus the newest
/// audit row for that map.
#[derive(Debug, Clone, FromRow)]
pub struct MapRow {
    pub match_id: i64,
    pub map_number: i32,
    pub is_forfeit: bool,
    pub home_score: i32,
    pub away_score: i32,
    pub score: Option<String>,
    pub map_name: Option<String>,
    pub has_winner: bool,
    /// Core's score audit has any row for the map: its `is_forfeit` flag is then
    /// authoritative, and the score is no longer evidence either way.
    pub has_audit: bool,
}

fn strip_spaces(value: &str) -> String {
    value.split_whitespace().collect()
}

fn legacy_one_zero(row: &MapRow) -> bool {
    matches!((row.home_score, row.away_score), (1, 0) | (0, 1))
        // The score text goes stale after a by-hand reversal (stats overwrite
        // only home/away), so it counts only while the numeric scores are unset.
        || (row.home_score == 0
            && row.away_score == 0
            && row
                .score
                .as_deref()
                .is_some_and(|score| matches!(strip_spaces(score).as_str(), "1-0" | "0-1")))
}

/// Why this map must not import played stats, if it must not. `13-0`/`0-13`
/// maps are deliberately not a signal. The `1-0`/`0-1` heuristic is for rows
/// that predate `is_forfeit`; once an admin edit or reversal is audited (a hand
/// clear of the flag keeps the old score), the flag alone decides.
pub fn skip_reason(row: &MapRow) -> Option<SkipReason> {
    let key = (row.match_id, row.map_number);
    if REVIEWED_VOID_MAPS.contains(&key) {
        return Some(SkipReason::ReviewedVoid);
    }
    if row.is_forfeit {
        return Some(SkipReason::Forfeit);
    }
    if !row.has_audit && legacy_one_zero(row) {
        return Some(SkipReason::LegacyOneZero);
    }
    None
}

/// Per-map skip decisions for one Core match.
#[derive(Debug, Clone, Default)]
pub struct MapGuard {
    skipped: BTreeMap<i32, SkipReason>,
    has_unskipped_played_map: bool,
    /// Core's `map_name` per `map_number`, for maps that have one.
    map_names: BTreeMap<i32, String>,
    map_numbers: BTreeSet<i32>,
    /// Highest map that was played, forfeited, or audited. Later rows are the
    /// unplayed placeholders a decided BO3 keeps (a 2-0 sweep's map 3).
    last_real_map: Option<i32>,
}

impl MapGuard {
    pub fn from_rows(rows: &[MapRow]) -> Self {
        let mut guard = Self::default();
        for row in rows {
            if let Some(name) = row.map_name.as_deref().filter(|name| !name.is_empty()) {
                guard.map_names.insert(row.map_number, name.to_owned());
            }
            guard.map_numbers.insert(row.map_number);
            let scored = row.has_winner || row.home_score != 0 || row.away_score != 0;
            let reason = skip_reason(row);
            if scored || row.has_audit || reason.is_some() {
                guard.last_real_map = guard.last_real_map.max(Some(row.map_number));
            }
            match reason {
                Some(reason) => {
                    guard.skipped.insert(row.map_number, reason);
                }
                None => {
                    // Same "played" test as the season query: a BO3's unplayed
                    // map-3 placeholder (0-0, no winner) is not a played map.
                    guard.has_unskipped_played_map |= scored;
                }
            }
        }
        guard
    }

    pub fn skip_reason(&self, map_number: i32) -> Option<SkipReason> {
        self.skipped.get(&map_number).copied()
    }

    /// Every played map is skipped, so there is nothing to import.
    pub fn whole_match_skipped(&self) -> bool {
        !self.skipped.is_empty() && !self.has_unskipped_played_map
    }

    pub fn has_skipped_maps(&self) -> bool {
        !self.skipped.is_empty()
    }

    pub fn map_name(&self, map_number: i32) -> Option<&str> {
        self.map_names.get(&map_number).map(String::as_str)
    }

    /// Core map numbers whose map name appears in `filename` (case-insensitive).
    pub fn map_numbers_named_in(&self, filename: &str) -> Vec<i32> {
        let filename = filename.to_ascii_lowercase();
        self.map_names
            .iter()
            .filter(|(_, name)| filename.contains(&name.to_ascii_lowercase()))
            .map(|(number, _)| *number)
            .collect()
    }

    /// A Core map, not a trailing placeholder. BO3 maps are played in order, so
    /// a 0-0 map before a played one is a reversed forfeit, not a placeholder.
    pub fn is_real_map(&self, map_number: i32) -> bool {
        self.map_numbers.contains(&map_number)
            && self.last_real_map.is_some_and(|last| map_number <= last)
    }

    /// Two Core maps share a `map_name` (one map played twice).
    pub fn has_repeated_map_name(&self) -> bool {
        let mut seen = BTreeSet::new();
        self.map_names
            .values()
            .any(|name| !seen.insert(name.to_ascii_lowercase()))
    }

    pub fn skipped_maps(&self) -> impl Iterator<Item = (i32, SkipReason)> + '_ {
        self.skipped.iter().map(|(map, reason)| (*map, *reason))
    }
}

const MAP_ROWS_SELECT: &str = r#"
    SELECT DISTINCT ON (ms.match_id, ms.map_number)
           ms.match_id::bigint AS match_id,
           ms.map_number,
           ms.is_forfeit,
           ms.home_score,
           ms.away_score,
           ms.score,
           ms.map_name,
           (ms.winner_id IS NOT NULL) AS has_winner,
           EXISTS (
             SELECT 1 FROM matches_matchscoreaudit sa
             WHERE sa.match_id = ms.match_id AND sa.map_number = ms.map_number
           ) AS has_audit
    FROM matches_matchstats ms
    JOIN matches_matches m ON m.id = ms.match_id AND m.match_type = 'REGULATION'
"#;

/// Canonical map rows for every regulation match of a season, by match id.
pub async fn fetch_season_guards(pool: &PgPool, season: i32) -> Result<HashMap<i64, MapGuard>> {
    let sql = format!(
        "{MAP_ROWS_SELECT}
         JOIN leagues_matchday md ON md.id = m.match_day_id
         JOIN leagues_seasons s ON s.id = md.season_id
         WHERE s.number = $1
         ORDER BY ms.match_id, ms.map_number, ms.id"
    );
    let rows = sqlx::query_as::<_, MapRow>(&sql)
        .bind(season)
        .fetch_all(pool)
        .await?;
    let mut by_match: HashMap<i64, Vec<MapRow>> = HashMap::new();
    for row in rows {
        by_match.entry(row.match_id).or_default().push(row);
    }
    Ok(by_match
        .into_iter()
        .map(|(match_id, rows)| (match_id, MapGuard::from_rows(&rows)))
        .collect())
}

/// Guard for one regulation match (`--directory` import).
pub async fn fetch_match_guard(pool: &PgPool, match_id: i64) -> Result<MapGuard> {
    let sql = format!(
        "{MAP_ROWS_SELECT}
         WHERE ms.match_id = $1
         ORDER BY ms.match_id, ms.map_number, ms.id"
    );
    let rows = sqlx::query_as::<_, MapRow>(&sql)
        .bind(match_id)
        .fetch_all(pool)
        .await?;
    Ok(MapGuard::from_rows(&rows))
}

#[cfg(test)]
pub mod test_rows {
    use super::MapRow;

    pub fn played(match_id: i64, map_number: i32, home: i32, away: i32) -> MapRow {
        MapRow {
            match_id,
            map_number,
            is_forfeit: false,
            home_score: home,
            away_score: away,
            score: Some(format!("{home} - {away}")),
            map_name: None,
            has_winner: true,
            has_audit: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_rows::played;
    use super::*;

    fn audited(mut row: MapRow) -> MapRow {
        row.has_audit = true;
        row
    }

    #[test]
    fn adjusted_zero_thirteen_and_plain_thirteen_zero_are_not_forfeits() {
        // 6034: official 0-13, manual but not is_forfeit.
        assert_eq!(skip_reason(&played(6034, 1, 0, 13)), None);
        assert_eq!(skip_reason(&played(9001, 1, 13, 0)), None);
        assert_eq!(skip_reason(&audited(played(6034, 1, 0, 13))), None);
    }

    #[test]
    fn flagged_forfeits_are_skipped_with_or_without_audit_history() {
        let mut flagged = played(8926, 1, 0, 13);
        flagged.is_forfeit = true;
        assert_eq!(skip_reason(&flagged), Some(SkipReason::Forfeit));
        assert_eq!(skip_reason(&audited(flagged)), Some(SkipReason::Forfeit));
    }

    #[test]
    fn unaudited_legacy_one_zero_maps_are_skipped() {
        assert_eq!(
            skip_reason(&played(9001, 1, 1, 0)),
            Some(SkipReason::LegacyOneZero)
        );
        assert_eq!(
            skip_reason(&played(9001, 1, 0, 1)),
            Some(SkipReason::LegacyOneZero)
        );
        let mut text_only = played(9001, 1, 0, 0);
        text_only.score = Some("0 - 1".to_owned());
        assert_eq!(skip_reason(&text_only), Some(SkipReason::LegacyOneZero));
    }

    #[test]
    fn audited_maps_follow_is_forfeit_not_the_score() {
        // Hand-cleared 13-0 forfeit: the flag is off, the old score and audit
        // remain, and a replay can legitimately finish 13-0.
        assert_eq!(skip_reason(&audited(played(9001, 1, 13, 0))), None);
        // Audited 0-1 reversal: the flag is off, so the replay may import.
        assert_eq!(skip_reason(&audited(played(9001, 1, 0, 1))), None);
        assert_eq!(skip_reason(&audited(played(9001, 1, 1, 0))), None);
        let mut stale_text = audited(played(9001, 1, 0, 0));
        stale_text.score = Some("0 - 1".to_owned());
        assert_eq!(skip_reason(&stale_text), None);
        // Reversed by the tool.
        assert_eq!(skip_reason(&audited(played(9001, 1, 0, 0))), None);
    }

    #[test]
    fn reviewed_void_maps_are_skipped_even_when_unflagged() {
        for id in [6841, 6881, 6929, 7086, 7339] {
            assert_eq!(
                skip_reason(&played(id, 1, 0, 0)),
                Some(SkipReason::ReviewedVoid)
            );
        }
        // Same id, other map: not void.
        assert_eq!(skip_reason(&played(6841, 2, 13, 5)), None);
    }

    #[test]
    fn guard_skips_only_the_forfeited_map_of_a_bo3() {
        let mut forfeited = played(9001, 1, 0, 13);
        forfeited.is_forfeit = true;
        let guard = MapGuard::from_rows(&[forfeited, played(9001, 2, 13, 0)]);
        assert_eq!(guard.skip_reason(1), Some(SkipReason::Forfeit));
        assert_eq!(guard.skip_reason(2), None);
        assert!(!guard.whole_match_skipped());
    }

    #[test]
    fn guard_skips_whole_match_when_only_an_unplayed_placeholder_remains() {
        let mut placeholder = played(9001, 3, 0, 0);
        placeholder.has_winner = false;
        placeholder.score = None;
        let mut one = played(9001, 1, 0, 13);
        one.is_forfeit = true;
        let mut two = played(9001, 2, 13, 0);
        two.is_forfeit = true;
        assert!(MapGuard::from_rows(&[one, two, placeholder]).whole_match_skipped());
        assert!(!MapGuard::default().whole_match_skipped());
    }
}
