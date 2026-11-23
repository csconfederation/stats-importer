//! Map-scoped forfeit/void guard shared by `backfill` and `--directory` imports.
//!
//! Decides per `(match_id, map_number)`, never per series: one forfeited map in
//! a BO3 must not suppress a played map. Score protection (Core's
//! `score_entered_manually`, D65) is a separate question from "may this map
//! import played stats" (D66), so adjusted non-forfeit maps such as S17 `6034`
//! (official `0-13`) and legitimate `13-0` maps are never classified here.

use std::collections::{BTreeMap, HashMap};

use anyhow::Result;
use sqlx::{FromRow, PgPool};

/// D66: forfeit or void maps that keep no played stats and that Core does not
/// flag `is_forfeit` (S18 `0-1`/`0-0` admin edits). Keyed `(Core match id,
/// map_number)`; every demo is `-0_` i.e. map 1. The four S20 forfeits
/// (8668, 8763, 8811, 8926) are `is_forfeit = true` in Core.
const REVIEWED_VOID_MAPS: [(i64, i32); 5] = [(6841, 1), (6881, 1), (6929, 1), (7086, 1), (7339, 1)];

/// D66: adjusted-score maps that DO import played stats. They override only the
/// audit-history rule (never `is_forfeit` or the void list), so a stray audit
/// row can't suppress them.
const D66_ADJUSTED_MAPS: [(i64, i32); 4] = [(6028, 1), (6034, 1), (6039, 1), (6143, 1)];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// `matches_matchstats.is_forfeit`.
    Forfeit,
    /// Legacy `1-0` / `0-1` map score (disciplinary forfeit, pre-flag data).
    LegacyOneZero,
    /// Latest `matches_matchscoreaudit` row for the map is a forfeit and the
    /// map still holds that result.
    ForfeitAudit,
    /// Reviewed D66 void map.
    ReviewedVoid,
}

impl SkipReason {
    pub fn describe(self) -> &'static str {
        match self {
            Self::Forfeit => "Core marks the map is_forfeit",
            Self::LegacyOneZero => "map score is a legacy 1-0/0-1 forfeit",
            Self::ForfeitAudit => "latest Core score audit is an unreversed forfeit",
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
    pub has_winner: bool,
    /// Newest audit row has a `forfeiting_team`. A reversal row names none.
    pub audit_forfeit: bool,
    pub audit_new_score: Option<String>,
}

fn strip_spaces(value: &str) -> String {
    value.split_whitespace().collect()
}

fn parse_scoreline(value: &str) -> Option<(i32, i32)> {
    let (home, away) = value.split_once('-')?;
    Some((home.trim().parse().ok()?, away.trim().parse().ok()?))
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

/// An audit forfeit only counts while the map still holds the result it wrote.
/// `reverse_match_forfeit` appends a non-forfeit audit row, so the newest row
/// handles tool reversals. The old by-hand reversal cleared only `is_forfeit`
/// and left the forfeit audit newest; once a stats submission overwrites the
/// scores, they no longer match and the map is played.
fn audit_forfeit_still_holds(row: &MapRow) -> bool {
    row.audit_forfeit
        && row
            .audit_new_score
            .as_deref()
            .and_then(parse_scoreline)
            .is_some_and(|score| score == (row.home_score, row.away_score))
}

/// Why this map must not import played stats, if it must not. `13-0`/`0-13`
/// maps are deliberately not a signal.
pub fn skip_reason(row: &MapRow) -> Option<SkipReason> {
    let key = (row.match_id, row.map_number);
    if REVIEWED_VOID_MAPS.contains(&key) {
        return Some(SkipReason::ReviewedVoid);
    }
    if row.is_forfeit {
        return Some(SkipReason::Forfeit);
    }
    if legacy_one_zero(row) {
        return Some(SkipReason::LegacyOneZero);
    }
    if !D66_ADJUSTED_MAPS.contains(&key) && audit_forfeit_still_holds(row) {
        return Some(SkipReason::ForfeitAudit);
    }
    None
}

/// Per-map skip decisions for one Core match.
#[derive(Debug, Clone, Default)]
pub struct MapGuard {
    skipped: BTreeMap<i32, SkipReason>,
    has_unskipped_played_map: bool,
}

impl MapGuard {
    pub fn from_rows(rows: &[MapRow]) -> Self {
        let mut guard = Self::default();
        for row in rows {
            match skip_reason(row) {
                Some(reason) => {
                    guard.skipped.insert(row.map_number, reason);
                }
                None => {
                    // Same "played" test as the season query: a BO3's unplayed
                    // map-3 placeholder (0-0, no winner) is not a played map.
                    guard.has_unskipped_played_map |=
                        row.has_winner || row.home_score != 0 || row.away_score != 0;
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
           (ms.winner_id IS NOT NULL) AS has_winner,
           coalesce(a.forfeiting_team_id IS NOT NULL, false) AS audit_forfeit,
           a.new_score AS audit_new_score
    FROM matches_matchstats ms
    JOIN matches_matches m ON m.id = ms.match_id AND m.match_type = 'REGULATION'
    LEFT JOIN LATERAL (
      SELECT sa.forfeiting_team_id, sa.new_score
      FROM matches_matchscoreaudit sa
      WHERE sa.match_id = ms.match_id AND sa.map_number = ms.map_number
      ORDER BY sa.created DESC, sa.id DESC
      LIMIT 1
    ) a ON true
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
            has_winner: true,
            audit_forfeit: false,
            audit_new_score: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_rows::played;
    use super::*;

    fn audit(mut row: MapRow, forfeit: bool, new_score: &str) -> MapRow {
        row.audit_forfeit = forfeit;
        row.audit_new_score = Some(new_score.to_owned());
        row
    }

    #[test]
    fn adjusted_zero_thirteen_and_plain_thirteen_zero_are_not_forfeits() {
        // 6034: official 0-13, manual but not is_forfeit.
        assert_eq!(skip_reason(&played(6034, 1, 0, 13)), None);
        assert_eq!(skip_reason(&played(9001, 1, 13, 0)), None);
    }

    #[test]
    fn d66_adjusted_map_ignores_a_stray_forfeit_audit_but_not_is_forfeit() {
        let stray = audit(played(6034, 1, 0, 13), true, "0 - 13");
        assert_eq!(skip_reason(&stray), None);
        let mut flagged = stray;
        flagged.is_forfeit = true;
        assert_eq!(skip_reason(&flagged), Some(SkipReason::Forfeit));
    }

    #[test]
    fn confirmed_forfeits_are_skipped() {
        let mut flagged = played(8926, 1, 0, 13);
        flagged.is_forfeit = true;
        assert_eq!(skip_reason(&flagged), Some(SkipReason::Forfeit));
        assert_eq!(
            skip_reason(&played(9001, 1, 1, 0)),
            Some(SkipReason::LegacyOneZero)
        );
        let mut text_only = played(9001, 1, 0, 0);
        text_only.score = Some("0 - 1".to_owned());
        assert_eq!(skip_reason(&text_only), Some(SkipReason::LegacyOneZero));
        assert_eq!(
            skip_reason(&audit(played(9001, 1, 13, 0), true, "13 - 0")),
            Some(SkipReason::ForfeitAudit)
        );
    }

    #[test]
    fn reversed_forfeit_with_audit_history_is_not_skipped() {
        // reverse_match_forfeit: newest audit row names no forfeiting team.
        let by_tool = audit(played(9001, 1, 0, 0), false, "0 - 0");
        assert_eq!(skip_reason(&by_tool), None);
        // Cleared by hand: the forfeit audit is still newest, but a stats
        // submission has since overwritten the scores.
        let by_hand = audit(played(9001, 1, 13, 7), true, "13 - 0");
        assert_eq!(skip_reason(&by_hand), None);
        // Same, for a 0-1 forfeit whose score text is stale.
        let mut stale_text = audit(played(9001, 1, 7, 13), true, "0 - 1");
        stale_text.score = Some("0 - 1".to_owned());
        assert_eq!(skip_reason(&stale_text), None);
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
