from etl.transform import (
    calculate_fantasy_points,
    parse_espn_stats,
    transform_players,
    transform_stats,
)


def test_fantasy_points_qb():
    stats = {"pass_yards": 300, "pass_td": 3, "interceptions": 1, "rush_yards": 20, "rush_td": 0}
    points = calculate_fantasy_points(stats)
    # 300 * 0.04 + 3 * 4 + (-2) + 20 * 0.1 = 12 + 12 - 2 + 2 = 24
    assert points == 24.0


def test_fantasy_points_rb():
    stats = {"rush_yards": 120, "rush_td": 2, "receptions": 5, "rec_yards": 40, "rec_td": 0}
    points = calculate_fantasy_points(stats)
    # 120 * 0.1 + 2 * 6 + 5 * 1 + 40 * 0.1 = 12 + 12 + 5 + 4 = 33
    assert points == 33.0


def test_fantasy_points_empty():
    assert calculate_fantasy_points({}) == 0.0


def test_transform_players():
    raw = [
        {"id": "1", "name": "Patrick Mahomes", "position": "QB", "team": "KC"},
        {"id": "2", "name": "Travis Kelce", "position": "TE", "team": "KC"},
        {"id": "3", "name": "Some Lineman", "position": "OL", "team": "KC"},
    ]
    df = transform_players(raw)
    assert len(df) == 2  # OL filtered out
    assert set(df["position"]) == {"QB", "TE"}


def test_transform_players_deduplicates():
    raw = [
        {"id": "1", "name": "Player A", "position": "RB", "team": "KC"},
        {"id": "1", "name": "Player A", "position": "RB", "team": "KC"},
    ]
    df = transform_players(raw)
    assert len(df) == 1


def test_transform_players_empty():
    df = transform_players([])
    assert len(df) == 0


def test_transform_stats():
    stats = {"pass_yards": 250, "pass_td": 2}
    result = transform_stats(stats, season=2024)
    assert result["season"] == 2024
    assert result["week"] == 0
    assert result["fantasy_points"] > 0


def test_transform_stats_none():
    assert transform_stats(None) is None


def test_parse_espn_stats_empty():
    assert parse_espn_stats(None) is None
    assert parse_espn_stats({}) is None
