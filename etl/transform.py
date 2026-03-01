from typing import List, Dict, Optional
import pandas as pd


def calculate_fantasy_points(row):
    """PPR fantasy scoring."""
    points = 0.0
    points += row.get("pass_yards", 0) * 0.04      # 1 point per 25 pass yards
    points += row.get("pass_td", 0) * 4             # 4 points per pass TD
    points += row.get("interceptions", 0) * -2      # -2 per interception
    points += row.get("rush_yards", 0) * 0.1        # 1 point per 10 rush yards
    points += row.get("rush_td", 0) * 6             # 6 points per rush TD
    points += row.get("receptions", 0) * 1.0        # 1 point per reception (PPR)
    points += row.get("rec_yards", 0) * 0.1         # 1 point per 10 rec yards
    points += row.get("rec_td", 0) * 6              # 6 points per rec TD
    return round(points, 1)


def parse_espn_stats(raw_stats):
    """Parse ESPN's stats response into flat stat dict."""
    if not raw_stats:
        return None

    categories = raw_stats.get("categories", [])
    stats = {}

    stat_mapping = {
        "passingYards": "pass_yards",
        "passingTouchdowns": "pass_td",
        "interceptions": "interceptions",
        "rushingYards": "rush_yards",
        "rushingTouchdowns": "rush_td",
        "receptions": "receptions",
        "receivingYards": "rec_yards",
        "receivingTouchdowns": "rec_td",
    }

    for category in categories:
        cat_name = category.get("name", "")
        stat_names = category.get("names", [])
        stat_values = category.get("values", [])

        for name, value in zip(stat_names, stat_values):
            mapped = stat_mapping.get(name)
            if mapped:
                try:
                    stats[mapped] = float(value)
                except (ValueError, TypeError):
                    stats[mapped] = 0

    return stats if stats else None


def transform_players(raw_players, season=2024):
    """Transform raw player data into structured records."""
    if not raw_players:
        return pd.DataFrame()

    df = pd.DataFrame(raw_players)

    # Filter to skill positions relevant for fantasy
    fantasy_positions = {"QB", "RB", "WR", "TE", "K"}
    df = df[df["position"].isin(fantasy_positions)].copy()

    # Drop duplicates by player ID
    df = df.drop_duplicates(subset=["id"])

    # Clean names
    df["name"] = df["name"].str.strip()

    df["season"] = season

    return df


def transform_stats(stats_dict, season=2024):
    """Transform parsed stats and add fantasy points."""
    if not stats_dict:
        return None

    stats_dict["fantasy_points"] = calculate_fantasy_points(stats_dict)
    stats_dict["season"] = season
    stats_dict["week"] = 0  # Season total

    return stats_dict
