from typing import Optional, List, Dict
import httpx

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/football/nfl"


def fetch_teams():
    """Fetch all NFL teams from ESPN API."""
    resp = httpx.get(f"{ESPN_BASE}/teams", timeout=15)
    resp.raise_for_status()
    data = resp.json()
    teams = []
    for team in data.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", []):
        t = team.get("team", {})
        teams.append({
            "id": t.get("id"),
            "name": t.get("displayName"),
            "abbreviation": t.get("abbreviation"),
        })
    return teams


def fetch_roster(team_id):
    """Fetch roster for a team from ESPN API."""
    resp = httpx.get(f"{ESPN_BASE}/teams/{team_id}/roster", timeout=15)
    resp.raise_for_status()
    data = resp.json()

    players = []
    for group in data.get("athletes", []):
        for athlete in group.get("items", []):
            players.append({
                "id": str(athlete.get("id")),
                "name": athlete.get("displayName", ""),
                "position": athlete.get("position", {}).get("abbreviation", ""),
            })
    return players


def fetch_player_stats(player_id, season=2024):
    """Fetch season stats for a player from ESPN API."""
    resp = httpx.get(
        f"https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/{player_id}/stats",
        params={"season": season},
        timeout=15,
    )
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def extract_all(season=2024):
    """Extract player data from all NFL teams."""
    teams = fetch_teams()
    all_players = []

    for team in teams:
        try:
            roster = fetch_roster(team["id"])
            for player in roster:
                player["team"] = team["abbreviation"]
            all_players.extend(roster)
        except Exception:
            continue  # Skip teams with roster issues

    return all_players, season
