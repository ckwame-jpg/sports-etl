from unittest.mock import patch, MagicMock
from etl.extract import fetch_teams, fetch_roster


def _mock_response(data, status=200):
    resp = MagicMock()
    resp.status_code = status
    resp.json.return_value = data
    resp.raise_for_status.return_value = None
    return resp


@patch("httpx.get")
def test_fetch_teams(mock_get):
    mock_get.return_value = _mock_response({
        "sports": [{"leagues": [{"teams": [
            {"team": {"id": "1", "displayName": "Chiefs", "abbreviation": "KC"}},
            {"team": {"id": "2", "displayName": "Eagles", "abbreviation": "PHI"}},
        ]}]}]
    })
    teams = fetch_teams()
    assert len(teams) == 2
    assert teams[0]["abbreviation"] == "KC"


@patch("httpx.get")
def test_fetch_roster(mock_get):
    mock_get.return_value = _mock_response({
        "athletes": [{"items": [
            {"id": 101, "displayName": "Patrick Mahomes", "position": {"abbreviation": "QB"}},
            {"id": 102, "displayName": "Travis Kelce", "position": {"abbreviation": "TE"}},
        ]}]
    })
    players = fetch_roster("1")
    assert len(players) == 2
    assert players[0]["name"] == "Patrick Mahomes"
