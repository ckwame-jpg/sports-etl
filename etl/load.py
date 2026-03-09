from typing import Dict, Optional

from sqlalchemy.orm import Session

from etl.models import Player, PlayerStats


def upsert_player(db: Session, external_id: str, name: str, team: Optional[str], position: Optional[str]):
    """Insert or update a player record."""
    player = db.query(Player).filter(Player.external_id == external_id).first()
    if player:
        player.name = name
        player.team = team
        player.position = position
    else:
        player = Player(external_id=external_id, name=name, team=team, position=position)
        db.add(player)
    db.flush()
    return player


def upsert_stats(db: Session, player_id: int, season: int, week: int, stats: Dict):
    """Insert or update player stats for a season/week."""
    existing = db.query(PlayerStats).filter(
        PlayerStats.player_id == player_id,
        PlayerStats.season == season,
        PlayerStats.week == week,
    ).first()

    if existing:
        for key, value in stats.items():
            if key not in ("season", "week") and hasattr(existing, key):
                setattr(existing, key, value)
    else:
        stat_record = PlayerStats(
            player_id=player_id,
            season=season,
            week=week,
            pass_yards=stats.get("pass_yards", 0),
            pass_td=stats.get("pass_td", 0),
            interceptions=stats.get("interceptions", 0),
            rush_yards=stats.get("rush_yards", 0),
            rush_td=stats.get("rush_td", 0),
            receptions=stats.get("receptions", 0),
            rec_yards=stats.get("rec_yards", 0),
            rec_td=stats.get("rec_td", 0),
            fantasy_points=stats.get("fantasy_points", 0),
        )
        db.add(stat_record)
