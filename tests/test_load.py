from etl.load import upsert_player, upsert_stats
from etl.models import Player, PlayerStats


def test_upsert_player_create(db_session):
    player = upsert_player(db_session, "ext1", "Test Player", "KC", "QB")
    db_session.commit()
    assert player.id is not None
    assert player.name == "Test Player"


def test_upsert_player_update(db_session):
    upsert_player(db_session, "ext1", "Old Name", "KC", "QB")
    db_session.commit()
    upsert_player(db_session, "ext1", "New Name", "PHI", "QB")
    db_session.commit()

    players = db_session.query(Player).all()
    assert len(players) == 1
    assert players[0].name == "New Name"
    assert players[0].team == "PHI"


def test_upsert_stats_create(db_session):
    player = upsert_player(db_session, "ext1", "Player", "KC", "QB")
    db_session.commit()

    stats = {"pass_yards": 300, "pass_td": 3, "fantasy_points": 24.0}
    upsert_stats(db_session, player.id, 2024, 0, stats)
    db_session.commit()

    result = db_session.query(PlayerStats).first()
    assert result.pass_yards == 300
    assert result.fantasy_points == 24.0


def test_upsert_stats_update(db_session):
    player = upsert_player(db_session, "ext1", "Player", "KC", "QB")
    db_session.commit()

    upsert_stats(db_session, player.id, 2024, 0, {"pass_yards": 200, "fantasy_points": 10.0})
    db_session.commit()

    upsert_stats(db_session, player.id, 2024, 0, {"pass_yards": 350, "fantasy_points": 20.0})
    db_session.commit()

    stats = db_session.query(PlayerStats).all()
    assert len(stats) == 1
    assert stats[0].pass_yards == 350
