from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from etl.database import Base


class Player(Base):
    __tablename__ = "players"

    id = Column(Integer, primary_key=True, index=True)
    external_id = Column(String, unique=True, index=True, nullable=False)
    name = Column(String, nullable=False)
    team = Column(String, nullable=True)
    position = Column(String, nullable=True)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    stats = relationship("PlayerStats", back_populates="player", cascade="all, delete-orphan")


class PlayerStats(Base):
    __tablename__ = "player_stats"
    __table_args__ = (UniqueConstraint("player_id", "season", "week", name="uq_player_season_week"),)

    id = Column(Integer, primary_key=True, index=True)
    player_id = Column(Integer, ForeignKey("players.id"), nullable=False)
    season = Column(Integer, nullable=False)
    week = Column(Integer, nullable=False)
    pass_yards = Column(Float, default=0)
    pass_td = Column(Integer, default=0)
    interceptions = Column(Integer, default=0)
    rush_yards = Column(Float, default=0)
    rush_td = Column(Integer, default=0)
    receptions = Column(Integer, default=0)
    rec_yards = Column(Float, default=0)
    rec_td = Column(Integer, default=0)
    fantasy_points = Column(Float, default=0)

    player = relationship("Player", back_populates="stats")


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True, index=True)
    started_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    finished_at = Column(DateTime, nullable=True)
    status = Column(String, default="running")
    records_processed = Column(Integer, default=0)
    error_message = Column(String, nullable=True)
