from datetime import datetime, timezone

import httpx
from rich.console import Console
from rich.progress import Progress

from etl.database import Base, SessionLocal, engine
from etl.extract import extract_all, fetch_player_stats
from etl.load import upsert_player, upsert_stats
from etl.models import PipelineRun
from etl.transform import parse_espn_stats, transform_players, transform_stats

console = Console()


def run_pipeline(season=2024):
    """Run the full ETL pipeline."""
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    run = PipelineRun(status="running")
    db.add(run)
    db.commit()

    try:
        console.print("[cyan]Extracting player data...[/cyan]")
        raw_players, season = extract_all(season)
        console.print(f"  Found {len(raw_players)} players across all teams")

        console.print("[cyan]Transforming data...[/cyan]")
        dataframe = transform_players(raw_players, season)
        console.print(f"  {len(dataframe)} fantasy-relevant players after filtering")

        console.print("[cyan]Loading into database...[/cyan]")
        records_processed = 0

        with Progress() as progress:
            task = progress.add_task("Processing players...", total=len(dataframe))

            for _, row in dataframe.iterrows():
                player = upsert_player(
                    db,
                    external_id=row["id"],
                    name=row["name"],
                    team=row.get("team"),
                    position=row.get("position"),
                )

                try:
                    raw_stats = fetch_player_stats(row["id"], season)
                    parsed_stats = parse_espn_stats(raw_stats)
                    transformed_stats = transform_stats(parsed_stats, season) if parsed_stats else None
                    if transformed_stats:
                        upsert_stats(db, player.id, season, 0, transformed_stats)
                        records_processed += 1
                except (httpx.HTTPError, KeyError, TypeError, ValueError):
                    pass

                progress.advance(task)

        db.commit()

        run.finished_at = datetime.now(timezone.utc)
        run.status = "completed"
        run.records_processed = records_processed
        db.commit()

        console.print(f"[green]Pipeline complete. {records_processed} stat records processed.[/green]")
        return records_processed

    except Exception as exc:
        run.finished_at = datetime.now(timezone.utc)
        run.status = "failed"
        run.error_message = str(exc)[:500]
        db.commit()
        console.print(f"[red]Pipeline failed: {exc}[/red]")
        raise
    finally:
        db.close()
