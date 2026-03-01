from datetime import datetime, timezone
from rich.console import Console
from rich.progress import Progress
from etl.database import SessionLocal, Base, engine
from etl.models import PipelineRun
from etl.extract import extract_all, fetch_player_stats
from etl.transform import transform_players, parse_espn_stats, transform_stats
from etl.load import upsert_player, upsert_stats

console = Console()


def run_pipeline(season=2024):
    """Run the full ETL pipeline."""
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    run = PipelineRun(status="running")
    db.add(run)
    db.commit()

    try:
        # Extract
        console.print("[cyan]Extracting player data...[/cyan]")
        raw_players, season = extract_all(season)
        console.print(f"  Found {len(raw_players)} players across all teams")

        # Transform
        console.print("[cyan]Transforming data...[/cyan]")
        df = transform_players(raw_players, season)
        console.print(f"  {len(df)} fantasy-relevant players after filtering")

        # Load
        console.print("[cyan]Loading into database...[/cyan]")
        records = 0

        with Progress() as progress:
            task = progress.add_task("Processing players...", total=len(df))

            for _, row in df.iterrows():
                player = upsert_player(
                    db,
                    external_id=row["id"],
                    name=row["name"],
                    team=row.get("team"),
                    position=row.get("position"),
                )

                # Fetch and load stats for this player
                try:
                    raw_stats = fetch_player_stats(row["id"], season)
                    parsed = parse_espn_stats(raw_stats)
                    if parsed:
                        transformed = transform_stats(parsed, season)
                        if transformed:
                            upsert_stats(db, player.id, season, 0, transformed)
                            records += 1
                except Exception:
                    pass  # Some players have no stats

                progress.advance(task)

        db.commit()

        run.finished_at = datetime.now(timezone.utc)
        run.status = "completed"
        run.records_processed = records
        db.commit()

        console.print(f"[green]Pipeline complete. {records} stat records processed.[/green]")
        return records

    except Exception as e:
        run.finished_at = datetime.now(timezone.utc)
        run.status = "failed"
        run.error_message = str(e)[:500]
        db.commit()
        console.print(f"[red]Pipeline failed: {e}[/red]")
        raise
    finally:
        db.close()
