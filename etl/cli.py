import click
from rich.console import Console
from rich.table import Table

console = Console()


@click.group()
def cli():
    """NFL fantasy data ETL pipeline."""
    pass


@cli.command()
@click.option("--season", default=2024, help="NFL season year")
def run(season):
    """Run the ETL pipeline."""
    from etl.pipeline import run_pipeline
    run_pipeline(season)


@cli.command()
def status():
    """Show recent pipeline runs."""
    from etl.database import Base, SessionLocal, engine
    from etl.models import PipelineRun

    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    runs = db.query(PipelineRun).order_by(PipelineRun.started_at.desc()).limit(5).all()

    if not runs:
        console.print("[dim]No pipeline runs yet.[/dim]")
        return

    table = Table(title="Recent Pipeline Runs")
    table.add_column("ID", justify="right")
    table.add_column("Started", style="dim")
    table.add_column("Status")
    table.add_column("Records", justify="right")
    table.add_column("Error", max_width=40)

    for r in runs:
        status_style = "green" if r.status == "completed" else "red" if r.status == "failed" else "yellow"
        table.add_row(
            str(r.id),
            r.started_at.strftime("%Y-%m-%d %H:%M") if r.started_at else "-",
            f"[{status_style}]{r.status}[/{status_style}]",
            str(r.records_processed),
            r.error_message or "-",
        )

    console.print(table)
    db.close()


@cli.command()
@click.option("--limit", default=20, help="Number of players to show")
def top(limit):
    """Show top fantasy players from the latest run."""
    from etl.database import Base, SessionLocal, engine
    from etl.models import Player, PlayerStats

    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    results = (
        db.query(Player, PlayerStats)
        .join(PlayerStats)
        .order_by(PlayerStats.fantasy_points.desc())
        .limit(limit)
        .all()
    )

    if not results:
        console.print("[dim]No data yet. Run the pipeline first.[/dim]")
        return

    table = Table(title=f"Top {limit} Fantasy Players (PPR)")
    table.add_column("Rank", justify="right")
    table.add_column("Player", style="bold")
    table.add_column("Pos", style="cyan")
    table.add_column("Team")
    table.add_column("Fantasy Pts", justify="right", style="green")
    table.add_column("Pass Yds", justify="right")
    table.add_column("Rush Yds", justify="right")
    table.add_column("Rec Yds", justify="right")
    table.add_column("TDs", justify="right")

    for i, (player, stats) in enumerate(results, 1):
        total_td = (stats.pass_td or 0) + (stats.rush_td or 0) + (stats.rec_td or 0)
        table.add_row(
            str(i),
            player.name,
            player.position or "-",
            player.team or "-",
            f"{stats.fantasy_points:.1f}",
            str(int(stats.pass_yards or 0)),
            str(int(stats.rush_yards or 0)),
            str(int(stats.rec_yards or 0)),
            str(total_td),
        )

    console.print(table)
    db.close()
