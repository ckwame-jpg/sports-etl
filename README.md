# Sports ETL

[![CI](https://github.com/ckwame-jpg/sports-etl/actions/workflows/ci.yml/badge.svg)](https://github.com/ckwame-jpg/sports-etl/actions/workflows/ci.yml)

An ETL pipeline that pulls NFL player stats from the ESPN API, calculates fantasy scores (PPR), and loads everything into PostgreSQL. Includes a CLI for running the pipeline and viewing top players.

## How It Works

1. **Extract** - Pulls rosters from all 32 NFL teams via ESPN's public API
2. **Transform** - Filters to fantasy-relevant positions (QB, RB, WR, TE, K), calculates PPR fantasy points
3. **Load** - Upserts players and stats into PostgreSQL with deduplication

## Quick Start

```bash
# Start PostgreSQL
docker compose up -d

# Install dependencies
pip install -r requirements.txt

# Run the pipeline
python -m etl.cli run

# View top fantasy players
python -m etl.cli top

# Check pipeline run history
python -m etl.cli status
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `run --season 2024` | Run the ETL pipeline for a season |
| `status` | Show recent pipeline runs |
| `top --limit 20` | Show top fantasy players |

## Tech Stack

- **httpx** - HTTP client for ESPN API
- **pandas** - data transformation
- **SQLAlchemy** - ORM and database management
- **PostgreSQL** - persistent storage
- **Click** - CLI framework
- **Rich** - terminal output formatting
- **Docker Compose** - database setup
- **pytest** - test suite

## Fantasy Scoring (PPR)

| Stat | Points |
|------|--------|
| Passing yards | 0.04/yard |
| Passing TD | 4 |
| Interception | -2 |
| Rushing yards | 0.1/yard |
| Rushing TD | 6 |
| Reception | 1 (PPR) |
| Receiving yards | 0.1/yard |
| Receiving TD | 6 |

## Running Tests

```bash
pytest -v
```

```text
tests/test_extract.py::test_fetch_teams           PASSED
tests/test_extract.py::test_fetch_roster           PASSED
tests/test_transform.py::test_fantasy_points_qb    PASSED
tests/test_transform.py::test_fantasy_points_rb    PASSED
tests/test_transform.py::test_fantasy_points_empty PASSED
tests/test_transform.py::test_transform_players    PASSED
tests/test_transform.py::test_transform_players_deduplicates PASSED
tests/test_transform.py::test_transform_players_empty PASSED
tests/test_transform.py::test_transform_stats      PASSED
tests/test_transform.py::test_transform_stats_none PASSED
tests/test_transform.py::test_parse_espn_stats_empty PASSED
tests/test_load.py::test_upsert_player_create      PASSED
tests/test_load.py::test_upsert_player_update      PASSED
tests/test_load.py::test_upsert_stats_create       PASSED
tests/test_load.py::test_upsert_stats_update       PASSED
```

## Project Structure

```text
sports-etl/
├── etl/
│   ├── extract.py      # ESPN API data extraction
│   ├── transform.py    # Data cleaning + fantasy scoring
│   ├── load.py         # Database upsert logic
│   ├── pipeline.py     # ETL orchestration
│   ├── models.py       # SQLAlchemy models
│   ├── database.py     # DB connection
│   └── cli.py          # CLI commands
├── tests/
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## Built By

Christopher Prempeh
