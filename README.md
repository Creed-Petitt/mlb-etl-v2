# MLB Data Scraping & Analytics Pipeline

This repository contains a comprehensive, automated ETL pipeline designed to collect, process, and store a wide array of Major League Baseball (MLB) data. The system fetches data from multiple sources—including live game data, advanced player statistics, and real-time betting odds—and loads it into a structured PostgreSQL database suitable for powering advanced analytics, machine learning models, or a user-facing dashboard.

The architecture is designed with automation and scalability in mind, with a clear roadmap for deployment as a fully-managed, serverless data pipeline on Google Cloud Platform.

## Key Features

*   **Multi-Source Data Aggregation**: Ingests data from a diverse set of official and public sources, including **Baseball Savant** (Statcast), the official **MLB Stats API**, and betting odds from **FanDuel**, **PrizePicks**, and **ESPN Bet**.
*   **Intelligent & Automated ETL**: Features "smart" job orchestration that automatically detects the correct date range to process, avoiding redundant data loads and gracefully handling future, unplayed games.
*   **High-Performance Parallel Processing**: Leverages concurrent execution to efficiently process large volumes of data in parallel, significantly reducing the runtime of batch jobs for game and player split data.
*   **Robust Data Transformation**: Employs a sophisticated processing layer that cleans, validates, normalizes (e.g., team names, player names across different sources), and enriches raw data before loading.
*   **Idempotent & Resilient Pipelines**: All ETL jobs are designed to be safely re-runnable without creating duplicate data, with robust error handling and data validation to withstand real-world data inconsistencies.
*   **Automated Bet Settlement**: Includes a dedicated processor that automatically settles open player prop bets by fetching final game results and comparing them against the betting line.

## Data Highlights

The pipeline is engineered to capture a deep and wide variety of data points, creating a rich dataset for analysis.

*   **Pitch-by-Pitch Granularity**: The core of the dataset is the **Statcast** data, which provides details for every pitch thrown in a game, including:
    *   **Pitch Dynamics**: Type (e.g., 4-Seam Fastball), release speed, and spin rate.
    *   **Movement Profile**: Detailed horizontal (`pfx_x`) and vertical (`pfx_z`) movement data.
    *   **Batted Ball Outcomes**: For pitches put in play, it captures the **exit velocity**, **launch angle**, and projected hit distance.

*   **Comprehensive Betting Markets**: The system goes beyond simple win/loss odds to capture a detailed view of the betting landscape.
    *   **Full Market Structure**: For sources like FanDuel, it loads the entire market hierarchy (Events → Markets → Runners), allowing it to capture standard game lines (Moneyline, Spread, Total) as well as hundreds of **player props** (e.g., "Player to Record a Hit", "Pitcher Strikeouts"), and **futures** (e.g., "World Series Winner").
    *   **Line Movement**: By capturing prices over time, the schema is capable of tracking and analyzing how odds and lines move.
    *   **Automated Prop Settlement**: The pipeline automatically resolves player prop outcomes (Over, Under, or Push) by comparing the betting line to the final, verified box score statistics.

*   **Advanced Player Analytics**: The dataset is enriched with modern, sabermetrically-inclined statistics from Baseball Savant, including:
    *   **Expected Stats**: `xBA` (Expected Batting Average), `xSLG` (Expected Slugging), and `xERA` (Expected ERA), which measure performance based on quality of contact.
    *   **Percentile Rankings**: Player rankings across a wide range of categories, from `xwOBA` and `Barrel %` to `Chase Rate` and `Sprint Speed`.
    *   **Pitch Arsenal Data**: Detailed reports on a pitcher's arsenal (usage % of each pitch type) and a batter's performance against each specific pitch type.

*   **Deep Situational Context**: Player performance is captured not just in aggregate, but across dozens of specific situations ("splits"), providing deep contextual data for queries like:
    *   A player's batting average **vs. Left-Handed Pitchers**.
    *   A pitcher's ERA when playing **at Home** vs. **Away**.
    *   A batter's OPS **with Runners in Scoring Position**.

## Architecture Overview

1.  **Orchestration Layer (`etl/loaders/`)**
    This layer contains the master executable scripts that serve as the entry point for each ETL job. These "loaders" are responsible for managing the high-level workflow, applying performance optimizations (like parallelization), handling command-line arguments, and coordinating the layers below.

2.  **Extraction Layer (`etl/clients/`)**
    This layer is responsible for the "Extract" part of the pipeline. Each client is a dedicated class that handles all communication with a single external API. It encapsulates source-specific complexities, such as authentication, required headers, or rate limiting, and returns raw, unprocessed data.

3.  **Transformation & Load Layer (`etl/processors/`)**
    This is the core logical engine of the pipeline, responsible for the "Transform" and "Load" steps. Processors take the raw data from clients, clean it, validate its integrity, normalize inconsistent values (e.g., mapping `CHW` to `CWS`), and transform it into the application's SQLAlchemy data models before loading it into the database.

4.  **Data Models (`models/`)**
    This layer defines the target database schema using SQLAlchemy ORM classes. It serves as the single source of truth for the application's data structure.

## ETL Jobs Overview

The pipeline is composed of several distinct, runnable jobs, each managed by a script in the `etl/loaders/` directory.

*   `game_loader.py`: The main daily job. It uses the `DateManager` to find the last fully completed day of games and processes all subsequent games in parallel. It loads all core data, including Statcast, box scores, and player information.
*   `fanduel_loader.py`, `espn_betting_loader.py`, `prizepicks_loader.py`: These jobs run on a frequent schedule to fetch the latest betting odds, markets, and player projections from their respective sportsbooks.
*   `prizepicks_settler.py`: A business logic job that runs after games are final. It finds open PrizePicks bets and settles them by comparing the betting line to the actual player stats from the loaded box scores.
*   `pybaseball_loader.py`: A bulk refresh job that runs periodically (e.g., daily) to load season-long advanced statistics (like Exit Velocity, xBA, etc.) for all players.
*   `mlb_splits_loader.py`: A bulk refresh job that fetches and loads player performance against dozens of specific situations ("splits"), such as vs. LHP, at Home, or with runners in scoring position.

## Planned Cloud Deployment & Automation

This project is designed to be deployed as a fully automated, serverless data pipeline on Google Cloud Platform.

1.  **Containerization**: Each loader script will be containerized using a **Docker** image, creating a portable and isolated environment for the job.
2.  **Serverless Execution**: The Docker containers will be deployed as **Google Cloud Run** jobs. This serverless platform automatically handles infrastructure and scaling, allowing each ETL task to run on-demand.
3.  **Automated Scheduling**: **Google Cloud Scheduler** will be used to trigger the Cloud Run jobs on a defined schedule (e.g., running the `game_loader` every hour, betting loaders every 15 minutes).
4.  **Managed Database**: The PostgreSQL database will be migrated to a managed **Google Cloud SQL** instance to ensure high availability, automated backups, and scalability.

This serverless architecture will provide a cost-effective, scalable, and zero-maintenance platform for running the data pipeline.

## Project Structure

```
/
├── etl/
│   ├── clients/      # Data Extraction: API clients for each data source.
│   ├── loaders/      # Orchestration: Executable scripts that manage each ETL job.
│   └── processors/   # Transform & Load: Business logic for processing and saving data.
├── models/
│   ├── betting_models.py # SQLAlchemy models for betting data.
│   ├── mlb_models.py     # SQLAlchemy models for core game/player/stat data.
│   └── database.py       # DB session management and schema creation.
├── requirements.txt  # Project dependencies.
└── README.md         # You are here.
```

## Quick Start / Local Development

### Prerequisites
*   Python 3.10+
*   Pip & Virtualenv
*   A running PostgreSQL database

### Setup Instructions

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/mlb-scraping-dashboardv2.git
    cd mlb-scraping-dashboardv2
    ```

2.  **Create and activate a virtual environment:**
    ```bash
    python -m venv venv
    source venv/bin/activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure environment variables:**
    Create a `.env` file in the project root. This file stores database credentials and API-specific tokens.
    ```.env
    # Database Connection
    DATABASE_URL=postgresql://user:password@localhost:5432/mlb_data

    # API Keys & Tokens (if required by clients)
    FANDUEL_PX_CONTEXT="your_perimeterx_token_here"
    # Other keys...
    ```
    *Note: The FanDuel client requires a special `FANDUEL_PX_CONTEXT` token to bypass anti-bot measures, which must be acquired and refreshed periodically.*

5.  **Create the database schema:**
    Run the database script to create all tables from the models.
    ```bash
    python -m models.database
    ```

### Running ETL Jobs

All jobs are run as Python modules from the project root.

*   **Load core game data for the current "smart" window:**
    ```bash
    python -m etl.loaders.game.game_loader
    ```

*   **Load current FanDuel markets and prices:**
    ```bash
    python -m etl.loaders.betting.fanduel_loader
    ```

*   **Load seasonal advanced stats from Pybaseball:**
    ```bash
    python -m etl.loaders.pybaseball.pybaseball_loader
    ```

## Technology Stack

*   **Language**: Python 3
*   **Data Access / ORM**: SQLAlchemy
*   **Data Transforming**: Pandas
*   **Core Libraries**: `requests`, `pybaseball`
*   **Database**: PostgreSQL
*   **Planned Cloud Stack**: Docker, Google Cloud Run, Cloud SQL, and Cloud Scheduler
