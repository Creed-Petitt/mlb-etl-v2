# MLB Scraping Dashboard v2

A comprehensive web scraping ETL tool for collecting and analyzing MLB data from multiple sources.

## Overview

This project is a powerful command-line tool that scrapes extensive MLB data and sports betting information, providing access to advanced baseball analytics through an intuitive CLI interface.

## Features

### MLB Data Collection
- **Game Data**: Live scores, box scores, and game results
- **Advanced Player Statistics**: Comprehensive batting and pitching metrics
- **Exit Velocity Data**: Ball-in-play analytics and exit velocity metrics
- **Pitch Data**: StatCast pitch-by-pitch information
- **Zone Data**: Strike zone analytics and heat maps
- **Advanced Splits**: Situational statistics and performance breakdowns

### Sports Betting Integration
- **Multi-Sportsbook Coverage**: Odds and props from 4 different sportsbooks
- **Game Odds**: Money lines, run lines, and totals
- **Player Props**: Betting lines for individual player performance

### CLI Interface
- Built with **Typer** for robust command-line functionality
- Enhanced with **Rich** for beautiful terminal output
- Interactive commands to query and display any collected data
- Flexible data viewing options

## Technology Stack

- **Python** - Core scripting language
- **Typer** - Modern CLI framework
- **Rich** - Terminal formatting and display
- **Web Scraping** - Multi-source data collection
- **ETL Pipeline** - Data extraction, transformation, and loading

## Usage

Use the CLI to access any MLB data or betting information:

```bash
python cli.py games --date today
python cli.py player-stats --player "Mike Trout"
python cli.py odds --game "LAA vs HOU"
```

## Data Sources

- MLB official statistics
- StatCast advanced metrics
- Multiple sportsbook APIs/scraping
- Real-time game data feeds

## Installation

```bash
git clone https://github.com/Creed-Petitt/mlb-scraping-dashboardv2.git
cd mlb-scraping-dashboardv2
pip install -r requirements.txt
```

## Contributing

Feel free to submit issues and enhancement requests!

## License

