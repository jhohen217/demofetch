# Scraper Module

This module handles the scraping of match data from the FACEIT API and FACEIT Hubs.

## Overview

The scraper module is responsible for:

1. Fetching match data from the FACEIT API
2. Fetching match data from FACEIT Hubs
3. Updating match_ids.txt files with new matches
4. Scheduling regular scraping with configurable intervals

## Files

- `__init__.py` - Package initialization
- `commands.py` - Command handling functions
- `config.py` - Configuration handling
- `service.py` - Core match scraping functionality
- `hub_service.py` - Hub match scraping functionality
- `utils.py` - Utility functions

## Commands

### Start Scraper

The main command to start scraping is simply `start`, which is handled by the MatchScraperCommands module. This command starts the continuous scraping process using the configured intervals from config.json.

### Force Scrape

Forces an immediate scrape, bypassing the wait.

```
force
```

Example:
```
force
```

### Hub Commands

#### Scrape Hub

Scrapes matches from a specific hub or all configured hubs.

```
hub scrape [hub_id] [hub_name]
```

- `hub_id` (optional) - Hub ID to scrape. If not provided, all configured hubs will be scraped.
- `hub_name` (optional) - Hub name for display.

Example:
```
hub scrape c7dc4af7-33ad-4973-90c2-5cce9376258b "My Hub"
```

#### List Hubs

Lists all configured hubs.

```
hub list
```

### Stop Scraper

Stops the continuous scraping process.

```
stop scraper
```

## Configuration

The scraper module uses the following configuration settings from `config.json`:

```json
{
  "downloader": {
    "fetch_delay": {
      "min": 180,
      "max": 300
    }
  }
}
```

- `min` - Minimum delay between scrapes in seconds. Default: 180 (3 minutes).
- `max` - Maximum delay between scrapes in seconds. Default: 300 (5 minutes).

## Recent Changes

- Refactored scraping functionality into a modular structure
- Simplified command structure - only `start` is needed to begin scraping
- Added validation to ensure reasonable scraping intervals (minimum 1 minute)
- Improved error handling and logging
- Fixed issue with scraping happening too frequently
