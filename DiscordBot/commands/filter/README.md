# Filter Module

This module handles the filtering and categorization of matches based on their statistics.

## Overview

The filter module is responsible for:

1. Processing matches to determine if they contain ace or quad kills
2. Categorizing matches into ace_matchids.txt, quad_matchids.txt, or unapproved_matchids.txt
3. Handling API rate limiting and retries
4. Tracking failed matches and permanent fails

## Files

- `__init__.py` - Package initialization
- `commands.py` - Command handling functions
- `config.py` - Configuration handling
- `service.py` - Core match filtering functionality
- `utils.py` - Utility functions and MatchResult class

## Commands

### Filter Matches

Filters matches for the current or specified month.

```
filter [month]
```

- `month` (optional) - Month to filter (e.g., "February"). Defaults to current month.

Example:
```
filter March
```

### Start Continuous Filtering

Starts the continuous filtering process with configurable intervals.

```
start filter [month]
```

- `month` (optional) - Month to filter (e.g., "February"). Defaults to current month.

Example:
```
start filter March
```

### Stop Filtering

Stops the continuous filtering process.

```
stop filter
```

## Configuration

The filter module uses the following configuration settings from `config.json`:

```json
{
  "filter": {
    "interval": 600
  },
  "faceit": {
    "api_key": "your-api-key"
  }
}
```

- `interval` - Interval between filtering cycles in seconds. Default: 600 (10 minutes).
- `api_key` - FACEIT API key used for accessing match statistics.

## Match Categorization

Matches are categorized based on the following criteria:

- **Ace Matches**: Matches containing at least one ace (5 kills in a round)
- **Quad Matches**: Matches containing at least one quad (4 kills in a round) but no aces
- **Unapproved Matches**: Matches containing neither aces nor quads

Categorized matches are written to the following files:

- `ace_matchids_{month}.txt` - Matches with aces
- `quad_matchids_{month}.txt` - Matches with quads but no aces
- `unapproved_matchids_{month}.txt` - Matches with neither aces nor quads

## Error Handling

The filter module includes robust error handling:

- **Rate Limiting**: Automatically handles API rate limiting with exponential backoff
- **Failed Matches**: Tracks failed matches and retries them up to a configurable limit
- **Permanent Fails**: Marks matches as permanent fails after exceeding retry limit
- **Detailed Logging**: Logs detailed error information for troubleshooting

## Recent Changes

- Improved error handling and logging
- Added detailed tracking of failed matches
- Enhanced rate limiting protection
- Added support for concurrent API requests with semaphores
