# Discord Bot Commands

This directory contains the command modules for the Discord bot.

## Module Structure

The commands are organized into the following modules:

- `scraper/` - Match scraping functionality
- `filter/` - Match filtering and categorization
- `parser/` - Demo parsing functionality
- Other command files for various bot functions

## Recent Refactoring

The match scraping and filtering functionality has been refactored into separate modules to improve maintainability and organization. The main changes include:

1. **Modular Structure**: Functionality is now organized into separate modules with clear responsibilities
2. **Improved Configuration**: Better handling of configuration with reasonable defaults
3. **Enhanced Error Handling**: More robust error handling and logging
4. **Rate Limiting Protection**: Better protection against API rate limiting
5. **Simplified Commands**: Streamlined command structure for easier use

### Scraper Module

The scraper module handles fetching match data from the FACEIT API and FACEIT Hubs. See [scraper/README.md](scraper/README.md) for details.

Key improvements:
- Maintained original scraping interval of 3-5 minutes by default
- Simplified command structure - only `start` is needed to begin scraping
- Added validation to ensure reasonable scraping intervals (minimum 1 minute)
- Improved error handling and logging
- Fixed issue with scraping happening too frequently

### Filter Module

The filter module handles processing and categorizing matches based on their statistics. See [filter/README.md](filter/README.md) for details.

Key improvements:
- Improved error handling and logging
- Added detailed tracking of failed matches
- Enhanced rate limiting protection
- Added support for concurrent API requests with semaphores

## Usage

The refactored modules maintain backward compatibility with existing commands. Here are some examples:

### Scraping Commands

```
start                                         # Start continuous scraping
force                                         # Force immediate scrape
hub scrape [hub_id] [hub_name]                # Scrape hub matches
hub list                                      # List configured hubs
stop scraper                                  # Stop scraping
```

### Filtering Commands

```
filter [month]                                # Filter matches
start filter [month]                          # Start continuous filtering
stop filter                                   # Stop filtering
```

## Legacy Commands

The legacy commands in `MatchScraperCommands.py` still work but delegate to the new modules. These will be maintained for backward compatibility.
