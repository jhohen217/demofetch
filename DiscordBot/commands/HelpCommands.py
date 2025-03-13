import logging

logger = logging.getLogger('discord_bot')

async def handle_message(bot, message):
    """Handle message-based help commands"""
    content = message.content.lower()
    
    if content == 'help' or content == 'list':
        # Split help text into multiple messages to avoid Discord's 2000 character limit
        help_text_parts = [
            """Available commands (Part 1/3):

Bot Commands:
help - Show this help message
ping - Check if bot is responsive
update - Update the bot (will restart the service)

Match Scraping:
start - Start fetching NA East match IDs (runs every 3-5 minutes)
start parser - Start the continuous demo parser service (automatically scans for and parses new demos)
start datefetch - Start processing undated matches to categorize by month
force - Immediately start a new match scraping cycle, bypassing the wait time
status - Check the current status of all services (match fetching, parser, etc.)
reset - Reset the match notification timer (clears the no-matches alert state)
stop [service] - Stop services (fetch/parser/datefetch or all if no service specified)

Note: The system will automatically send an alert if no new matches are found for over an hour.""",

            """Available commands (Part 2/3):

Demo Management:
info - Show detailed stats and storage information with per-month breakdown, including parsing statistics
download <category> <month> <number> [loop] - Download demos from a specific month
  Examples: 
    - download ace february 100
    - download quad january all
    - download ace december 50 loop (downloads in batches, waiting for parser to finish each batch)
  Categories: ace, quad
  Month: Full month name (e.g., january, february)
  Number: Specific number or 'all'
  Loop: Optional 'loop' parameter to continuously download in batches (automatically starts parser if not running)
dlstats - Get download statistics

User Operations:
fetch <username> - Fetch matches for a user
getdemos <username> - Download demos for a user""",

            """Available commands (Part 3/3):

Parser Operations:
start parser [month] [number] [parallel_limit] [scan_interval] - Start the continuous demo parser service
  (You can also use "start parse" as an alias)
  Examples:
    - start parser February 100 5 300
    - start parser (processes all months, continuously scanning for new demos)
    - start parse December all
  Month: Full month name (e.g., January, February) - Optional, if not provided will process all months
  Number: Specific number or 'all' - Optional, limits demos processed per scan
  Parallel_limit: Number of demos to process in parallel (default: 5) - Optional
  Scan_interval: Seconds between scans for new demos (default: 300) - Optional
stop parser - Stop the demo parser service (finishes current demos but doesn't start new ones)
parse <source> [number] - Parse demos from category/user (source: ace/quad/username)
rebuild parsed [month] - Rebuild the parsed file(s) by scanning the KillCollections directory
  Examples:
    - rebuild parsed December
    - rebuild parsed (rebuilds all months)

Note: All match files are organized by month (January, February, etc.) in the textfiles directory.
Each month folder contains its own set of files for matches, downloads, and categories."""
        ]
        
        # Send each part of the help text
        for part in help_text_parts:
            await bot.send_message(message.author, part)
        
        return True
        
    return False

def setup(bot):
    """Required setup function for the extension"""
    return True
