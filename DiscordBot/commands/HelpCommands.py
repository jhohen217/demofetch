import logging

logger = logging.getLogger('discord_bot')

async def handle_message(bot, message):
    """Handle message-based help commands"""
    content = message.content.lower()
    
    if content == 'help' or content == 'list':
        help_text = """Available commands:

Bot Commands:
help - Show this help message
ping - Check if bot is responsive
update - Update the bot (will restart the service)

Match Scraping:
start - Start fetching NA East match IDs (runs every 3-5 minutes)
start datefetch - Start processing undated matches to categorize by month
status - Check the current status of scraping and downloading services
stop [service] - Stop services (fetch/download/parse/datefetch or all if no service specified)

Demo Management:
info - Show detailed stats and storage information
download <category> <month> <number> - Download demos from a specific month
  Examples: 
    - download ace february 100
    - download quad january all
  Categories: ace, quad
  Month: Full month name (e.g., january, february)
  Number: Specific number or 'all'
dlstats - Get download statistics

User Operations:
fetch <username> - Fetch matches for a user
getdemos <username> - Download demos for a user
parse <source> [number] - Parse demos from category/user (source: ace/quad/username)"""
        await bot.send_message(message.author, help_text)
        return True
        
    return False

def setup(bot):
    """Required setup function for the extension"""
    return True
