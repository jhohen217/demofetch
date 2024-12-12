import logging

logger = logging.getLogger('discord_bot')

async def handle_message(bot, message):
    """Handle message-based help commands"""
    content = message.content.lower()
    
    if content == 'help' or content == 'list':
        help_text = """Available commands:
help - Show this help message
ping - Check if bot is responsive
demos - Check demos directory
info - Show detailed stats and storage information
download <category> - Start downloading demos (ace/quad)
dlstats - Get download statistics
fetch <username> - Fetch matches for a user
getdemos <username> - Download demos for a user
parse <source> [number] - Parse demos from category/user (source: ace/quad/username)
start - Start fetching NA East match IDs (runs every 3-5 minutes)
stop [service] - Stop services (fetch/download/parse or all if no service specified)"""
        await bot.send_message(message.author, help_text)
        return True
        
    return False

def setup(bot):
    """Required setup function for the extension"""
    return True
