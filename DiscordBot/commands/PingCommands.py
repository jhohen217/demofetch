import logging

logger = logging.getLogger('discord_bot')

async def handle_message(bot, message):
    """Handle message-based ping command"""
    content = message.content.lower()
    
    if content == 'ping':
        await bot.send_message(message.author, 'Pong! Bot is responsive.')
        return True
            
    return False

def setup(bot):
    """Required setup function for the extension"""
    return True
