import logging
import os
from core.migrate_matches import migrate_matches

logger = logging.getLogger('discord_bot')

async def handle_message(bot, message):
    """Handle service-related commands"""
    content = message.content.lower()
    args = content.split()
    command = args[0] if args else ""

    if command == 'start':
        try:
            # Start fetching NA East match IDs
            await bot.send_message(message.author, "Starting match fetching service...")
            # TODO: Implement match fetching service start
            return True
        except Exception as e:
            error_msg = f"Error starting service: {str(e)}"
            await bot.send_message(message.author, error_msg)
            return True

    elif command == 'prefix':
        try:
            # Run match migration to update prefixes
            await bot.send_message(message.author, "Starting match prefix migration...")
            await migrate_matches()
            await bot.send_message(message.author, "Match prefix migration completed!")
            return True
        except Exception as e:
            error_msg = f"Error during prefix migration: {str(e)}"
            await bot.send_message(message.author, error_msg)
            return True

    return False

def setup(bot):
    """Required setup function for the extension"""
    return True
