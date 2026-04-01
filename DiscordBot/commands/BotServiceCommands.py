import logging
import asyncio
import sys
import nextcord
from commands import MatchScraperCommands
from commands.parser import get_parser_stats

logger = logging.getLogger('discord_bot')

# Store background tasks
background_tasks = {}

# IMPORTANT: This module is deprecated and redirects to MatchScraperCommands
# All match scraping functionality has been consolidated in MatchScraperCommands.py

async def run_match_scraping():
    """
    DEPRECATED: This function is kept for backward compatibility.
    All match scraping is now handled by MatchScraperCommands.
    """
    logger.warning("BotServiceCommands.run_match_scraping is deprecated. Using MatchScraperCommands instead.")
    await MatchScraperCommands.continuous_scraping()

async def handle_message(bot, message):
    """
    DEPRECATED: This function is kept for backward compatibility.
    All commands are now handled by MatchScraperCommands.
    """
    logger.warning("BotServiceCommands.handle_message is deprecated. Redirecting to MatchScraperCommands.")
    
    # Special case for 'update' command which needs to be handled here for backward compatibility
    if message.content.lower().strip() == 'update':
        try:
            logger.info("Update command received")
            await bot.send_message(message.author, "Initiating update process. Bot will restart momentarily...")
            logger.info("Exiting program for service restart.")
            sys.exit(0)  # Exit with success code for clean restart
            return True
        except Exception as e:
            error_msg = f"Error during update: {str(e)}"
            logger.error(error_msg)
            await bot.send_message(message.author, error_msg)
            return True
    
    # Forward all other commands to MatchScraperCommands
    return await MatchScraperCommands.handle_message(bot, message)

def setup(bot):
    """Required setup function for the extension"""
    logger.warning("BotServiceCommands is deprecated. Please use MatchScraperCommands instead.")
    logger.debug("Service commands module setup (redirecting to MatchScraperCommands)")
    
    # Make sure MatchScraperCommands is set up
    MatchScraperCommands.setup(bot)
    return True
