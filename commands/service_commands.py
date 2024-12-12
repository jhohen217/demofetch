import logging
import asyncio
from core.migrate_matches import migrate_matches
from core.match_scrape import start_match_scraping

logger = logging.getLogger('discord_bot')

# Store background tasks
background_tasks = {}

async def run_match_scraping():
    """Run match scraping in a loop"""
    while True:
        try:
            await start_match_scraping()
        except Exception as e:
            logger.error(f"Error in match scraping: {e}")
        await asyncio.sleep(300)  # Wait 5 minutes between runs

async def handle_message(bot, message):
    """Handle service-related commands"""
    content = message.content.lower()
    args = content.split()
    command = args[0] if args else ""

    if command == 'start':
        try:
            # Check if service is already running
            if 'match_scraping' in background_tasks and not background_tasks['match_scraping'].done():
                await bot.send_message(message.author, "Match fetching service is already running!")
                return True

            # Start match fetching service
            await bot.send_message(message.author, "Starting match fetching service...")
            task = asyncio.create_task(run_match_scraping())
            background_tasks['match_scraping'] = task
            await bot.send_message(message.author, "Match fetching service started successfully!")
            return True

        except Exception as e:
            error_msg = f"Error starting service: {str(e)}"
            logger.error(error_msg)
            await bot.send_message(message.author, error_msg)
            return True

    elif command == 'stop':
        try:
            # Stop match fetching service if running
            if 'match_scraping' in background_tasks:
                background_tasks['match_scraping'].cancel()
                await bot.send_message(message.author, "Match fetching service stopped.")
            else:
                await bot.send_message(message.author, "No services are currently running.")
            return True

        except Exception as e:
            error_msg = f"Error stopping service: {str(e)}"
            logger.error(error_msg)
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
            logger.error(error_msg)
            await bot.send_message(message.author, error_msg)
            return True

    return False

def setup(bot):
    """Required setup function for the extension"""
    return True
