import logging
import asyncio
import sys
import nextcord
from core.migrate_matches import migrate_matches
from core.FaceitMatchScraper import start_match_scraping
from core.MatchScoreFilter import start_match_filtering

logger = logging.getLogger('discord_bot')

# Store background tasks
background_tasks = {}

async def run_match_scraping():
    """Run match scraping in a loop"""
    while True:
        try:
            logger.debug("Starting match scraping cycle")
            result = await start_match_scraping()
            if result:
                logger.debug("Match scraping completed successfully")
                
                # Always run filtering to catch any unfiltered matches
                logger.debug("Starting match filtering...")
                try:
                    filter_result = await start_match_filtering()
                    if filter_result:
                        logger.debug("Match filtering completed successfully")
                    else:
                        logger.error("Match filtering failed")
                except Exception as filter_error:
                    logger.error(f"Error in filtering task: {str(filter_error)}")
            else:
                logger.error("Match scraping encountered an error")
            
            logger.debug("Match scraping cycle completed")
        except Exception as e:
            logger.error(f"Error in match scraping: {e}")
        logger.debug("Waiting 5 minutes before next scraping cycle")
        await asyncio.sleep(300)  # Wait 5 minutes between runs

async def handle_message(bot, message):
    """Handle service-related commands"""
    content = message.content.lower()
    args = content.split()
    command = args[0] if args else ""

    logger.debug(f"Service commands handling message: {content}")

    if command == 'start':
        try:
            # Check if service is already running
            if 'match_scraping' in background_tasks and not background_tasks['match_scraping'].done():
                logger.debug("Match fetching service is already running")
                await bot.send_message(message.author, "Match fetching service is already running!")
                return True

            # Start match fetching service
            logger.debug("Starting match fetching service")
            await bot.send_message(message.author, "Starting match fetching service...")
            # Update service status and bot presence
            bot.is_service_running = True
            await bot.update_status()
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
                logger.debug("Stopping match fetching service")
                background_tasks['match_scraping'].cancel()
                # Update service status and bot presence
                bot.is_service_running = False
                await bot.update_status()
                await bot.send_message(message.author, "Match fetching service stopped.")
            else:
                logger.debug("No services are currently running")
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
            logger.debug("Starting match prefix migration")
            await bot.send_message(message.author, "Starting match prefix migration...")
            await migrate_matches()
            logger.debug("Match prefix migration completed")
            await bot.send_message(message.author, "Match prefix migration completed!")
            return True
        except Exception as e:
            error_msg = f"Error during prefix migration: {str(e)}"
            logger.error(error_msg)
            await bot.send_message(message.author, error_msg)
            return True

    elif command == 'update':
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

    logger.debug(f"Command not handled: {content}")
    return False

def setup(bot):
    """Required setup function for the extension"""
    logger.debug("Service commands module setup")
    return True
