"""
Command handling functions for the scraper module.
"""

import os
import asyncio
import logging
import random
from typing import Dict, List, Set, Tuple, Optional
from datetime import datetime, timedelta

from commands.scraper.service import start_match_scraping
from commands.scraper.hub_service import process_all_hubs, start_hub_scraping
from commands.scraper.config import get_config, get_available_months

# Set up logging
logger = logging.getLogger('discord_bot')

# Global tasks
scraping_task = None
hub_scraping_task = None

# Default minimum delay between scrapes (in seconds)
DEFAULT_MIN_DELAY = 180  # 3 minutes
DEFAULT_MAX_DELAY = 300  # 5 minutes

async def handle_message(bot, message):
    """
    Handle message-based scraper commands.
    
    Args:
        bot: Discord bot instance
        message: Message object
        
    Returns:
        bool: True if command was handled, False otherwise
    """
    content = message.content.lower().strip()
    args = content.split()
    command = args[0] if args else ""
    
    global scraping_task, hub_scraping_task
    
    if command == "force":
        logger.info("Force command received")
        try:
            # Cancel any existing scraping task
            if scraping_task and not scraping_task.done():
                scraping_task.cancel()
                try:
                    await scraping_task  # Wait for cancellation to complete
                except asyncio.CancelledError:
                    pass
                logger.info("Existing scraping task cancelled")

            # Start a new scraping task immediately, bypassing the wait
            await bot.send_message(message.author, "Forcing immediate match scraping...")
            
            # Check if month parameter is provided
            month = None
            if len(args) > 1:
                month = args[1].capitalize()
                if month.lower() not in [
                    'january', 'february', 'march', 'april', 'may', 'june',
                    'july', 'august', 'september', 'october', 'november', 'december'
                ]:
                    await bot.send_message(message.author, f"Invalid month: {month}. Using current month.")
                    month = None
            
            # Start scraping
            scraping_task = asyncio.create_task(start_match_scraping(bot, month))
            result = await scraping_task
            
            if result:
                await bot.send_message(message.author, "Match scraping completed successfully.")
                
                # Start hub scraping
                hub_wait_msg = "Waiting 60 seconds before starting hub scraping..."
                await bot.send_message(message.author, hub_wait_msg)
                await asyncio.sleep(60)
                
                hub_start_msg = "Starting hub match scraping for all configured hubs..."
                await bot.send_message(message.author, hub_start_msg)
                
                hub_scraping_task = asyncio.create_task(process_all_hubs(bot, month))
                hub_result = await hub_scraping_task
                
                if hub_result:
                    await bot.send_message(message.author, "Hub match scraping completed successfully.")
                else:
                    await bot.send_message(message.author, "Hub match scraping encountered an error.")
            else:
                await bot.send_message(message.author, "Match scraping encountered an error.")
            
            return True
        except Exception as e:
            await bot.send_message(message.author, f"Error forcing scrape: {str(e)}")
            return True

    elif command == "start" and len(args) > 1 and args[1] == "scraper":
        try:
            if scraping_task and not scraping_task.done():
                await bot.send_message(message.author, "Match scraping is already running")
                return True

            # Check if month parameter is provided
            month = None
            if len(args) > 2:
                month = args[2].capitalize()
                if month.lower() not in [
                    'january', 'february', 'march', 'april', 'may', 'june',
                    'july', 'august', 'september', 'october', 'november', 'december'
                ]:
                    await bot.send_message(message.author, f"Invalid month: {month}. Using current month.")
                    month = None
            
            # Check if delay parameters are provided
            min_delay = DEFAULT_MIN_DELAY
            max_delay = DEFAULT_MAX_DELAY
            
            if len(args) > 3:
                try:
                    min_delay = int(args[3])
                    if min_delay < 60:  # Minimum 1 minute
                        await bot.send_message(message.author, f"Minimum delay must be at least 60 seconds (1 minute). Using {DEFAULT_MIN_DELAY} seconds.")
                        min_delay = DEFAULT_MIN_DELAY
                except ValueError:
                    await bot.send_message(message.author, f"Invalid minimum delay. Using default of {DEFAULT_MIN_DELAY} seconds.")
            
            if len(args) > 4:
                try:
                    max_delay = int(args[4])
                    if max_delay < min_delay:
                        await bot.send_message(message.author, f"Maximum delay must be greater than minimum delay. Using {min_delay + 120} seconds.")
                        max_delay = min_delay + 120
                except ValueError:
                    await bot.send_message(message.author, f"Invalid maximum delay. Using default of {DEFAULT_MAX_DELAY} seconds.")
            
            # Create and start the continuous scraping task
            await bot.send_message(message.author, f"Started fetching Match IDs for {month or 'current month'} with delay range {min_delay}-{max_delay} seconds")
            
            # Update service status and bot presence
            bot.is_service_running = True
            await bot.update_status()
            
            # Start scraping
            scraping_task = asyncio.create_task(continuous_scraping(bot, month, min_delay, max_delay))
            
            return True
        except Exception as e:
            await bot.send_message(message.author, f"Error: {str(e)}")
            return True
    
    elif command == "hub" and len(args) > 1:
        try:
            # Check if hub ID is provided
            hub_id = None
            hub_name = None
            
            if len(args) > 2:
                hub_id = args[2]
                if len(args) > 3:
                    hub_name = args[3]
            
            # Check if month parameter is provided
            month = None
            if len(args) > 4:
                month = args[4].capitalize()
                if month.lower() not in [
                    'january', 'february', 'march', 'april', 'may', 'june',
                    'july', 'august', 'september', 'october', 'november', 'december'
                ]:
                    await bot.send_message(message.author, f"Invalid month: {month}. Using current month.")
                    month = None
            
            # Start hub scraping
            if args[1] == "scrape":
                await bot.send_message(message.author, f"Starting hub scraping for {hub_name or hub_id or 'default hub'}")
                
                if hub_id:
                    result = await start_hub_scraping(bot, hub_id, hub_name, month)
                else:
                    result = await process_all_hubs(bot, month)
                
                if result:
                    await bot.send_message(message.author, "Hub scraping completed successfully")
                else:
                    await bot.send_message(message.author, "Hub scraping encountered an error")
                
                return True
            
            # List configured hubs
            elif args[1] == "list":
                config = get_config()
                hubs = config.get('faceit', {}).get('hubs', [])
                
                if not hubs:
                    await bot.send_message(message.author, "No hubs configured")
                    return True
                
                hub_list = []
                for i, hub in enumerate(hubs):
                    hub_id = hub.get('id', 'Unknown')
                    hub_name = hub.get('name', f"Hub {i+1}")
                    hub_list.append(f"{i+1}. {hub_name} ({hub_id})")
                
                await bot.send_message(message.author, "Configured hubs:\n" + "\n".join(hub_list))
                return True
            
        except Exception as e:
            await bot.send_message(message.author, f"Error: {str(e)}")
            return True
    
    elif command == "stop" and len(args) > 1 and args[1] == "scraper":
        try:
            # Stop scraping task if it exists
            if scraping_task and not scraping_task.done():
                scraping_task.cancel()
                try:
                    await scraping_task
                except asyncio.CancelledError:
                    pass
                scraping_task = None
                # Update service status and bot presence
                bot.is_service_running = False
                await bot.update_status()
                await bot.send_message(message.author, "Match scraping stopped")
            else:
                await bot.send_message(message.author, "Match scraping is not running")
            
            # Stop hub scraping task if it exists
            if hub_scraping_task and not hub_scraping_task.done():
                hub_scraping_task.cancel()
                try:
                    await hub_scraping_task
                except asyncio.CancelledError:
                    pass
                hub_scraping_task = None
                await bot.send_message(message.author, "Hub match scraping stopped")
            
            return True
        except Exception as e:
            await bot.send_message(message.author, f"Error: {str(e)}")
            return True
    
    return False

async def continuous_scraping(bot=None, month=None, min_delay=DEFAULT_MIN_DELAY, max_delay=DEFAULT_MAX_DELAY):
    """
    Continuously scrape matches with random intervals.
    
    Args:
        bot: Discord bot instance
        month: Month to scrape (e.g., "February")
        min_delay: Minimum delay between scrapes in seconds
        max_delay: Maximum delay between scrapes in seconds
    """
    logger.info(f"continuous_scraping called for {month or 'current month'} with delay range {min_delay}-{max_delay} seconds")
    
    while True:
        try:
            # Wait for configured interval before starting
            wait_time = random.randint(min_delay, max_delay)
            logger.info(f"Waiting {wait_time} seconds before next scrape...")
            
            # Calculate and display next scrape time
            next_scrape = datetime.now() + timedelta(seconds=wait_time)
            next_hub_scrape = next_scrape + timedelta(seconds=60)
            
            print("\n" + "=" * 50)
            print("      NEXT SCRAPES SCHEDULE")
            print("=" * 50)
            print(f"🔄 Main Scrape: {next_scrape.strftime('%H:%M:%S')}")
            print(f"🔄 Hub Scrape: {next_hub_scrape.strftime('%H:%M:%S')} (60s after main)")
            print("=" * 50)
            
            logger.info(f"Next scrape scheduled for: {next_scrape.strftime('%H:%M:%S')}")
            logger.info(f"Next hub scrape scheduled for: {next_hub_scrape.strftime('%H:%M:%S')}")
            
            await asyncio.sleep(wait_time)
            
            # Start scraping
            result = await start_match_scraping(bot, month)
            
            if result:
                logger.info("Match scraping completed successfully")
                
                # Start hub scraping after a delay
                hub_scrape_time = datetime.now() + timedelta(seconds=60)
                print("\n" + "=" * 50)
                print("⏳ WAITING: Hub scraping will start in 60 seconds")
                print(f"🕒 Hub scrape scheduled for: {hub_scrape_time.strftime('%H:%M:%S')}")
                print("=" * 50)
                
                logger.info(f"Waiting 60 seconds before starting hub scraping (at {hub_scrape_time.strftime('%H:%M:%S')})")
                await asyncio.sleep(60)
                
                # Start hub scraping
                print("\n" + "=" * 50)
                print("⏳ STARTING: Hub match scraping")
                print("=" * 50)
                
                logger.info("Starting hub match scraping for all configured hubs")
                global hub_scraping_task
                hub_scraping_task = asyncio.create_task(process_all_hubs(bot, month))
                hub_result = await hub_scraping_task
                
                if hub_result:
                    logger.info("Hub match scraping completed successfully")
                else:
                    logger.error("Hub match scraping encountered an error")
            else:
                logger.error("Match scraping encountered an error")
        
        except asyncio.CancelledError:
            logger.info("Scraping task cancelled")
            break
        except Exception as e:
            logger.error(f"Error in continuous scraping: {str(e)}")
            # Wait before retrying on error
            await asyncio.sleep(60)

def setup(bot):
    """Required setup function for the extension"""
    logger.info("Scraper commands module setup complete")
    bot.is_service_running = False
    return True
