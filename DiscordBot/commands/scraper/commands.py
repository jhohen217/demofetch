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
scraping_task: Optional[asyncio.Task] = None
hub_scraping_task: Optional[asyncio.Task] = None

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
        # Remember whether continuous scraping was already running before force
        was_service_running = getattr(bot, 'is_service_running', False)
        try:
            # Cancel any existing one-shot scraping task (not continuous)
            if scraping_task and not scraping_task.done():
                scraping_task.cancel()
                try:
                    await scraping_task  # Wait for cancellation to complete
                except asyncio.CancelledError:
                    pass
                logger.info("Existing scraping task cancelled")

            # Start a new scraping task immediately, bypassing the wait
            await bot.send_message(message.author, "Forcing immediate match scraping...")

            # Mark service as active during the force scrape
            bot.is_service_running = True
            await bot.update_status()

            # Start scraping
            scraping_task = asyncio.create_task(start_match_scraping(bot))
            result = await scraping_task

            if result:
                await bot.send_message(message.author, "Match scraping completed successfully.")

                # Start hub scraping
                hub_wait_msg = "Waiting 60 seconds before starting hub scraping..."
                await bot.send_message(message.author, hub_wait_msg)
                await asyncio.sleep(60)

                hub_start_msg = "Starting hub match scraping for all configured hubs..."
                await bot.send_message(message.author, hub_start_msg)

                hub_scraping_task = asyncio.create_task(process_all_hubs(bot))
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
        finally:
            # Reset service running flag only if continuous scraping was not already
            # running before force was called (avoid clearing a legitimate start)
            if not was_service_running:
                import sys
                _msc = sys.modules.get('commands.MatchScraperCommands')
                continuous_still_running = (
                    (_msc and _msc.scraping_task and not _msc.scraping_task.done())
                )
                if not continuous_still_running:
                    bot.is_service_running = False
                    await bot.update_status()
    
    elif command == "hub" and len(args) > 1:
        try:
            # Check if hub ID is provided
            hub_id = None
            hub_name = None
            
            if len(args) > 2:
                hub_id = args[2]
                if len(args) > 3:
                    hub_name = args[3]
            
            # Start hub scraping
            if args[1] == "scrape":
                await bot.send_message(message.author, f"Starting hub scraping for {hub_name or hub_id or 'default hub'}")
                
                if hub_id:
                    result = await start_hub_scraping(bot, hub_id, hub_name)
                else:
                    result = await process_all_hubs(bot)
                
                if result:
                    await bot.send_message(message.author, "Hub scraping completed successfully")
                else:
                    await bot.send_message(message.author, "Hub scraping encountered an error")
                
                return True
            
            # List configured hubs
            elif args[1] == "list":
                config = get_config()
                hubs = []
                if config.has_section('Faceit'):
                    i = 0
                    while config.has_option('Faceit', f'hub_{i}_id'):
                        hub_id = config.get('Faceit', f'hub_{i}_id')
                        hub_name = config.get('Faceit', f'hub_{i}_name', fallback=f"Hub {i+1}")
                        hubs.append({'id': hub_id, 'name': hub_name})
                        i += 1
                
                if not hubs:
                    await bot.send_message(message.author, "No hubs configured")
                    return True
                
                hub_list = []
                for i, hub in enumerate(hubs):
                    hub_id = hub.get('id', 'Unknown')
                    hub_name = hub.get('name', 'Unknown')
                    hub_list.append(f"{i+1}. {hub_name} ({hub_id})")
                
                await bot.send_message(message.author, "Configured hubs:\n" + "\n".join(hub_list))
                return True
            
        except Exception as e:
            await bot.send_message(message.author, f"Error: {str(e)}")
            return True
    
    elif command == "stop" and len(args) > 1 and args[1] == "scraper":
        try:
            import sys
            stopped_any = False

            # Stop this module's scraping task
            if scraping_task and not scraping_task.done():
                scraping_task.cancel()
                try:
                    await scraping_task
                except asyncio.CancelledError:
                    pass
                scraping_task = None
                stopped_any = True

            # Also stop MatchScraperCommands.scraping_task if it's running
            # (started via the 'start' command or auto-start)
            _msc = sys.modules.get('commands.MatchScraperCommands')
            _msc_scrape = getattr(_msc, 'scraping_task', None) if _msc else None
            if _msc_scrape and not _msc_scrape.done():
                _msc_scrape.cancel()
                try:
                    await _msc_scrape
                except asyncio.CancelledError:
                    pass
                setattr(_msc, 'scraping_task', None)
                stopped_any = True

            if stopped_any:
                bot.is_service_running = False
                await bot.update_status()
                await bot.send_message(message.author, "Match scraping stopped")
            else:
                await bot.send_message(message.author, "Match scraping is not running")

            # Stop hub scraping tasks (both sources)
            if hub_scraping_task and not hub_scraping_task.done():
                hub_scraping_task.cancel()
                try:
                    await hub_scraping_task
                except asyncio.CancelledError:
                    pass
                hub_scraping_task = None
                await bot.send_message(message.author, "Hub match scraping stopped")

            _msc_hub = getattr(_msc, 'hub_scraping_task', None) if _msc else None
            if _msc_hub and not _msc_hub.done():
                _msc_hub.cancel()
                try:
                    await _msc_hub
                except asyncio.CancelledError:
                    pass
                setattr(_msc, 'hub_scraping_task', None)
                await bot.send_message(message.author, "Hub match scraping stopped")

            return True
        except Exception as e:
            await bot.send_message(message.author, f"Error: {str(e)}")
            return True
    
    return False

async def continuous_scraping(bot=None):
    """
    Continuously scrape matches with random intervals.
    
    Args:
        bot: Discord bot instance
    """
    # Get configuration
    config = get_config()
    fetch_delay_min = config.getint('Downloader', 'min_fetch_delay', fallback=DEFAULT_MIN_DELAY)
    fetch_delay_max = config.getint('Downloader', 'max_fetch_delay', fallback=DEFAULT_MAX_DELAY)
    
    logger.info(f"continuous_scraping called with delay range {fetch_delay_min}-{fetch_delay_max} seconds")
    
    while True:
        try:
            # Wait for configured interval before starting
            wait_time = random.randint(fetch_delay_min, fetch_delay_max)
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
            result = await start_match_scraping(bot)
            
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
                hub_scraping_task = asyncio.create_task(process_all_hubs(bot))
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
