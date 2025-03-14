import os
import asyncio
import random
import json
import logging
import nextcord
from datetime import datetime, timedelta

# Import the new modules
from commands.scraper.commands import handle_message as scraper_handle_message
from commands.scraper.commands import continuous_scraping
from commands.filter.commands import handle_message as filter_handle_message
from core.AsyncDemoDownloader import stop_processes
import subprocess

# Set up logging first so we can use it in the module
logger = logging.getLogger('discord_bot')

# Flag to track if date fetching is available
date_fetch_available = False

# Define a dummy function to use if the real one isn't available
async def start_date_fetching(bot=None):
    logger.error("Date fetching is not available. The required module is missing.")
    if bot and bot.owner:
        await bot.send_message(bot.owner, "Date fetching is not available. The required module is missing.")
    return False

# We'll try to import the real function later when it's actually needed

# Global variables

# Global tasks
scraping_task = None
hub_scraping_task = None
filtering_task = None
parsing_task = None
datefetch_task = None

# Match monitoring variables
last_new_match_time = None
no_matches_notification_sent = False

# Load configuration from project root
core_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # DiscordBot directory
config_path = os.path.join(os.path.dirname(core_dir), 'config.json')

with open(config_path, 'r') as f:
    config = json.load(f)

# Get fetch delay settings from config
fetch_delay_min = config.get('downloader', {}).get('fetch_delay', {}).get('min', 180)
fetch_delay_max = config.get('downloader', {}).get('fetch_delay', {}).get('max', 300)

async def handle_message(bot, message):
    """Handle message-based scraper commands"""
    content = message.content.lower().strip()
    global scraping_task, hub_scraping_task, filtering_task, parsing_task, datefetch_task
    
    # First, try to handle with the new scraper module
    if await scraper_handle_message(bot, message):
        return True
    
    # Then, try to handle with the new filter module
    if await filter_handle_message(bot, message):
        return True
    
    # Handle legacy commands that haven't been migrated yet
    if content == "start":
        try:
            if scraping_task and not scraping_task.done():
                await bot.send_message(message.author, "Match scraping is already running")
                return True

            # Create and start the continuous scraping task, waiting as normal
            await bot.send_message(message.author, "Started fetching NA East Match IDs")
            # Update service status and bot presence
            bot.is_service_running = True
            await bot.update_status()
            scraping_task = asyncio.create_task(continuous_scraping(bot))  # immediate=False is implicit
            return True

        except Exception as e:
            await bot.send_message(message.author, f"Error: {str(e)}")
            return True
    
    elif content == "start datefetch":
        try:
            if datefetch_task and not datefetch_task.done():
                await bot.send_message(message.author, "Date fetching is already running")
                return True

            # Create and start the date fetching task
            await bot.send_message(message.author, "Started processing undated matches")
            datefetch_task = asyncio.create_task(start_date_fetching(bot))
            return True
            
        except Exception as e:
            await bot.send_message(message.author, f"Error: {str(e)}")
            return True

    elif content == "reset":
        try:
            global last_new_match_time, no_matches_notification_sent
            
            # Reset the notification timer
            last_new_match_time = datetime.now()
            no_matches_notification_sent = False
            
            await bot.send_message(message.author, "Match notification timer has been reset. The system will now use the current time as the last successful match time.")
            return True
            
        except Exception as e:
            await bot.send_message(message.author, f"Error resetting notification timer: {str(e)}")
            return True
            
    elif content == "next":
        try:
            if not scraping_task or scraping_task.done():
                await bot.send_message(message.author, "Match scraping is not currently running. Use 'start' to begin scraping.")
                return True
                
            # Get the task's wait time from the sleep call
            frame = scraping_task.get_coro().cr_frame
            if frame and 'wait_time' in frame.f_locals:
                wait_time = frame.f_locals['wait_time']
                elapsed = frame.f_locals.get('_time', 0)  # Time elapsed in sleep
                remaining = max(0, wait_time - elapsed)
                
                next_scrape = datetime.now() + timedelta(seconds=remaining)
                
                response = (
                    f"Next scraping event in: {remaining} seconds\n"
                    f"Time of next scrape: {next_scrape.strftime('%H:%M:%S')}\n"
                    f"Scraping interval: {fetch_delay_min}-{fetch_delay_max} seconds"
                )
            else:
                response = (
                    f"Scraping is active but timing information is not available.\n"
                    f"Scraping interval: {fetch_delay_min}-{fetch_delay_max} seconds"
                )
                
            await bot.send_message(message.author, response)
            return True
            
        except Exception as e:
            await bot.send_message(message.author, f"Error getting next scrape time: {str(e)}")
            return True

    elif content == "status":
        try:
            # Build status message
            status_parts = []
            
            # Check auto-start scraping setting
            auto_start_scraping = config.get('downloader', {}).get('auto_start_scraping', False)
            if auto_start_scraping:
                status_parts.append("⚙️ Auto-start scraping is ENABLED in config")
            else:
                status_parts.append("⚙️ Auto-start scraping is DISABLED in config")
            
            # Check scraping status
            if scraping_task and not scraping_task.done():
                status_parts.append("\n🟢 Match scraping is ACTIVE")
                
                # Get next scrape time
                frame = scraping_task.get_coro().cr_frame
                if frame and 'wait_time' in frame.f_locals:
                    wait_time = frame.f_locals['wait_time']
                    elapsed = frame.f_locals.get('_time', 0)  # Time elapsed in sleep
                    remaining = max(0, wait_time - elapsed)
                    next_scrape = datetime.now() + timedelta(seconds=remaining)
                    status_parts.append(f"Next scrape at: {next_scrape.strftime('%H:%M:%S')}")
                
                # Add information about last successful match
                if last_new_match_time:
                    time_since_last_match = datetime.now() - last_new_match_time
                    hours = int(time_since_last_match.total_seconds() / 3600)
                    minutes = int((time_since_last_match.total_seconds() % 3600) / 60)
                    
                    if hours > 0:
                        status_parts.append(f"Last new match: {hours}h {minutes}m ago ({last_new_match_time.strftime('%Y-%m-%d %H:%M:%S')})")
                    else:
                        status_parts.append(f"Last new match: {minutes}m ago ({last_new_match_time.strftime('%Y-%m-%d %H:%M:%S')})")
                    
                    # Add warning indicator if approaching the 1-hour threshold
                    if time_since_last_match > timedelta(minutes=45):
                        status_parts.append("⚠️ No new matches in a while")
            else:
                status_parts.append("🔴 Match scraping is INACTIVE")
                
            # Check hub scraping status - only show if match scraping is inactive
            if scraping_task and not scraping_task.done():
                # Hub scraping is part of the main match scraping process
                status_parts.append("\n🟢 Hub match scraping is integrated with match scraping")
            elif hub_scraping_task and not hub_scraping_task.done():
                status_parts.append("\n🟢 Hub match scraping is ACTIVE")
            else:
                status_parts.append("\n🔴 Hub match scraping is INACTIVE")

            # Check date fetching status
            if datefetch_task and not datefetch_task.done():
                status_parts.append("\n🟢 Date fetching is ACTIVE")
            else:
                status_parts.append("\n🔴 Date fetching is INACTIVE")
                
            # Get download status from DemoDownloader
            download_stats = stop_processes.__globals__['download_stats']
            # Show downloads as active only if there are matches to process
            if download_stats['total'] > 0 and not download_stats['is_complete']:
                status_parts.append("\n🟢 Downloads are ACTIVE")
                status_parts.append(f"Progress: {download_stats['successful']}/{download_stats['total']} matches")
                status_parts.append(f"Failed: {download_stats['failed']}")
                status_parts.append(f"Rejected: {download_stats['rejected']}")
                if download_stats['last_match_id']:
                    status_parts.append(f"Current match: {download_stats['last_match_id']}")
            else:
                status_parts.append("\n🔴 Downloads are INACTIVE")
                if download_stats['last_match_id']:
                    status_parts.append(f"Last downloaded match: {download_stats['last_match_id']}")
            
            await bot.send_message(message.author, "\n".join(status_parts))
            return True
                
        except Exception as e:
            await bot.send_message(message.author, f"Error getting status: {str(e)}")
            return True

    elif content.startswith("stop"):
        try:
            args = content.split()
            service = args[1] if len(args) > 1 else None
            message_parts = []
            
            # Handle specific service stops
            if service == "fetch":
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
                    message_parts.append("Match scraping stopped")
                else:
                    message_parts.append("Fetch service wasn't running")
                    
                # Also stop hub scraping if it exists
                if hub_scraping_task and not hub_scraping_task.done():
                    hub_scraping_task.cancel()
                    try:
                        await hub_scraping_task
                    except asyncio.CancelledError:
                        pass
                    hub_scraping_task = None
                    message_parts.append("Hub match scraping stopped")

            elif service == "download":
                result = stop_processes()
                message_parts.append("Download service stopped")

            elif service == "parse":
                # Stop parsing task if it exists
                if parsing_task and not parsing_task.done():
                    parsing_task.cancel()
                    try:
                        await parsing_task
                    except asyncio.CancelledError:
                        pass
                    parsing_task = None
                    message_parts.append("Parse service stopped")
                else:
                    message_parts.append("Parse service wasn't running")

            elif service == "datefetch":
                # Stop date fetching task if it exists
                if datefetch_task and not datefetch_task.done():
                    datefetch_task.cancel()
                    try:
                        await datefetch_task
                    except asyncio.CancelledError:
                        pass
                    datefetch_task = None
                    message_parts.append("Date fetching stopped")
                else:
                    message_parts.append("Date fetching service wasn't running")

            # Handle 'stop' with no service specified - stop everything
            elif service is None:
                # Stop scraping task
                if scraping_task and not scraping_task.done():
                    scraping_task.cancel()
                    try:
                        await scraping_task
                    except asyncio.CancelledError:
                        pass
                    scraping_task = None
                    bot.is_service_running = False
                    await bot.update_status()
                    message_parts.append("Match scraping stopped")
                    
                # Stop hub scraping task
                if hub_scraping_task and not hub_scraping_task.done():
                    hub_scraping_task.cancel()
                    try:
                        await hub_scraping_task
                    except asyncio.CancelledError:
                        pass
                    hub_scraping_task = None
                    message_parts.append("Hub match scraping stopped")

                # Stop filtering task
                if filtering_task and not filtering_task.done():
                    filtering_task.cancel()
                    try:
                        await filtering_task
                    except asyncio.CancelledError:
                        pass
                    filtering_task = None
                    message_parts.append("Match filtering stopped")

                # Stop parsing task
                if parsing_task and not parsing_task.done():
                    parsing_task.cancel()
                    try:
                        await parsing_task
                    except asyncio.CancelledError:
                        pass
                    parsing_task = None
                    message_parts.append("Match parsing stopped")

                # Stop date fetching task
                if datefetch_task and not datefetch_task.done():
                    datefetch_task.cancel()
                    try:
                        await datefetch_task
                    except asyncio.CancelledError:
                        pass
                    datefetch_task = None
                    message_parts.append("Date fetching stopped")

                # Stop download processes
                result = stop_processes()
                message_parts.append("All download services stopped")

                if not message_parts:  # If no services were running
                    message_parts.append("No active services to stop")
            else:
                await bot.send_message(
                    message.author,
                    "Invalid service. Use 'stop [fetch|download|parse|datefetch]' or just 'stop' to stop all services."
                )
                return True

            await bot.send_message(message.author, "\n".join(message_parts))
            return True
        except Exception as e:
            await bot.send_message(message.author, f"Error: {str(e)}")
            return True

    return False

def setup(bot):
    """Required setup function for the extension"""
    logger.info("MatchScraperCommands module setup complete")
    
    # Set up the scraper and filter modules
    from commands.scraper.commands import setup as setup_scraper
    from commands.filter.commands import setup as setup_filter
    
    setup_scraper(bot)
    setup_filter(bot)
    
    return True
