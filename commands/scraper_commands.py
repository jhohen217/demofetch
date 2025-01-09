import os
import asyncio
import random
import json
import logging
import nextcord
from datetime import datetime, timedelta
from core.match_scrape import start_match_scraping
from core.score_filter import start_match_filtering
from core.public_downloader import stop_processes
from core.parser import DemoParser

# Set up logging
logger = logging.getLogger('discord_bot')

# Global tasks
scraping_task = None
filtering_task = None
parsing_task = None

# Load configuration
with open('config.json', 'r') as f:
    config = json.load(f)

# Get fetch delay settings from config
fetch_delay_min = config.get('downloader', {}).get('fetch_delay', {}).get('min', 180)
fetch_delay_max = config.get('downloader', {}).get('fetch_delay', {}).get('max', 300)

async def continuous_scraping():
    """Continuously scrape matches with random intervals"""
    while True:
        try:
            # Wait for configured interval before starting
            wait_time = random.randint(fetch_delay_min, fetch_delay_max)
            logger.info(f"Waiting {wait_time} seconds before next scrape...")
            await asyncio.sleep(wait_time)
            
            # Start scraping
            result = await start_match_scraping()
            if result:
                logger.info("Match scraping completed successfully")
                
                # Get project directory from config
                project_dir = config['project']['directory']
                match_ids_path = os.path.join(project_dir, "textfiles", "match_ids.txt")
                
                # Check if new matches were found
                with open(match_ids_path, 'r') as f:
                    match_count_before = sum(1 for line in f if line.strip())
                
                # Start filtering task
                global filtering_task
                logger.info("Starting match filtering...")
                try:
                    # Run filtering
                    filter_result = await start_match_filtering()
                    if filter_result:
                        logger.info("Match filtering completed successfully")
                        
                        # Check if new matches were added
                        with open(match_ids_path, 'r') as f:
                            match_count_after = sum(1 for line in f if line.strip())
                        
                        if match_count_after > match_count_before:
                            logger.info(f"Found {match_count_after - match_count_before} new matches, running filter again...")
                            # Run filtering again to catch new matches
                            filter_result = await start_match_filtering()
                            if filter_result:
                                logger.info("Second filtering pass completed successfully")
                            else:
                                logger.error("Second filtering pass failed")
                    else:
                        logger.error("Match filtering failed")
                except Exception as filter_error:
                    logger.error(f"Error in filtering task: {str(filter_error)}")
                
                # Start parsing task
                global parsing_task
                try:
                    parser = DemoParser()
                    parsing_task = asyncio.create_task(parser.parse_new_matches())
                    await parsing_task
                    logger.info("Match parsing completed")
                except Exception as parse_error:
                    logger.error(f"Error in parsing task: {str(parse_error)}")
                
                logger.info("All processing completed")
                
                # Calculate and display next scrape time
                next_scrape = datetime.now() + timedelta(seconds=wait_time)
                logger.info("=" * 50)
                logger.info(f"Next scrape scheduled for: {next_scrape.strftime('%H:%M:%S')}")
                logger.info("=" * 50)
            else:
                logger.error("Match scraping encountered an error")
                
                # Even on error, show next scrape time
                next_scrape = datetime.now() + timedelta(seconds=wait_time)
                logger.info("=" * 50)
                logger.info(f"Next scrape scheduled for: {next_scrape.strftime('%H:%M:%S')}")
                logger.info("=" * 50)
            
        except asyncio.CancelledError:
            logger.info("Scraping task cancelled")
            break
        except Exception as e:
            logger.error(f"Error in continuous scraping: {str(e)}")
            # Wait before retrying on error
            await asyncio.sleep(60)

async def handle_message(bot, message):
    """Handle message-based scraper commands"""
    content = message.content.lower().strip()
    global scraping_task, filtering_task, parsing_task

    if content == "start":
        try:
            if scraping_task and not scraping_task.done():
                await bot.send_message(message.author, "Match scraping is already running")
                return True

            # Create and start the continuous scraping task
            await bot.send_message(message.author, "Started fetching NA East Match IDs")
            # Change status to watching when scraping starts
            await bot.change_presence(activity=nextcord.Activity(type=nextcord.ActivityType.watching, name="for demos"))
            scraping_task = asyncio.create_task(continuous_scraping())
            return True
            
        except Exception as e:
            await bot.send_message(message.author, f"Error: {str(e)}")
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

    elif content.startswith("stop"):
        try:
            args = content.split()
            service = args[1] if len(args) > 1 else None
            message_parts = []
            
            if service is None or service == "fetch":
                # Stop scraping task if it exists
                if scraping_task and not scraping_task.done():
                    scraping_task.cancel()
                    try:
                        await scraping_task
                    except asyncio.CancelledError:
                        pass
                    scraping_task = None
                    # Change status back to listening when scraping stops
                    await bot.change_presence(activity=nextcord.Activity(type=nextcord.ActivityType.listening, name="for commands"))
                    message_parts.append("Match scraping stopped")

                # Stop filtering task if it exists
                if filtering_task and not filtering_task.done():
                    filtering_task.cancel()
                    try:
                        await filtering_task
                    except asyncio.CancelledError:
                        pass
                    filtering_task = None
                    message_parts.append("Match filtering stopped")

                # Stop parsing task if it exists
                if parsing_task and not parsing_task.done():
                    parsing_task.cancel()
                    try:
                        await parsing_task
                    except asyncio.CancelledError:
                        pass
                    parsing_task = None
                    message_parts.append("Match parsing stopped")

            if service is None:
                # Stop all other processes
                result = stop_processes()
                message_parts.append("All download services stopped")
                if not message_parts:  # If no services were running
                    message_parts.append("No active services to stop")
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
            elif service == "fetch":
                if not message_parts:  # If scraping wasn't running
                    message_parts.append("Fetch service wasn't running")
            else:
                await bot.send_message(
                    message.author,
                    "Invalid service. Use 'stop [fetch|download|parse]' or just 'stop' to stop all services."
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
    return True
