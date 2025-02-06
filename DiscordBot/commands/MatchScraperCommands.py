import os
import asyncio
import random
import json
import logging
import nextcord
from datetime import datetime, timedelta
from core.FaceitMatchScraper import start_match_scraping
from core.MatchScoreFilter import start_match_filtering
from core.DemoDownloader import stop_processes
from textfiles.undated.DateFetch import start_date_fetching
import subprocess
import os

# Set up logging
logger = logging.getLogger('discord_bot')

# Global tasks
scraping_task = None
filtering_task = None
parsing_task = None
datefetch_task = None

# Load configuration from project root
core_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # DiscordBot directory
config_path = os.path.join(os.path.dirname(core_dir), 'config.json')

with open(config_path, 'r') as f:
    config = json.load(f)

# Get fetch delay settings from config
fetch_delay_min = config.get('downloader', {}).get('fetch_delay', {}).get('min', 180)
fetch_delay_max = config.get('downloader', {}).get('fetch_delay', {}).get('max', 300)

async def continuous_scraping(bot=None):
    """Continuously scrape matches with random intervals"""
    while True:
        try:
            # Wait for configured interval before starting
            wait_time = random.randint(fetch_delay_min, fetch_delay_max)
            logger.info(f"Waiting {wait_time} seconds before next scrape...")
            await asyncio.sleep(wait_time)
            
            # Get textfiles directory and current month from config
            textfiles_dir = config['project']['textfiles_directory']
            current_month = datetime.now().strftime("%B")  # e.g., "February"
            month_dir = os.path.join(textfiles_dir, current_month)
            month_lower = current_month.lower()
            match_ids_path = os.path.join(month_dir, f"match_ids_{month_lower}.txt")
            
            # Get count before scraping
            match_count_before = 0
            if os.path.exists(match_ids_path):
                with open(match_ids_path, 'r') as f:
                    match_count_before = sum(1 for line in f if line.strip())
            else:
                # Create month directory if it doesn't exist
                os.makedirs(month_dir, exist_ok=True)
            
            # Start scraping
            result = await start_match_scraping(bot)
            if result:
                logger.info("Match scraping completed successfully")
                
                # Always run filtering to catch any unfiltered matches
                logger.info("Starting match filtering...")
                
                # Start filtering task
                global filtering_task
                try:
                    # Run filtering
                    filter_result = await start_match_filtering(bot)
                    if filter_result:
                        logger.info("Match filtering completed successfully")
                    else:
                        logger.error("Match filtering failed")
                except Exception as filter_error:
                    logger.error(f"Error in filtering task: {str(filter_error)}")
                
                # Start C# parser
                global parsing_task
                try:
                    # Get path to StartCollectionsParse.py
                    parser_script = os.path.join(
                        os.path.dirname(core_dir),
                        "CSharpParser", "demofile-net", "examples",
                        "DemoFile.Example.FastParser", "StartCollectionsParse.py"
                    )
                    
                    # Get demos directory from config
                    demos_dir = config['project']['public_demos_directory']
                    
                    # Run the C# parser
                    parsing_task = asyncio.create_task(
                        asyncio.to_thread(
                            subprocess.run,
                            ["python", parser_script, demos_dir],
                            capture_output=True,
                            text=True
                        )
                    )
                    result = await parsing_task
                    
                    if result.returncode == 0:
                        logger.info("Match parsing completed successfully")
                        if result.stdout:
                            logger.info(f"Parser output: {result.stdout}")
                    else:
                        logger.error(f"Parser error: {result.stderr}")
                        
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
    global scraping_task, filtering_task, parsing_task, datefetch_task

    if content == "start datefetch":
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

    elif content == "start":
        try:
            if scraping_task and not scraping_task.done():
                await bot.send_message(message.author, "Match scraping is already running")
                return True

            # Create and start the continuous scraping task
            await bot.send_message(message.author, "Started fetching NA East Match IDs")
            # Update service status and bot presence
            bot.is_service_running = True
            await bot.update_status()
            scraping_task = asyncio.create_task(continuous_scraping(bot))
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

    elif content == "status":
        try:
            # Build status message
            status_parts = []
            
            # Check scraping status
            if scraping_task and not scraping_task.done():
                status_parts.append("🟢 Match scraping is ACTIVE")
                
                # Get next scrape time
                frame = scraping_task.get_coro().cr_frame
                if frame and 'wait_time' in frame.f_locals:
                    wait_time = frame.f_locals['wait_time']
                    elapsed = frame.f_locals.get('_time', 0)
                    remaining = max(0, wait_time - elapsed)
                    next_scrape = datetime.now() + timedelta(seconds=remaining)
                    status_parts.append(f"Next scrape at: {next_scrape.strftime('%H:%M:%S')}")
            else:
                status_parts.append("🔴 Match scraping is INACTIVE")

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
    return True
