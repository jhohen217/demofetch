import asyncio
import random
import json
import logging
from core import start_match_scraping, start_match_filtering, stop_processes
from core.parser import DemoParser

# Set up logging
logger = logging.getLogger('discord_bot')

class ScraperCommands:
    def __init__(self):
        self.scraping_task = None
        self.filtering_task = None
        self.parsing_task = None
        
        # Load configuration
        with open('config.json', 'r') as f:
            config = json.load(f)
        
        # Get fetch delay settings from config
        self.fetch_delay_min = config.get('downloader', {}).get('fetch_delay', {}).get('min', 180)
        self.fetch_delay_max = config.get('downloader', {}).get('fetch_delay', {}).get('max', 300)

    async def continuous_scraping(self):
        """Continuously scrape matches with random intervals"""
        while True:
            try:
                # Start scraping
                result = await start_match_scraping()
                if result:
                    logger.info("Match scraping completed successfully")
                    
                    # Start filtering
                    self.filtering_task = asyncio.create_task(start_match_filtering())
                    
                    # Start parsing in parallel for any previously unfiltered matches
                    parser = DemoParser()
                    self.parsing_task = asyncio.create_task(parser.parse_new_matches())
                    
                    # Wait for both tasks to complete
                    await asyncio.gather(self.filtering_task, self.parsing_task)
                    
                    logger.info("All processing completed")
                else:
                    logger.error("Match scraping encountered an error")

                # Wait for random interval before next fetch
                wait_time = random.randint(self.fetch_delay_min, self.fetch_delay_max)
                logger.info(f"Next fetch in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
                
            except asyncio.CancelledError:
                logger.info("Scraping task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in continuous scraping: {str(e)}")
                # Wait before retrying
                await asyncio.sleep(60)

    async def handle_message(self, bot, message):
        """Handle DM commands for scraper control"""
        content = message.content.lower().strip()

        if content == "start":
            try:
                if self.scraping_task and not self.scraping_task.done():
                    await bot.send_message(message.author, "Match scraping is already running")
                    return True

                # Create and start the continuous scraping task
                await bot.send_message(message.author, "Started fetching NA East Match IDs")
                self.scraping_task = asyncio.create_task(self.continuous_scraping())
                return True
                
            except Exception as e:
                await bot.send_message(message.author, f"Error: {str(e)}")
                return True

        elif content.startswith("stop"):
            try:
                args = content.split()
                service = args[1] if len(args) > 1 else None
                message_parts = []
                
                if service is None or service == "fetch":
                    # Stop scraping task if it exists
                    if self.scraping_task and not self.scraping_task.done():
                        self.scraping_task.cancel()
                        try:
                            await self.scraping_task
                        except asyncio.CancelledError:
                            pass
                        self.scraping_task = None
                        message_parts.append("Match scraping stopped")

                    # Stop filtering task if it exists
                    if self.filtering_task and not self.filtering_task.done():
                        self.filtering_task.cancel()
                        try:
                            await self.filtering_task
                        except asyncio.CancelledError:
                            pass
                        self.filtering_task = None
                        message_parts.append("Match filtering stopped")

                    # Stop parsing task if it exists
                    if self.parsing_task and not self.parsing_task.done():
                        self.parsing_task.cancel()
                        try:
                            await self.parsing_task
                        except asyncio.CancelledError:
                            pass
                        self.parsing_task = None
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
                    if self.parsing_task and not self.parsing_task.done():
                        self.parsing_task.cancel()
                        try:
                            await self.parsing_task
                        except asyncio.CancelledError:
                            pass
                        self.parsing_task = None
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
    """Add the scraper commands to the bot's command modules"""
    bot.command_modules.append(ScraperCommands())
