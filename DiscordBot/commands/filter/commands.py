"""
Command handling functions for the filter module.
"""

import os
import asyncio
import logging
from typing import Dict, List, Set, Tuple, Optional

from commands.filter.service import start_match_filtering
from commands.filter.config import get_config, get_available_months

# Set up logging
logger = logging.getLogger('discord_bot')

# Global task
filtering_task = None

async def handle_message(bot, message):
    """
    Handle message-based filter commands.
    
    Args:
        bot: Discord bot instance
        message: Message object
        
    Returns:
        bool: True if command was handled, False otherwise
    """
    content = message.content.lower().strip()
    args = content.split()
    command = args[0] if args else ""
    
    global filtering_task
    
    if command == "filter":
        try:
            # Check if filtering is already running
            if filtering_task and not filtering_task.done():
                await bot.send_message(message.author, "Match filtering is already running")
                return True
            
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
            
            # Start filtering
            await bot.send_message(message.author, f"Starting match filtering for {month or 'current month'}...")
            
            # Update service status and bot presence
            bot.is_filter_running = True
            await bot.update_status()
            
            # Start filtering
            filtering_task = asyncio.create_task(start_match_filtering(bot, month))
            result = await filtering_task
            
            if result:
                await bot.send_message(message.author, "Match filtering completed successfully")
            else:
                await bot.send_message(message.author, "Match filtering encountered an error")
            
            # Update service status and bot presence
            bot.is_filter_running = False
            await bot.update_status()
            
            return True
        except Exception as e:
            await bot.send_message(message.author, f"Error: {str(e)}")
            return True
    
    elif command == "start" and len(args) > 1 and args[1] == "filter":
        try:
            # Check if filtering is already running
            if filtering_task and not filtering_task.done():
                await bot.send_message(message.author, "Match filtering is already running")
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
            
            # Create and start the continuous filtering task
            await bot.send_message(message.author, f"Started continuous match filtering for {month or 'current month'}")
            
            # Update service status and bot presence
            bot.is_filter_running = True
            await bot.update_status()
            
            # Start filtering
            filtering_task = asyncio.create_task(continuous_filtering(bot, month))
            
            return True
        except Exception as e:
            await bot.send_message(message.author, f"Error: {str(e)}")
            return True
    
    elif command == "stop" and len(args) > 1 and args[1] == "filter":
        try:
            # Stop filtering task if it exists
            if filtering_task and not filtering_task.done():
                filtering_task.cancel()
                try:
                    await filtering_task
                except asyncio.CancelledError:
                    pass
                filtering_task = None
                # Update service status and bot presence
                bot.is_filter_running = False
                await bot.update_status()
                await bot.send_message(message.author, "Match filtering stopped")
            else:
                await bot.send_message(message.author, "Match filtering is not running")
            
            return True
        except Exception as e:
            await bot.send_message(message.author, f"Error: {str(e)}")
            return True
    
    return False

async def continuous_filtering(bot=None, month=None):
    """
    Continuously filter matches with intervals.
    
    Args:
        bot: Discord bot instance
        month: Month to filter (e.g., "February")
    """
    import random
    from datetime import datetime, timedelta
    
    # Get configuration
    config = get_config()
    filter_interval = config.get('filter', {}).get('interval', 600)  # Default to 10 minutes
    
    logger.info(f"continuous_filtering called for {month or 'current month'}")
    
    while True:
        try:
            # Start filtering
            result = await start_match_filtering(bot, month)
            
            if result:
                logger.info("Match filtering completed successfully")
            else:
                logger.error("Match filtering encountered an error")
            
            # Wait for configured interval before starting again
            next_filter = datetime.now() + timedelta(seconds=filter_interval)
            
            print("\n" + "=" * 50)
            print("      NEXT FILTER SCHEDULE")
            print("=" * 50)
            print(f"🔄 Next Filter: {next_filter.strftime('%H:%M:%S')}")
            print("=" * 50)
            
            logger.info(f"Waiting {filter_interval} seconds before next filter...")
            logger.info(f"Next filter scheduled for: {next_filter.strftime('%H:%M:%S')}")
            
            await asyncio.sleep(filter_interval)
        
        except asyncio.CancelledError:
            logger.info("Filtering task cancelled")
            break
        except Exception as e:
            logger.error(f"Error in continuous filtering: {str(e)}")
            # Wait before retrying on error
            await asyncio.sleep(60)

def setup(bot):
    """Required setup function for the extension"""
    logger.info("Filter commands module setup complete")
    bot.is_filter_running = False
    return True
