import asyncio
import logging
from datetime import datetime
from core.FaceitUserFetcher import fetch_user_matches
from core.UserDemoDownloader import download_user_demos
from core.DemoDownloader import (
    start_downloading,
    get_download_stats
)

def validate_month(month: str) -> str:
    """
    Validate and format month name.
    Returns formatted month name or None if invalid.
    """
    try:
        # Convert to datetime to validate month name
        month_num = datetime.strptime(month, "%B").month
        # Convert back to month name to ensure consistent capitalization
        return datetime(2000, month_num, 1).strftime("%B")
    except ValueError:
        return None

logger = logging.getLogger('discord_bot')

class DownloadState:
    def __init__(self):
        self.download_task = None

download_state = DownloadState()

async def handle_message(bot, message):
    """Handle message-based download commands"""
    content = message.content.lower()
    args = content.split()
    command = args[0] if args else ""

    if command == 'download':
        if len(args) != 4:
            await bot.send_message(message.author, "Usage: download <category> <month> <number>\nExample: download ace february 100")
            return True
            
        category, month, number = args[1:4]
        
        # Validate category
        if category.lower() not in ['ace', 'quad']:
            await bot.send_message(message.author, "Invalid category. Please use 'ace' or 'quad'.")
            return True
            
        # Validate month
        formatted_month = validate_month(month.capitalize())
        if not formatted_month:
            await bot.send_message(message.author, "Invalid month name. Please use full month name (e.g., February).")
            return True
            
        # Validate number
        try:
            limit = None if number.lower() == 'all' else int(number)
            if limit is not None and limit <= 0:
                await bot.send_message(message.author, "Number must be positive or 'all'.")
                return True
        except ValueError:
            await bot.send_message(message.author, "Invalid number. Please use a positive number or 'all'.")
            return True
            
        if download_state.download_task and not download_state.download_task.done():
            await bot.send_message(message.author, "Download already in progress")
            return True
            
        try:
            download_state.download_task = asyncio.create_task(
                start_downloading(category.lower(), formatted_month, limit)
            )
            await bot.send_message(
                message.author, 
                f"Started downloading {category} demos from {formatted_month}" + 
                (f" (limit: {limit})" if limit else " (all matches)")
            )
            return True
        except Exception as e:
            await bot.send_message(message.author, f"Error starting download: {str(e)}")
            return True

    elif content == 'dlstats':
        try:
            stats = get_download_stats()
            await bot.send_message(message.author, f"Download Stats:\n{stats}")
            return True
        except Exception as e:
            await bot.send_message(message.author, f"Error getting stats: {str(e)}")
            return True

    elif command == 'fetch':
        if len(args) != 2:
            await bot.send_message(message.author, "Usage: fetch <username>")
            return True
            
        username = args[1]
        try:
            await fetch_user_matches(username)
            await bot.send_message(message.author, f"Fetched matches for user {username}")
            return True
        except Exception as e:
            await bot.send_message(message.author, f"Error fetching matches: {str(e)}")
            return True

    elif command == 'getdemos':
        if len(args) != 2:
            await bot.send_message(message.author, "Usage: getdemos <username>")
            return True
            
        username = args[1]
        try:
            await download_user_demos(username)
            await bot.send_message(message.author, f"Downloaded demos for user {username}")
            return True
        except Exception as e:
            await bot.send_message(message.author, f"Error downloading demos: {str(e)}")
            return True

    return False

def setup(bot):
    """Required setup function for the extension"""
    return True
