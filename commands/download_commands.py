import asyncio
import logging
from core.user_fetcher import fetch_user_matches
from core.user_demo_downloader import download_user_demos
from core.public_downloader import (
    start_downloading,
    get_download_stats
)

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
        if len(args) != 2:
            await bot.send_message(message.author, "Usage: download <category> (ace/quad)")
            return True
            
        category = args[1].lower()
        if category not in ['ace', 'quad']:
            await bot.send_message(message.author, "Invalid category. Please use 'ace' or 'quad'.")
            return True
            
        if download_state.download_task and not download_state.download_task.done():
            await bot.send_message(message.author, "Download already in progress")
            return True
            
        try:
            download_state.download_task = asyncio.create_task(start_downloading(category))
            await bot.send_message(message.author, f"Started downloading {category} demos")
            return True
        except Exception as e:
            await bot.send_message(message.author, f"Error starting download: {str(e)}")
            return True

    elif content == 'dlstats':
        try:
            stats = await get_download_stats()
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
