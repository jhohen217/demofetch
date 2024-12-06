import discord
from discord import app_commands
from discord.ext import commands
import json
import os
import asyncio
import threading
from typing import Optional
import sys
from pathlib import Path

# Add the project root to Python path
current_dir = Path(__file__).parent.absolute()
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))

from user_fetcher import fetch_user_matches
from user_demo_downloader import download_user_demos, get_api_usage
from public_downloader import (
    start_match_scraping,
    start_downloading,
    stop_processes,
    calculate_storage_cost,
    get_match_ids_count,
    get_downloaded_match_ids_count,
    get_rejected_match_ids_count,
    get_undownloaded_match_ids_count,
    get_category_counts,
    get_download_stats,
    downloader_thread
)

# Load configuration
if not os.path.exists('config.json'):
    print("Error: config.json not found. Please copy config.json.example to config.json and fill in your bot token.")
    exit(1)

with open('config.json', 'r') as f:
    config = json.load(f)

# Set up the bot with necessary intents
intents = discord.Intents.default()
intents.message_content = True
intents.guilds = True  # Required for slash commands

# Create bot instance with command tree
class DemoBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=intents)
        self.scraping_task = None
        self.download_check_task = None
        
    async def setup_hook(self):
        # This will sync commands with Discord
        await self.tree.sync()
        print("Command tree synced")

bot = DemoBot()

# Owner ID from config
OWNER_ID = config['discord']['owner_id']

def is_owner():
    """Check if the user is the bot owner"""
    async def predicate(interaction: discord.Interaction) -> bool:
        if interaction.user.id != OWNER_ID:
            await interaction.response.send_message("You don't have permission to use this command.", ephemeral=True)
            return False
        return True
    return app_commands.check(predicate)

@bot.event
async def on_ready():
    status_msg = f'Logged in as {bot.user.name}\nOwner ID: {OWNER_ID}'
    print(status_msg)
    print('------')
    try:
        await bot.tree.sync()
        sync_msg = "Commands synced successfully!"
        print(sync_msg)
    except Exception as e:
        error_msg = f"Failed to sync commands: {e}"
        print(error_msg)

@bot.tree.command(name="ping", description="Check if the bot is responsive")
@is_owner()
async def ping(interaction: discord.Interaction):
    await interaction.response.send_message("pong!")

@bot.tree.command(name="fetch", description="Fetch FACEIT matches for a given username and save to file")
@is_owner()
@app_commands.describe(username="FACEIT username to fetch matches for")
async def fetch(interaction: discord.Interaction, username: str):
    await interaction.response.defer()
    
    try:
        success, message = fetch_user_matches(username)
        
        if success:
            file_path = f"{username}_matches.txt"
            if os.path.exists(file_path):
                await interaction.followup.send(
                    content=message,
                    file=discord.File(file_path, filename=f"{username}_matches.txt")
                )
            else:
                await interaction.followup.send(message)
        else:
            await interaction.followup.send(f"Error: {message}")
    except Exception as e:
        error_msg = f"An error occurred: {str(e)}"
        print(error_msg)
        await interaction.followup.send(error_msg)

async def check_download_completion(interaction: discord.Interaction):
    """Check if downloads are complete and send stats"""
    try:
        while True:
            if not downloader_thread or not downloader_thread.is_alive():
                stats = get_download_stats()
                if stats['total'] > 0:  # Only send message if there were downloads
                    completion_msg = (
                        "📥 Download Complete!\n"
                        f"Total matches processed: {stats['total']}\n"
                        f"Successfully downloaded: {stats['successful']}\n"
                        f"Failed: {stats['failed']}\n"
                        f"Rejected: {stats['rejected']}\n"
                    )
                    if stats['last_match_id']:
                        completion_msg += f"Last downloaded match: {stats['last_match_id']}"
                    
                    try:
                        await interaction.followup.send(completion_msg, ephemeral=True)
                    except discord.NotFound:
                        print("Interaction expired, couldn't send completion message")
                break
            await asyncio.sleep(1)
    except Exception as e:
        print(f"Error in download completion check: {e}")

@bot.tree.command(
    name="download",
    description="Download demos for a user or category (auto/ace/quad/acequad)"
)
@is_owner()
@app_commands.describe(
    target="Username or category (auto/ace/quad/acequad)",
    number="Number of demos to download (optional for categories)"
)
async def download(interaction: discord.Interaction, target: str, number: Optional[int] = None):
    await interaction.response.defer()
    
    try:
        # Check if target is a category
        categories = ['auto', 'ace', 'quad', 'acequad']
        if target.lower() in categories:
            category = target.lower()
            result = start_downloading(category=category, limit=number)
            await interaction.followup.send(result)
            
            # Start checking for download completion
            if bot.download_check_task:
                bot.download_check_task.cancel()
            bot.download_check_task = asyncio.create_task(check_download_completion(interaction))
            return

        # If not a category, treat as username
        if number is None:
            await interaction.followup.send("Error: Number of demos is required when downloading user demos")
            return

        # Handle user demo download
        fetch_msg = f"Fetching latest matches for {target}..."
        print(fetch_msg)
        await interaction.followup.send(fetch_msg)
        
        fetch_success, fetch_message = fetch_user_matches(target)
        if not fetch_success:
            error_msg = f"Error fetching matches: {fetch_message}"
            print(error_msg)
            await interaction.followup.send(error_msg)
            return

        download_msg = f"Starting download of {number} demos for {target}..."
        print(download_msg)
        await interaction.followup.send(download_msg)
        
        success, result = download_user_demos(target, number)
        if success:
            quota = result['quota_info']
            quota_msg = (
                f"Download Complete!\n"
                f"{result['summary']}\n\n"
                f"API Quota Status:\n"
                f"Downloads: {quota['used']}/{quota['total']}\n"
                f"Bandwidth: {int(quota['bytes_used'])/1024/1024:.2f}MB / {int(quota['bytes_total'])/1024/1024:.2f}MB"
            )
            print(quota_msg)
            await interaction.followup.send(quota_msg)
        else:
            error_msg = f"Error: {result}"
            print(error_msg)
            await interaction.followup.send(error_msg)
    except Exception as e:
        error_msg = f"An error occurred: {str(e)}"
        print(error_msg)
        await interaction.followup.send(error_msg)

@bot.tree.command(name="usage", description="Check storage information and demo statistics")
@is_owner()
async def usage(interaction: discord.Interaction):
    try:
        size_gb, cost, file_count = calculate_storage_cost()
        total_matches = get_match_ids_count()
        downloaded_matches = get_downloaded_match_ids_count()
        rejected_matches = get_rejected_match_ids_count()
        undownloaded_matches = get_undownloaded_match_ids_count()
        
        # Get category counts
        category_counts = get_category_counts()
        
        status_msg = (
            f"Storage Information:\n"
            f"Total Size: {size_gb:.2f} GB\n"
            f"Total Files: {file_count} demos\n"
            f"Estimated Cost: ${cost:.2f}\n\n"
            f"Match Categories:\n"
            f"Ace: {category_counts['ace']}\n"
            f"Quad: {category_counts['quad']}\n"
            f"Ace+Quad: {category_counts['acequad']}\n"
            f"Unapproved: {category_counts['unapproved']}\n\n"
            f"Download Status:\n"
            f"Total Matches: {total_matches}\n"
            f"Downloaded: {downloaded_matches}\n"
            f"Rejected: {rejected_matches}\n"
            f"Undownloaded: {undownloaded_matches}"
        )
        await interaction.response.send_message(status_msg, ephemeral=True)
    except Exception as e:
        await interaction.response.send_message(f"An error occurred: {str(e)}", ephemeral=True)

@bot.tree.command(name="start", description="Start fetching NA East match IDs (runs every 3-5 minutes)")
@is_owner()
async def start_scraper(interaction: discord.Interaction):
    try:
        if bot.scraping_task and not bot.scraping_task.done():
            await interaction.response.send_message("Match scraping is already running")
            return

        # Create and start the scraping task
        bot.scraping_task = asyncio.create_task(start_match_scraping())
        await interaction.response.send_message("Started fetching NA East Match IDs")
    except Exception as e:
        await interaction.response.send_message(f"Error: {str(e)}", ephemeral=True)

@bot.tree.command(name="stop", description="Stop all running processes (match fetching and downloading)")
@is_owner()
async def stop_all(interaction: discord.Interaction):
    try:
        # Cancel the scraping task if it exists
        if bot.scraping_task and not bot.scraping_task.done():
            bot.scraping_task.cancel()
            try:
                await bot.scraping_task
            except asyncio.CancelledError:
                pass
            bot.scraping_task = None

        # Cancel the download check task if it exists
        if bot.download_check_task and not bot.download_check_task.done():
            bot.download_check_task.cancel()
            try:
                await bot.download_check_task
            except asyncio.CancelledError:
                pass
            bot.download_check_task = None

        # Stop other processes
        result = stop_processes()
        await interaction.response.send_message("All processes stopped")
    except Exception as e:
        await interaction.response.send_message(f"Error: {str(e)}", ephemeral=True)

async def safe_send(interaction: discord.Interaction, content: str, ephemeral: bool = False):
    """Safely send a message through an interaction"""
    try:
        if not interaction.response.is_done():
            await interaction.response.send_message(content, ephemeral=ephemeral)
        else:
            await interaction.followup.send(content, ephemeral=ephemeral)
    except discord.errors.NotFound:
        print(f"Could not send message: {content}")
    except Exception as e:
        print(f"Error sending message: {str(e)}")

@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CheckFailure):
        # This error is already handled in the is_owner check
        pass
    else:
        error_msg = f"An error occurred: {str(error)}"
        print(error_msg)
        await safe_send(interaction, error_msg, ephemeral=True)

def run_bot():
    """Run the bot in a separate thread"""
    bot.run(config['discord']['token'])

def main():
    # Start the bot in a separate thread
    bot_thread = threading.Thread(target=run_bot)
    bot_thread.start()
    
    print("Bot is running. Type 'exit' to stop the bot.")
    print("Commands will be registered shortly...")
    
    # Main loop for command input
    while True:
        try:
            cmd = input().strip().lower()
            if cmd == 'exit':
                print("Shutting down bot...")
                asyncio.run_coroutine_threadsafe(bot.close(), bot.loop)
                break
        except EOFError:
            break
        except KeyboardInterrupt:
            break

    bot_thread.join()

if __name__ == "__main__":
    main()
