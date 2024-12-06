import asyncio
import os
import threading
from match_scrape import start_match_scraping
from score_filter import start_match_filtering
from bot import run_bot

async def main():
    """
    Main entry point that runs all processes:
    1. Discord Bot
    2. Match Scraping
    3. Score Filtering
    """
    try:
        print("\n" + "="*50)
        print("           FACEIT Demo Manager")
        print("="*50 + "\n")

        print("Starting all services...\n")

        # Start Discord bot in a separate thread
        print("=== Discord Bot ===")
        print("Starting Discord bot...\n")
        bot_thread = threading.Thread(target=run_bot)
        bot_thread.daemon = True  # Make thread daemon so it exits when main thread exits
        bot_thread.start()

        # Give the bot a moment to initialize
        await asyncio.sleep(2)
        
        print("Bot is running. Use Discord commands to interact:")
        print("- /start - Begin fetching NA East matches")
        print("- /stop - Stop all processes")
        print("- /download_queue - Start downloading demos")
        print("- /usage - Check storage and statistics")
        print("\nPress Ctrl+C to stop all processes\n")

        # Keep the main thread alive
        while True:
            await asyncio.sleep(1)

    except KeyboardInterrupt:
        print("\nShutting down...")
        return True
    except Exception as e:
        print(f"\nError in main process: {str(e)}")
        return False

if __name__ == "__main__":
    """
    This is the main entry point for the entire application.
    
    To start everything:
    1. Ensure you have config.json set up (copy from config.json.example)
    2. Run this file with: python start.py
    
    This will start:
    - Discord Bot (for commands and interaction)
    - Match Scraping (when started via /start command)
    - Score Filtering (runs automatically after match scraping)
    """
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown complete")
    except Exception as e:
        print(f"\nFatal error: {str(e)}")
        exit(1)
