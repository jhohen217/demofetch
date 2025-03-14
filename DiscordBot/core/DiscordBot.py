import os
import sys
import json
import asyncio
import logging
from pathlib import Path

try:
    import nextcord
except ImportError:
    print("Error: nextcord package not found. Please install it using 'pip install nextcord'")
    sys.exit(1)

# Configure logging to suppress nextcord messages
logging.getLogger('nextcord').setLevel(logging.ERROR)
logging.getLogger('nextcord.client').setLevel(logging.ERROR)
logging.getLogger('nextcord.gateway').setLevel(logging.ERROR)
logging.getLogger('nextcord.websocket').setLevel(logging.ERROR)

# Set up minimal logging for our bot
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG level for more info
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('discord_bot')

class DemoBot(nextcord.Client):
    def __init__(self, config):
        self.config = config
        
        # Initialize bot with intents
        intents = nextcord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.messages = True
        
        # Initialize without activity - we'll set it in on_ready
        super().__init__(
            intents=intents
        )

        # Store owner ID from config
        self.owner_id = self.config['discord']['owner_id']
        
        # Initialize command modules list
        self.command_modules = []
        
        # Load command modules
        self.load_commands()

        # Track service status
        self.is_service_running = False

    def format_message(self, content: str) -> str:
        """Format a message in a code block for highlighted appearance"""
        return f"```\n{content}\n```"

    async def send_message(self, destination, content: str):
        """Send a formatted message to the specified destination"""
        try:
            await destination.send(self.format_message(content))
        except Exception as e:
            logger.error(f"Error sending message: {str(e)}")
            # Fallback to channel if DM fails
            if hasattr(destination, 'channel'):
                await destination.channel.send(self.format_message(content))

    def load_commands(self):
        """Load all command modules"""
        try:
            # Get commands directory path relative to DiscordBot.py
            core_dir = os.path.dirname(os.path.abspath(__file__))
            project_dir = os.path.dirname(core_dir)
            commands_dir = os.path.join(project_dir, "commands")
            
            # Add project directory to Python path if not already there
            if project_dir not in sys.path:
                sys.path.insert(0, project_dir)
                logger.debug(f"Added {project_dir} to Python path")
            
            # Import command modules
            for filename in os.listdir(commands_dir):
                if filename.endswith(".py") and not filename.startswith("__"):
                    try:
                        module_name = f"commands.{filename[:-3]}"
                        logger.debug(f"Attempting to load module: {module_name}")
                        
                        # Import the module using importlib to ensure proper package resolution
                        import importlib
                        module = importlib.import_module(module_name)
                        
                        # Store module if it has handle_message
                        if hasattr(module, 'handle_message'):
                            # Call setup function if it exists
                            if hasattr(module, 'setup'):
                                try:
                                    module.setup(self)
                                except Exception as e:
                                    logger.error(f"Error in setup for {module_name}: {e}")
                            
                            self.command_modules.append(module)
                            logger.debug(f"Successfully loaded module {module_name}")
                        else:
                            logger.warning(f"Module {module_name} has no handle_message function")
                    except Exception as e:
                        logger.error(f'Failed to load module {filename}: {e}')
                        logger.exception("Full traceback:")
        except Exception as e:
            logger.error(f"Error loading commands: {str(e)}")
            logger.exception("Full traceback:")

    async def periodic_match_filtering(self):
        """Run match filtering periodically"""
        try:
            from core.MatchScoreFilter import start_match_filtering
            while True:
                logger.info("Starting periodic match filtering...")
                try:
                    await start_match_filtering()
                    logger.info("Periodic match filtering completed")
                except Exception as e:
                    logger.error(f"Error in periodic match filtering: {str(e)}")
                    logger.exception("Full traceback:")
                
                # Wait 5 minutes before next filtering
                await asyncio.sleep(300)
        except Exception as e:
            logger.error(f"Error in periodic match filtering task: {str(e)}")
            logger.exception("Full traceback:")

    async def start_background_tasks(self):
        """Start background tasks like match filtering and optionally match scraping"""
        try:
            # Start periodic match filtering
            self.loop.create_task(self.periodic_match_filtering())
            logger.info("Started periodic match filtering task")
            
            # Check if auto-start scraping is enabled in config
            auto_start_scraping = self.config.get('downloader', {}).get('auto_start_scraping', False)
            
            if auto_start_scraping:
                # Start match scraping using MatchScraperCommands
                try:
                    from commands import MatchScraperCommands
                    logger.info("Auto-starting match scraping service (enabled in config)...")
                    
                    # Set service as running
                    self.is_service_running = True
                    await self.update_status()
                    
                    # Create and start the continuous scraping task
                    scraping_task = self.loop.create_task(MatchScraperCommands.continuous_scraping(self))
                    logger.info("Match scraping service started successfully")
                except Exception as scraper_error:
                    logger.error(f"Error starting match scraping service: {str(scraper_error)}")
                    logger.exception("Full traceback:")
            else:
                logger.info("Auto-start scraping is disabled in config. Use 'start' command to start scraping manually.")
            
        except Exception as e:
            logger.error(f"Error starting background tasks: {str(e)}")
            logger.exception("Full traceback:")

    async def update_status(self):
        """Update bot status based on service state"""
        if self.is_service_running:
            await self.change_presence(activity=nextcord.Activity(type=nextcord.ActivityType.watching, name="for demos"))
        else:
            await self.change_presence(activity=nextcord.Activity(type=nextcord.ActivityType.listening, name="for commands"))

    async def on_ready(self):
        """Called when the bot is ready"""
        logger.info(f"Bot is ready as {self.user}")
        # Log loaded command modules
        logger.info(f"Loaded command modules: {[m.__name__ for m in self.command_modules]}")
        
        # Send DM to owner
        try:
            owner = await self.fetch_user(self.owner_id)
            if owner:
                await self.send_message(owner, f"Bot is now online and ready as {self.user}")
            
            # Set initial status
            await self.update_status()
                
            # Start background tasks
            self.loop.create_task(self.start_background_tasks())
            
        except Exception as e:
            logger.error(f"Failed to send startup DM to owner: {e}")

    def is_owner(self, user_id):
        """Check if a user is the bot owner"""
        return user_id == self.owner_id

    async def on_message(self, message):
        """Handle message events"""
        # Don't process messages from the bot itself
        if message.author == self.user:
            return

        # Only process direct messages from the bot owner
        if not self.is_owner(message.author.id) or message.guild is not None:
            return

        # Log the received message
        logger.debug(f"Processing message: {message.content}")
        logger.debug(f"Available command modules: {[m.__name__ for m in self.command_modules]}")

        # Let each command module handle the message
        command_handled = False
        for module in self.command_modules:
            try:
                # Each module should return True if it handled the command
                handled = await module.handle_message(self, message)
                if handled:
                    command_handled = True
                    logger.debug(f"Command handled by module: {module.__name__}")
                    # Break after first module handles the command
                    break
            except Exception as e:
                logger.error(f"Error in module {module.__name__}: {str(e)}")
                logger.exception("Full traceback:")
                command_handled = True  # Consider errored commands as handled

        # If no module handled the command, send unknown command message
        if not command_handled:
            logger.warning(f"No module handled command: {message.content}")
            await self.send_message(message.author, "Unknown command. Use 'help' to see available commands.")

    async def on_error(self, event, *args, **kwargs):
        """Handle any errors"""
        logger.error(f'Error in {event}: {sys.exc_info()[1]}')
        logger.exception("Full traceback:")

# Export the DemoBot class
__all__ = ['DemoBot']
