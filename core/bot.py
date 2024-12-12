import os
import sys
import json
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
    level=logging.ERROR,
    format='%(levelname)s - %(message)s',
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
        
        super().__init__(
            intents=intents,
            activity=nextcord.Activity(type=nextcord.ActivityType.watching, name="for demos")
        )

        # Store owner ID from config
        self.owner_id = self.config['discord']['owner_id']
        
        # Load command modules
        self.load_commands()

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
            # Get commands directory path relative to bot.py
            core_dir = os.path.dirname(os.path.abspath(__file__))
            project_dir = os.path.dirname(core_dir)
            commands_dir = os.path.join(project_dir, "commands")
            
            # Add project directory to Python path if not already there
            if project_dir not in sys.path:
                sys.path.insert(0, project_dir)
                logger.debug(f"Added {project_dir} to Python path")
            
            # Import and store command modules
            self.command_modules = []
            for filename in os.listdir(commands_dir):
                if filename.endswith(".py") and not filename.startswith("__"):
                    try:
                        module_name = f"commands.{filename[:-3]}"
                        
                        # Import the module using importlib to ensure proper package resolution
                        import importlib
                        module = importlib.import_module(module_name)
                        
                        # Call setup function
                        if hasattr(module, 'setup'):
                            module.setup(self)
                        
                        # Store module if it has handle_message
                        if hasattr(module, 'handle_message'):
                            self.command_modules.append(module)
                            logger.debug(f"Successfully loaded module {module_name}")
                    except Exception as e:
                        logger.error(f'Failed to load module {filename}: {e}')
        except Exception as e:
            logger.error(f"Error loading commands: {str(e)}")

    async def on_ready(self):
        """Called when the bot is ready"""
        logger.info(f"Bot is ready as {self.user}")

    def is_owner(self, user_id):
        """Check if a user is the bot owner"""
        return user_id == self.owner_id

    async def on_message(self, message):
        """Handle message events"""
        # Don't process messages from the bot itself
        if message.author == self.user:
            return

        # Only process messages from the bot owner
        if not self.is_owner(message.author.id):
            return

        # Let each command module handle the message
        command_handled = False
        for module in self.command_modules:
            try:
                # Each module should return True if it handled the command
                handled = await module.handle_message(self, message)
                if handled:
                    command_handled = True
                    # Break after first module handles the command
                    break
            except Exception as e:
                logger.error(f"Error in module {module.__name__}: {str(e)}")
                await self.send_message(message.author, f"Error processing command: {str(e)}")
                command_handled = True  # Consider errored commands as handled
                break

        # If no module handled the command, send unknown command message
        if not command_handled:
            await self.send_message(message.author, "Unknown command. Use 'help' to see available commands.")

    async def on_error(self, event, *args, **kwargs):
        """Handle any errors"""
        logger.error(f'Error in {event}: {sys.exc_info()[1]}')

# Export the DemoBot class
__all__ = ['DemoBot']
