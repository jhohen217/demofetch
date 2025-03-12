import os
import sys
import logging
import asyncio

# Set up logging
logger = logging.getLogger('discord_bot')

def _get_script_path():
    """Get the path to the kill_collection_analyzer.py script"""
    try:
        # Navigate from commands directory to scripts directory
        commands_dir = os.path.dirname(os.path.abspath(__file__))
        project_dir = os.path.dirname(commands_dir)
        script_path = os.path.join(project_dir, 'scripts', 'kill_collection_analyzer.py')
        
        return script_path
    except Exception as e:
        logger.error(f"Error getting script path: {str(e)}")
        return None

async def handle_message(bot, message):
    """Handle message-based commands for kill collection analysis"""
    content = message.content.lower()
    
    if content.startswith('scan'):
        try:
            # Parse arguments
            args = content.split()
            
            # Default values
            collection_type = None
            month = None
            
            # Process arguments
            if len(args) > 1:
                # Check if the first argument is a valid type or month
                valid_types = ['ace', 'quad', 'triple', 'multi', 'double', 'single', 'all']
                valid_months = ['january', 'february', 'march', 'april', 'may', 'june', 
                               'july', 'august', 'september', 'october', 'november', 'december', 'all']
                
                # Log the arguments for debugging
                logger.info(f"Scan command arguments: {args}")
                
                if args[1].lower() in valid_types:
                    collection_type = args[1].upper() if args[1].lower() != 'all' else None
                    logger.info(f"Recognized type argument: {collection_type}")
                    
                    # Check for month as second argument
                    if len(args) > 2 and args[2].lower() in valid_months:
                        month = args[2].capitalize() if args[2].lower() != 'all' else None
                        logger.info(f"Recognized month argument: {month}")
                elif args[1].lower() in valid_months:
                    month = args[1].capitalize() if args[1].lower() != 'all' else None
                    logger.info(f"Recognized month argument: {month}")
                    
                    # Check for type as second argument (unusual but supported)
                    if len(args) > 2 and args[2].lower() in valid_types:
                        collection_type = args[2].upper() if args[2].lower() != 'all' else None
                        logger.info(f"Recognized type argument: {collection_type}")
            
            # Get the script path
            script_path = _get_script_path()
            if not script_path or not os.path.exists(script_path):
                await bot.send_message(message.author, "Error: Kill collection analyzer script not found.")
                return True
            
            # Prepare command to run the script
            # Try to find the correct Python executable
            python_executable = sys.executable
            if not python_executable or not os.path.exists(python_executable):
                # Try common Python executable names
                for exe in ['python', 'python3', 'py']:
                    try:
                        subprocess_result = await asyncio.create_subprocess_exec(
                            exe, '--version',
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE
                        )
                        await subprocess_result.communicate()
                        if subprocess_result.returncode == 0:
                            python_executable = exe
                            break
                    except:
                        continue
            
            if not python_executable:
                await bot.send_message(message.author, "Error: Could not find Python executable.")
                return True
                
            cmd = [python_executable, script_path]
            if collection_type:
                cmd.append(collection_type)
            if month:
                cmd.append(month)
            
            # Log the command being executed
            logger.info(f"Executing kill collection analyzer with command: {cmd}")
            
            # Send a processing message
            await bot.send_message(message.author, f"Processing kill collection data...")
            
            # Run the script as a subprocess
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            # Wait for the process to complete
            stdout, stderr = await process.communicate()
            
            # Get the output and error
            result = stdout.decode().strip()
            error = stderr.decode().strip()
            
            # Log both for debugging
            logger.info(f"Kill collection analyzer result: {result}")
            if error:
                logger.info(f"Kill collection analyzer stderr: {error}")
            
            # Check for errors in the return code
            if process.returncode != 0:
                error_msg = error if error else "Unknown error"
                logger.error(f"Error running kill collection analyzer (return code {process.returncode}): {error_msg}")
                await bot.send_message(message.author, f"Error analyzing kill collections: {error_msg}")
                return True
            
            # Send the result
            if result:
                await bot.send_message(message.author, result)
            else:
                await bot.send_message(message.author, "No results found for the specified criteria.")
        except Exception as e:
            logger.error(f"Error processing scan command: {str(e)}")
            await bot.send_message(message.author, f"An error occurred: {str(e)}")
        
        return True
    
    return False

def setup(bot):
    """Required setup function for the extension"""
    return True
