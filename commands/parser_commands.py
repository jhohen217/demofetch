import logging

logger = logging.getLogger('discord_bot')

async def handle_message(bot, message):
    """Handle message-based parser commands"""
    content = message.content.lower()
    args = content.split()
    command = args[0] if args else ""

    if command == 'parse':
        if len(args) not in [2, 3]:
            await bot.send_message(message.author, "Usage: parse <source> [number] (source: ace/quad/username)")
            return True

        source = args[1].lower()
        number = args[2] if len(args) == 3 else None

        if source not in ['ace', 'quad', 'username']:
            await bot.send_message(message.author, "Invalid source. Please use 'ace', 'quad', or a valid username.")
            return True

        try:
            # Placeholder for actual parsing logic
            parse_result = f"Parsed data from source: {source}"
            if number:
                parse_result += f" with number: {number}"
            
            await bot.send_message(message.author, parse_result)
            return True
        except Exception as e:
            error_msg = f"An error occurred while parsing: {str(e)}"
            await bot.send_message(message.author, error_msg)
            return True

    return False

def setup(bot):
    """Required setup function for the extension"""
    return True
