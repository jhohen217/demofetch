"""
Demo parser commands module.
This module is now a wrapper around the new parser package structure.
It imports and re-exports all the functionality from the parser package.
"""

import logging
import asyncio
from typing import Dict, List, Set, Tuple, Optional, Counter

# Import utilities from the parser package
from commands.parser.utils import (
    format_match_id,
    extract_short_id,
    format_time_duration,
    read_file_lines,
    write_file_lines,
    append_file_line,
    async_read_file_lines,
    async_write_file_lines,
    async_append_file_line,
    retry_operation
)

# Import config functions
from commands.parser.config import (
    get_config,
    get_month_files,
    get_available_months,
    get_demo_path,
    get_demo_path_async
)

# Import demo processor functions
from commands.parser.demo_processor import (
    process_demo,
    count_tickbytick_by_type
)

# Import queue manager functions
from commands.parser.queue_manager import (
    prepare_parse_queue,
    prepare_parse_queue_async
)

# Import batch processor functions
from commands.parser.batch_processor import (
    process_month_queue,
    process_month_queue_async
)

# Import service functions
from commands.parser.service import (
    parser_task,
    stop_parser_event,
    parser_stats,
    get_parser_stats,
    parser_loop,
    start_parsing
)

# Import rebuilder functions
from commands.parser.rebuilder import (
    rebuild_parsed_file,
    rebuild_all_parsed_files,
    rebuild_downloaded_file,
    rebuild_all_downloaded_files
)

# Import command handlers
from commands.parser.commands import (
    setup,
    handle_message
)

logger = logging.getLogger('discord_bot')
