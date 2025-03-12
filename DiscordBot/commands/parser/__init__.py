"""
Parser module for processing CS2 demo files.
This package contains modules for parsing, processing, and managing CS2 demo files.
"""

# Configure loggers
from commands.parser.logger_config import configure_loggers
main_logger, debug_logger = configure_loggers()

# Import commonly used functions and variables for easier access
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

from commands.parser.config import (
    get_config,
    get_month_files,
    get_available_months,
    get_demo_path,
    get_demo_path_async
)

from commands.parser.demo_processor import (
    process_demo,
    count_tickbytick_by_type
)

from commands.parser.queue_manager import (
    prepare_parse_queue,
    prepare_parse_queue_async
)

from commands.parser.batch_processor import (
    process_month_queue,
    process_month_queue_async
)

from commands.parser.service import (
    parser_task,
    stop_parser_event,
    parser_stats,
    get_parser_stats,
    parser_loop,
    start_parsing
)

from commands.parser.rebuilder import (
    rebuild_parsed_file,
    rebuild_all_parsed_files
)

# Setup function for the Discord bot extension
from commands.parser.commands import setup, handle_message
