# Import bot first to avoid circular imports
from .bot import DemoBot

# Import resscore functions
try:
    from .resscore import (
        calculate_storage_cost,
        get_match_ids_count,
        get_downloaded_match_ids_count,
        get_rejected_match_ids_count,
        get_undownloaded_match_ids_count,
        get_category_counts
    )
except ImportError as e:
    import logging
    logging.error(f"Error importing resscore: {e}")
    # Provide default implementations
    def calculate_storage_cost(): return 0.0, 0.0, 0
    def get_match_ids_count(): return 0
    def get_downloaded_match_ids_count(): return 0
    def get_rejected_match_ids_count(): return 0
    def get_undownloaded_match_ids_count(): return 0
    def get_category_counts(): return {'ace': 0, 'quad': 0, 'unapproved': 0}

__all__ = [
    'DemoBot',
    'calculate_storage_cost',
    'get_match_ids_count',
    'get_downloaded_match_ids_count',
    'get_rejected_match_ids_count',
    'get_undownloaded_match_ids_count',
    'get_category_counts'
]
