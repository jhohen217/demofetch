# Parser Module Optimization

This document explains the optimizations made to the parser module to reduce console spam and improve performance.

## Changes Made

1. **Created UUID-only File Processing**
   - Added a new function `create_uuid_only_file()` in `utils.py` that creates temporary files with UUIDs extracted from prefixed matchid files
   - This allows for faster and more efficient comparison between different files by using set operations on UUIDs

2. **Reduced Console Logging**
   - Created a separate debug logger that writes detailed logs to a file but not to the console
   - Moved verbose logging statements from the main logger to the debug logger
   - Added summary logging instead of logging each individual matchid check

3. **Optimized Comparison Logic**
   - Updated the queue preparation process to use set operations on UUID-only files
   - This is more efficient than extracting UUIDs on the fly during comparison

4. **Added Logger Configuration**
   - Created a new `logger_config.py` module to properly configure loggers
   - Set up the debug logger to write to a file but not output to the console
   - Configured the main logger to only show important information

## How It Works

### Before a Parse Job Begins

1. The system reads from multiple files:
   - `downloaded_[month].txt` (contains UUIDs without prefixes)
   - `parsed_[month].txt` (contains UUIDs of already parsed demos)
   - `rejected_[month].txt` (contains UUIDs of rejected demos)
   - `ace_matchids_[month].txt` (contains prefixed demo IDs: date_time_UUID)
   - `quad_matchids_[month].txt` (contains prefixed demo IDs: date_time_UUID)

2. For the ace_matchids and quad_matchids files:
   - Creates temporary UUID-only files with the prefixes removed
   - Uses set operations to efficiently find UUIDs that are not in downloaded, parsed, or rejected sets
   - Adds the UUIDs (without prefixes) to the queue for consistency

3. The system then:
   - Adds unprocessed demos to the parse queue
   - Alphabetizes and removes duplicates from the queue
   - Logs summary information rather than details for each matchid

### During Processing

- The system processes each demo in the queue
- Detailed logs are written to the debug log file but not shown in the console
- Only important information and summaries are shown in the console

## Benefits

1. **Reduced Console Spam**: Only important information is shown in the console
2. **Improved Performance**: Using set operations on UUID-only files is faster than extracting UUIDs on the fly
3. **Better Debugging**: Detailed logs are still available in the debug log file for troubleshooting
4. **Cleaner Code**: The code is now more organized and easier to maintain
5. **Consistent Format**: All entries in the parse queue now use the same format (UUIDs without prefixes)

## Debug Logs

Detailed debug logs are written to the `logs/debug.log` file. This file contains all the verbose logging that was previously shown in the console.
