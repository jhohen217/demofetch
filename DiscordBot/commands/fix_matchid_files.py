"""
Script to fix matchid files by ensuring they have the correct format:
- matchid.txt files (ace_matchids, quad_matchids) should keep prefixes and be alphabetized
- other files (downloaded, parsed, parse_queue) should have prefixes removed
"""

import os
import asyncio
import sys
import json
from typing import List, Set, Dict, Tuple

# Add the project root to the Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(script_dir)
sys.path.append(project_dir)

from commands.parser.utils import alphabetize_file, has_prefix, extract_uuid_from_demo_id

async def read_file_lines(file_path: str) -> List[str]:
    """Read lines from a file into a list"""
    lines = []
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f if line.strip()]
    return lines

async def write_file_lines(file_path: str, lines: List[str]):
    """Write lines to a file"""
    with open(file_path, 'w', encoding='utf-8') as f:
        for line in lines:
            f.write(f"{line}\n")

async def fix_matchid_file(file_path: str, keep_prefix: bool, alphabetize: bool, preserve_chronological: bool = True) -> Tuple[int, int]:
    """
    Fix a matchid file by ensuring it has the correct format.
    
    Args:
        file_path: Path to the matchid file to fix
        keep_prefix: Whether to keep the prefix or remove it
        alphabetize: Whether to alphabetize the file
        preserve_chronological: Whether to preserve chronological order for prefixed entries
        
    Returns:
        Tuple[int, int]: (Number of entries fixed, Number of entries processed)
    """
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return (0, 0)
        
    print(f"Fixing file: {file_path}")
    
    # Read the file
    lines = await read_file_lines(file_path)
    if not lines:
        print(f"File is empty: {file_path}")
        return (0, 0)
    
    # Process each line
    fixed_lines = []
    fixed_count = 0
    
    for line in lines:
        original_line = line
        
        if not keep_prefix:
            # Remove prefix if present
            if has_prefix(line):
                line = extract_uuid_from_demo_id(line)
                fixed_count += 1
        
        fixed_lines.append(line)
    
    # Alphabetize if requested
    if alphabetize:
        if preserve_chronological and os.path.basename(file_path).startswith(('ace_matchids', 'quad_matchids')):
            # For matchid files, we want to sort by date/time prefix if present
            # This ensures older demos are processed first
            def sort_key(line):
                if has_prefix(line):
                    # Extract the date/time part for sorting
                    parts = line.split('_')
                    if len(parts) >= 2:
                        # Return the date/time parts for sorting
                        return parts[0] + '_' + parts[1]
                # For lines without prefix, use the UUID
                return extract_uuid_from_demo_id(line)
            
            fixed_lines.sort(key=sort_key)
            print(f"Chronologically sorted file: {file_path}")
        else:
            # Standard alphabetical sort
            fixed_lines.sort()
            print(f"Alphabetically sorted file: {file_path}")
    
    # Write the fixed lines back to the file
    await write_file_lines(file_path, fixed_lines)
    
    print(f"Successfully fixed file: {file_path} (Fixed {fixed_count} entries)")
    return (fixed_count, len(lines))

async def fix_all_matchid_files(textfiles_dir: str, month: str = None, preserve_chronological: bool = True) -> Dict:
    """
    Fix all matchid files for a specific month or all months.
    
    Args:
        textfiles_dir: Directory containing month folders
        month: Month name (e.g., "February") or None for all months
        preserve_chronological: Whether to preserve chronological order for prefixed entries
        
    Returns:
        dict: Results of fixing operation
    """
    results = {
        'success': 0,
        'failed': 0,
        'skipped': 0,
        'fixed_entries': 0,
        'total_entries': 0
    }
    
    try:
        if month:
            # Fix files for specific month
            month_dir = os.path.join(textfiles_dir, month)
            month_lower = month.lower()
            
            # Files that should keep prefixes and be alphabetized
            matchid_files = [
                (os.path.join(month_dir, f'ace_matchids_{month_lower}.txt'), True, True, preserve_chronological),
                (os.path.join(month_dir, f'quad_matchids_{month_lower}.txt'), True, True, preserve_chronological)
            ]
            
            # Files that should have prefixes removed and not be alphabetized
            other_files = [
                (os.path.join(month_dir, f'parsed_{month_lower}.txt'), False, False, False),
                (os.path.join(month_dir, f'downloaded_{month_lower}.txt'), False, False, False),
                (os.path.join(month_dir, f'parse_queue_{month_lower}.txt'), False, True, preserve_chronological)
            ]
            
            # Process all files
            for file_info in matchid_files + other_files:
                file_path = file_info[0]
                keep_prefix = file_info[1]
                alphabetize = file_info[2]
                preserve_chrono = file_info[3] if len(file_info) > 3 else preserve_chronological
                
                if os.path.exists(file_path):
                    try:
                        fixed_count, total_count = await fix_matchid_file(file_path, keep_prefix, alphabetize, preserve_chrono)
                        results['success'] += 1
                        results['fixed_entries'] += fixed_count
                        results['total_entries'] += total_count
                    except Exception as e:
                        print(f"Error fixing file {file_path}: {str(e)}")
                        results['failed'] += 1
                else:
                    results['skipped'] += 1
                    print(f"Skipped non-existent file: {file_path}")
        else:
            # Fix files for all months
            month_dirs = [d for d in os.listdir(textfiles_dir) 
                         if os.path.isdir(os.path.join(textfiles_dir, d)) 
                         and d not in ['$RECYCLE.BIN', 'System Volume Information', 'MergeMe', 'undated']]
            
            for month_dir in month_dirs:
                month_path = os.path.join(textfiles_dir, month_dir)
                month_lower = month_dir.lower()
                
                # Files that should keep prefixes and be alphabetized
                matchid_files = [
                    (os.path.join(month_path, f'ace_matchids_{month_lower}.txt'), True, True, preserve_chronological),
                    (os.path.join(month_path, f'quad_matchids_{month_lower}.txt'), True, True, preserve_chronological)
                ]
                
                # Files that should have prefixes removed and not be alphabetized
                other_files = [
                    (os.path.join(month_path, f'parsed_{month_lower}.txt'), False, False, False),
                    (os.path.join(month_path, f'downloaded_{month_lower}.txt'), False, False, False),
                    (os.path.join(month_path, f'parse_queue_{month_lower}.txt'), False, True, preserve_chronological)
                ]
                
                # Process all files
                for file_info in matchid_files + other_files:
                    file_path = file_info[0]
                    keep_prefix = file_info[1]
                    alphabetize = file_info[2]
                    preserve_chrono = file_info[3] if len(file_info) > 3 else preserve_chronological
                    
                    if os.path.exists(file_path):
                        try:
                            fixed_count, total_count = await fix_matchid_file(file_path, keep_prefix, alphabetize, preserve_chrono)
                            results['success'] += 1
                            results['fixed_entries'] += fixed_count
                            results['total_entries'] += total_count
                        except Exception as e:
                            print(f"Error fixing file {file_path}: {str(e)}")
                            results['failed'] += 1
                    else:
                        results['skipped'] += 1
    except Exception as e:
        print(f"Error fixing matchid files: {str(e)}")
        results['failed'] += 1
        
    return results

async def main():
    # Load configuration from project root
    config_path = os.path.join(os.path.dirname(project_dir), 'config.json')
    
    with open(config_path, 'r') as f:
        config = json.load(f)
        
    textfiles_dir = config['project']['textfiles_directory']
    
    # Get month from command line argument if provided
    month = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Default to preserving chronological order
    preserve_chronological = True
    
    # Check if preserve_chronological flag is provided
    if len(sys.argv) > 2:
        preserve_chronological = sys.argv[2].lower() in ('true', 'yes', '1')
    
    # Fix matchid files
    results = await fix_all_matchid_files(textfiles_dir, month, preserve_chronological)
    
    print(f"Fixing complete: {results['success']} files successful, {results['failed']} failed, {results['skipped']} skipped")
    print(f"Fixed {results['fixed_entries']} entries out of {results['total_entries']} total entries")

if __name__ == "__main__":
    asyncio.run(main())
