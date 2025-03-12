import os
import glob
from datetime import datetime

def get_month_files(textfiles_dir, month):
    """Get file paths for a specific month"""
    month_dir = os.path.join(textfiles_dir, month)
    month_lower = month.lower()
    return {
        'dir': month_dir,
        'match_ids': os.path.join(month_dir, f'match_ids_{month_lower}.txt'),
        'downloaded': os.path.join(month_dir, f'downloaded_{month_lower}.txt'),
        'rejected': os.path.join(month_dir, f'rejected_{month_lower}.txt'),
        'download_queue': os.path.join(month_dir, f'download_queue_{month_lower}.txt'),
        'ace': os.path.join(month_dir, f'ace_matchids_{month_lower}.txt'),
        'quad': os.path.join(month_dir, f'quad_matchids_{month_lower}.txt'),
        'unapproved': os.path.join(month_dir, f'unapproved_matchids_{month_lower}.txt')
    }

def sort_matchid_file(filepath, preserve_chronological=True):
    """
    Sort a matchid file in chronological order.
    
    Args:
        filepath: Path to the matchid file to sort
        preserve_chronological: Whether to preserve chronological order for prefixed entries
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return False
        
    try:
        # Read file contents
        with open(filepath, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f if line.strip()]
            
        if not lines:
            print(f"File is empty: {filepath}")
            return True
        
        # Import the necessary functions from utils
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from commands.parser.utils import has_prefix, extract_uuid_from_demo_id
        
        # Sort lines based on the preserve_chronological flag
        if preserve_chronological and os.path.basename(filepath).startswith(('ace_matchids', 'quad_matchids')):
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
            
            lines.sort(key=sort_key)
            print(f"Chronologically sorted file: {filepath}")
        else:
            # Standard alphabetical sort
            lines.sort()
            print(f"Alphabetically sorted file: {filepath}")
        
        # Write sorted lines back to file
        with open(filepath, 'w', encoding='utf-8') as f:
            for line in lines:
                f.write(line + '\n')
                
        print(f"Successfully sorted file: {filepath}")
        return True
    except Exception as e:
        print(f"Error sorting file {filepath}: {str(e)}")
        return False

def sort_all_matchid_files(textfiles_dir, month=None, preserve_chronological=True):
    """
    Sort all matchid files for a specific month or all months.
    
    Args:
        textfiles_dir: Directory containing month folders
        month: Month name (e.g., "February") or None for all months
        preserve_chronological: Whether to preserve chronological order for prefixed entries
        
    Returns:
        dict: Results of sorting operation
    """
    results = {
        'success': 0,
        'failed': 0,
        'skipped': 0
    }
    
    try:
        if month:
            # Sort files for specific month
            files = get_month_files(textfiles_dir, month)
            for category in ['ace', 'quad']:
                filepath = files[category]
                if os.path.exists(filepath):
                    if sort_matchid_file(filepath, preserve_chronological):
                        results['success'] += 1
                    else:
                        results['failed'] += 1
                else:
                    results['skipped'] += 1
                    print(f"Skipped non-existent file: {filepath}")
        else:
            # Sort files for all months
            month_dirs = [d for d in os.listdir(textfiles_dir) 
                         if os.path.isdir(os.path.join(textfiles_dir, d)) 
                         and d not in ['$RECYCLE.BIN', 'System Volume Information', 'MergeMe', 'undated']]
            
            for month_dir in month_dirs:
                month_path = os.path.join(textfiles_dir, month_dir)
                
                # Find all ace and quad matchid files
                ace_files = glob.glob(os.path.join(month_path, 'ace_matchids_*.txt'))
                quad_files = glob.glob(os.path.join(month_path, 'quad_matchids_*.txt'))
                
                for filepath in ace_files + quad_files:
                    if sort_matchid_file(filepath, preserve_chronological):
                        results['success'] += 1
                    else:
                        results['failed'] += 1
    except Exception as e:
        print(f"Error sorting matchid files: {str(e)}")
        results['failed'] += 1
        
    return results

if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) > 1:
        textfiles_dir = sys.argv[1]
        month = sys.argv[2] if len(sys.argv) > 2 else None
        preserve_chronological = True  # Default to preserving chronological order
        
        # Check if preserve_chronological flag is provided
        if len(sys.argv) > 3:
            preserve_chronological = sys.argv[3].lower() in ('true', 'yes', '1')
            
        results = sort_all_matchid_files(textfiles_dir, month, preserve_chronological)
        print(f"Sorting complete: {results['success']} successful, {results['failed']} failed, {results['skipped']} skipped")
    else:
        print("Usage: python sort_matchids.py <textfiles_dir> [month] [preserve_chronological]")
