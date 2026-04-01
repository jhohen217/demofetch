#!/usr/bin/env python3
"""
reorganize_textfiles.py

Scans month folders inside the textfiles directory and reorganizes entries
by year based on the date prefix in each entry (MM-DD-YY format).

Behavior:
  - Entries with date 00-00-00 are considered undated and are DELETED.
  - Valid entries are grouped by 2-digit year (25, 26, etc.).
  - Year-specific subfolders are created: e.g. March25/, March26/
  - .txt files are written with year-suffixed names: ace_matchids_march25.txt
  - JSON files are COPIED (as-is) into every year folder found for that month.
  - The original unsuffixed month folder is DELETED after successful processing.
  - The 'undated/' folder is left completely alone.

Usage:
    python reorganize_textfiles.py [textfiles_dir] [--dry-run]

    textfiles_dir  Path to the textfiles folder (default: auto-detected relative
                   to this script's location).
    --dry-run      Preview changes without writing or deleting anything.
"""

import os
import re
import json
import shutil
import argparse
import sys
from collections import defaultdict
from pathlib import Path

# Canonical month names (case-insensitive matching used when scanning folders)
MONTHS = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
]

MONTH_LOWER = {m.lower(): m for m in MONTHS}

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def is_undated(line: str) -> bool:
    """Return True if the line is an undated entry (00-00-00 prefix)."""
    return line.strip().startswith('00-00-00')


def extract_year_from_ace_entry(line: str):
    """
    Extract 2-digit year from MM-DD-YY_HHMM_... format.
    Returns the year string (e.g. '25') or None.
    """
    m = re.match(r'\d{2}-\d{2}-(\d{2})_', line.strip())
    return m.group(1) if m else None


def extract_year_from_match_ids_entry(line: str):
    """
    Extract 2-digit year from  1-uuid,YYYY-MM-DDTHH:MM:SS.mssZ  format.
    Returns the year string (e.g. '26') or None.
    """
    m = re.search(r',(\d{4})-\d{2}-\d{2}T', line)
    if m:
        return m.group(1)[2:]   # '2026' -> '26'
    return None


def is_match_ids_file(filename: str) -> bool:
    """
    Return True if this file uses the  1-uuid,timestamp  format.
    Distinguish from ace_matchids, quad_matchids, unapproved_matchids, etc.
    """
    base = filename.lower()
    # Files that start with a plain 'match_ids' prefix (not ace_, quad_, etc.)
    return re.match(r'^match_ids', base) is not None


# ──────────────────────────────────────────────────────────────────────────────
# Core processing
# ──────────────────────────────────────────────────────────────────────────────

def process_txt_file(filepath: Path, dry_run: bool) -> dict:
    """
    Read a .txt file, classify each line by year, and return a dict:
        { year_str: [line, line, ...], ... }
    Lines that are undated (00-00-00) are silently dropped.
    """
    filename = filepath.name
    use_match_ids_parser = is_match_ids_file(filename)

    year_entries: dict = defaultdict(list)
    deleted_count = 0
    unknown_count = 0

    with open(filepath, 'r', encoding='utf-8', errors='replace') as fh:
        for raw_line in fh:
            line = raw_line.rstrip('\n')
            if not line.strip():
                continue

            if use_match_ids_parser:
                year = extract_year_from_match_ids_entry(line)
            else:
                if is_undated(line):
                    deleted_count += 1
                    continue
                year = extract_year_from_ace_entry(line)

            if year:
                year_entries[year].append(line)
            else:
                unknown_count += 1
                # Lines with no parseable year are logged but kept under a
                # special 'unknown' bucket so nothing is silently lost.
                year_entries['unknown'].append(line)

    if deleted_count:
        print(f"    [deleted {deleted_count} undated lines] in {filename}")
    if unknown_count:
        print(f"    [WARNING: {unknown_count} lines had no parseable year] in {filename}")

    return dict(year_entries)


def sort_key_for_entry(line: str) -> tuple:
    """
    Return a sort key tuple for chronological ordering.
    Handles MM-DD-YY_HHMM_... format -> (YY, MM, DD, HHMM).
    Falls back to the raw line so unknown formats sort consistently.
    """
    m = re.match(r'(\d{2})-(\d{2})-(\d{2})_(\d{4})', line.strip())
    if m:
        mm, dd, yy, hhmm = m.group(1), m.group(2), m.group(3), m.group(4)
        return (yy, mm, dd, hhmm)
    # match_ids format: 1-uuid,YYYY-MM-DDTHH:MM:SS
    m2 = re.search(r',(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})', line)
    if m2:
        yyyy, mo, day, hh, mi = m2.groups()
        return (yyyy[2:], mo, day, hh + mi)
    return (line,)


def write_year_files(
    year_entries: dict,
    filename: str,
    month_canonical: str,
    textfiles_dir: Path,
    dry_run: bool,
) -> set:
    """
    For each year found in year_entries, create  <Month><YY>/  and write
    <filename_without_ext><YY>.txt   (e.g. ace_matchids_march25.txt).
    Entries are sorted chronologically before writing.

    Returns the set of year strings that were written.
    """
    years_written = set()
    base, ext = os.path.splitext(filename)

    for year, entries in year_entries.items():
        if not entries:
            continue

        # Sort entries chronologically
        sorted_entries = sorted(entries, key=sort_key_for_entry)

        year_folder_name = f"{month_canonical}{year}"
        year_folder_path = textfiles_dir / year_folder_name
        new_filename = f"{base}{year}{ext}"
        output_path = year_folder_path / new_filename

        print(f"    -> {year_folder_name}/{new_filename}  ({len(sorted_entries)} entries)")

        if not dry_run:
            year_folder_path.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as fh:
                fh.write('\n'.join(sorted_entries) + '\n')

        years_written.add(year)

    return years_written


def copy_json_to_year_folders(
    json_file: Path,
    month_canonical: str,
    years: set,
    textfiles_dir: Path,
    dry_run: bool,
):
    """
    Copy a JSON file into every year folder that was produced for this month.
    The copied file gets a year suffix too:
        hub_output_march.json  ->  March25/hub_output_march25.json
    """
    base, ext = os.path.splitext(json_file.name)
    for year in sorted(years):
        year_folder_name = f"{month_canonical}{year}"
        year_folder_path = textfiles_dir / year_folder_name
        new_filename = f"{base}{year}{ext}"
        dest = year_folder_path / new_filename

        print(f"    [JSON copy] {year_folder_name}/{new_filename}")

        if not dry_run:
            year_folder_path.mkdir(parents=True, exist_ok=True)
            shutil.copy2(json_file, dest)


def process_month_folder(
    month_folder: Path,
    textfiles_dir: Path,
    dry_run: bool,
):
    """
    Process all files in a single month folder (e.g. March/).
    """
    month_canonical = MONTH_LOWER.get(month_folder.name.lower(), month_folder.name)
    print(f"\n{'='*60}")
    print(f"Processing: {month_folder.name}  ->  canonical: {month_canonical}")
    print(f"{'='*60}")

    all_years_seen: set = set()
    json_files: list = []

    # ── Pass 1: process .txt files ──────────────────────────────────────────
    for item in sorted(month_folder.iterdir()):
        if not item.is_file():
            continue

        if item.suffix.lower() == '.json':
            json_files.append(item)
            continue

        if item.suffix.lower() != '.txt':
            print(f"  [SKIP non-txt/json] {item.name}")
            continue

        print(f"  Processing txt: {item.name}")
        year_entries = process_txt_file(item, dry_run)
        years_written = write_year_files(
            year_entries, item.name, month_canonical, textfiles_dir, dry_run
        )
        all_years_seen |= years_written

    # ── Pass 2: copy JSON files into every year folder found ────────────────
    for jf in json_files:
        if not all_years_seen:
            print(f"  [JSON skip – no year folders created] {jf.name}")
            continue
        print(f"  Processing json: {jf.name}")
        copy_json_to_year_folders(jf, month_canonical, all_years_seen, textfiles_dir, dry_run)

    # ── Pass 3: delete the original unsuffixed folder ───────────────────────
    if all_years_seen:
        print(f"\n  [DELETE original folder] {month_folder}")
        if not dry_run:
            shutil.rmtree(month_folder)
    else:
        print(f"\n  [WARNING] No entries with a parseable year were found.")
        print(f"  Original folder '{month_folder.name}' was NOT deleted.")


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

def find_default_textfiles_dir() -> Path:
    """Walk up from script location to find the textfiles directory."""
    script_dir = Path(__file__).resolve().parent
    # Script is in DiscordBot/scripts/, textfiles is in DiscordBot/textfiles/
    candidate = script_dir.parent / 'textfiles'
    if candidate.is_dir():
        return candidate
    # Fallback: current working directory / DiscordBot / textfiles
    candidate2 = Path.cwd() / 'DiscordBot' / 'textfiles'
    if candidate2.is_dir():
        return candidate2
    return candidate  # return best guess even if it doesn't exist


def main():
    parser = argparse.ArgumentParser(
        description='Reorganize textfiles by year based on entry date prefixes.'
    )
    parser.add_argument(
        'textfiles_dir',
        nargs='?',
        default=None,
        help='Path to the textfiles directory (default: auto-detected)',
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Preview changes without writing, copying, or deleting anything.',
    )
    args = parser.parse_args()

    if args.textfiles_dir:
        textfiles_dir = Path(args.textfiles_dir).resolve()
    else:
        textfiles_dir = find_default_textfiles_dir()

    if not textfiles_dir.is_dir():
        print(f"ERROR: textfiles directory not found: {textfiles_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Textfiles directory : {textfiles_dir}")
    print(f"Dry run             : {args.dry_run}")
    print()

    # Find all unsuffixed month folders (e.g. March/, April/)
    # Skip folders that already have a year suffix (March25, March26, undated, etc.)
    month_folders = []
    for item in sorted(textfiles_dir.iterdir()):
        if not item.is_dir():
            continue
        name_lower = item.name.lower()
        # Skip the 'undated' folder and year-suffixed folders (end with digits)
        if name_lower == 'undated':
            print(f"[SKIP] {item.name}  (undated folder – left alone)")
            continue
        if re.search(r'\d+$', item.name):
            print(f"[SKIP] {item.name}  (already has year suffix)")
            continue
        if name_lower in MONTH_LOWER:
            month_folders.append(item)
        else:
            print(f"[SKIP] {item.name}  (not a recognised month folder)")

    if not month_folders:
        print("\nNo unsuffixed month folders found. Nothing to do.")
        return

    print(f"\nFound {len(month_folders)} month folder(s) to process:")
    for mf in month_folders:
        print(f"  {mf.name}")

    for month_folder in month_folders:
        process_month_folder(month_folder, textfiles_dir, args.dry_run)

    print('\n' + '='*60)
    if args.dry_run:
        print("DRY RUN complete – no files were written, copied, or deleted.")
    else:
        print("Reorganization complete.")
    print('='*60)


if __name__ == '__main__':
    main()
