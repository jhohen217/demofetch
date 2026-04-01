#!/usr/bin/env python3
"""
fix_month_mismatches.py

Second-pass script for already year-split textfiles folders.

Each folder is named  <Month><YY>  (e.g. January26, March25).
Each entry inside a .txt file starts with  MM-DD-YY_  where MM is the
1-based month number.

This script:
  1. Reads every .txt file in every <Month><YY> folder.
  2. Checks whether the MM in each entry actually belongs to that folder's month.
  3. Moves misplaced entries to the correct  <CorrectMonth><YY>/  folder,
     appending to (or creating) the matching .txt file there.
  4. Rewrites the source file with only the entries that belong to it.
  5. Leaves JSON files and the 'undated' folder untouched.
  6. Entries that are 00-00-00 are deleted.
  7. Entries where the month number cannot be parsed are left in place with a warning.

Usage:
    python fix_month_mismatches.py [textfiles_dir] [--dry-run]
"""

import os
import re
import sys
import shutil
import argparse
from collections import defaultdict
from pathlib import Path

MONTHS = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
]

# month number (1-based) -> canonical name
MONTH_NUM_TO_NAME = {str(i + 1).zfill(2): MONTHS[i] for i in range(12)}
MONTH_NUM_TO_NAME.update({str(i + 1): MONTHS[i] for i in range(12)})

# canonical name (lower) -> month number string '01'..'12'
MONTH_NAME_TO_NUM = {m.lower(): str(i + 1).zfill(2) for i, m in enumerate(MONTHS)}

# ──────────────────────────────────────────────────────────────────────────────

def parse_folder_name(folder_name: str):
    """
    Parse a folder like 'January26' into ('January', '26').
    Returns (month_canonical, year_str) or (None, None) if not recognised.
    """
    m = re.match(r'^([A-Za-z]+)(\d+)$', folder_name)
    if not m:
        return None, None
    name_part = m.group(1)
    year_part = m.group(2)
    # normalise to canonical
    canonical = next((mn for mn in MONTHS if mn.lower() == name_part.lower()), None)
    return canonical, year_part


def entry_month_num(line: str):
    """
    Extract the 2-digit month from MM-DD-YY_ prefix.
    Returns '01'..'12' or None.
    """
    m = re.match(r'^(\d{2})-\d{2}-\d{2}_', line.strip())
    return m.group(1) if m else None


def is_undated(line: str) -> bool:
    return line.strip().startswith('00-00-00')


def derive_destination_filename(source_filename: str, dest_month: str, source_month: str, year: str) -> str:
    """
    Given e.g. 'ace_matchids_january26.txt' and dest_month='February', source_month='January',
    return 'ace_matchids_february26.txt'.
    """
    base, ext = os.path.splitext(source_filename)
    # strip trailing year digits if present
    base_no_year = re.sub(r'\d+$', '', base)
    # replace the source month (case-insensitive) in the base name with dest month (lower)
    new_base = re.sub(
        re.escape(source_month), dest_month.lower(),
        base_no_year, flags=re.IGNORECASE
    )
    return f"{new_base}{year}{ext}"


# ──────────────────────────────────────────────────────────────────────────────

def process_all(textfiles_dir: Path, dry_run: bool):
    # Collect all <Month><YY> folders
    year_folders = []
    for item in sorted(textfiles_dir.iterdir()):
        if not item.is_dir():
            continue
        if item.name.lower() == 'undated':
            print(f"[SKIP] {item.name}  (undated folder)")
            continue
        canonical, year = parse_folder_name(item.name)
        if canonical is None:
            print(f"[SKIP] {item.name}  (not a recognised Month+Year folder)")
            continue
        year_folders.append((item, canonical, year))

    if not year_folders:
        print("No Month+Year folders found.")
        return

    print(f"Found {len(year_folders)} folder(s) to scan.\n")

    total_moved = 0
    total_deleted = 0

    for folder_path, folder_month, folder_year in year_folders:
        folder_month_num = MONTH_NAME_TO_NUM[folder_month.lower()]

        for txt_file in sorted(folder_path.glob('*.txt')):
            keep_lines = []
            # misplaced[dest_folder_name] = [lines...]
            misplaced: dict = defaultdict(list)
            deleted = 0

            with open(txt_file, 'r', encoding='utf-8', errors='replace') as fh:
                lines = [l.rstrip('\n') for l in fh if l.strip()]

            for line in lines:
                if is_undated(line):
                    deleted += 1
                    continue

                mm = entry_month_num(line)
                if mm is None:
                    # Can't parse month – leave in place
                    keep_lines.append(line)
                    continue

                if mm == folder_month_num:
                    keep_lines.append(line)
                else:
                    # Wrong month – find correct destination
                    dest_month_name = MONTH_NUM_TO_NAME.get(mm)
                    if dest_month_name is None:
                        keep_lines.append(line)
                        continue
                    dest_folder_name = f"{dest_month_name}{folder_year}"
                    misplaced[dest_folder_name].append(line)

            moved_count = sum(len(v) for v in misplaced.values())
            if moved_count == 0 and deleted == 0:
                continue  # nothing to do for this file

            print(f"\n  {folder_path.name}/{txt_file.name}")
            if deleted:
                print(f"    [DELETE] {deleted} undated lines")
            for dest_folder_name, dest_lines in sorted(misplaced.items()):
                dest_month_name = re.match(r'^([A-Za-z]+)', dest_folder_name).group(1)
                dest_filename = derive_destination_filename(
                    txt_file.name, dest_month_name, folder_month, folder_year
                )
                dest_folder_path = textfiles_dir / dest_folder_name
                dest_file_path = dest_folder_path / dest_filename
                print(f"    -> MOVE {len(dest_lines)} entries to {dest_folder_name}/{dest_filename}")

                if not dry_run:
                    dest_folder_path.mkdir(parents=True, exist_ok=True)
                    # Append to existing file or create new
                    mode = 'a' if dest_file_path.exists() else 'w'
                    with open(dest_file_path, mode, encoding='utf-8') as fh:
                        fh.write('\n'.join(dest_lines) + '\n')

            total_moved += moved_count
            total_deleted += deleted

            # Rewrite source file with only the correct lines
            if not dry_run:
                if keep_lines:
                    with open(txt_file, 'w', encoding='utf-8') as fh:
                        fh.write('\n'.join(keep_lines) + '\n')
                else:
                    print(f"    [EMPTY after move – deleting source file] {txt_file.name}")
                    txt_file.unlink()

    print(f"\n{'='*60}")
    if dry_run:
        print(f"DRY RUN complete. Would move {total_moved} entries, delete {total_deleted} undated.")
    else:
        print(f"Done. Moved {total_moved} entries, deleted {total_deleted} undated entries.")
    print('='*60)


# ──────────────────────────────────────────────────────────────────────────────

def find_default_textfiles_dir() -> Path:
    script_dir = Path(__file__).resolve().parent
    candidate = script_dir.parent / 'textfiles'
    if candidate.is_dir():
        return candidate
    candidate2 = Path.cwd() / 'DiscordBot' / 'textfiles'
    if candidate2.is_dir():
        return candidate2
    return candidate


def main():
    parser = argparse.ArgumentParser(
        description='Fix month mismatches in already year-split textfiles folders.'
    )
    parser.add_argument('textfiles_dir', nargs='?', default=None)
    parser.add_argument('--dry-run', action='store_true',
                        help='Preview changes without writing anything.')
    args = parser.parse_args()

    textfiles_dir = Path(args.textfiles_dir).resolve() if args.textfiles_dir else find_default_textfiles_dir()

    if not textfiles_dir.is_dir():
        print(f"ERROR: {textfiles_dir} not found.", file=sys.stderr)
        sys.exit(1)

    print(f"Textfiles directory : {textfiles_dir}")
    print(f"Dry run             : {args.dry_run}\n")

    process_all(textfiles_dir, args.dry_run)


if __name__ == '__main__':
    main()
