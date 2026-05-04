# utils.py
# Utility functions shared across collector modules.
# Functions here are stateless — no database connection or instance required.
#
# calculate_sample_size — percentage based sampling bounded by min and max
# get_file_hash         — MD5 hash of a file's contents
# has_file_changed      — compares current hash against stored hash
# save_file_hash        — writes current hash to disk after successful parse

import hashlib
import os


def calculate_sample_size(
    row_count: int,
    sample_percentage: float = 1.0,
    min_sample_rows: int = 20,
    max_sample_rows: int = 1000
) -> int:
    """
    Calculate sample size as a percentage of table rows.
    Bounded by min and max to handle very small and very large tables.

    Examples:
        10 rows      → 20   (floor applied)
        500 rows     → 20   (floor applied)
        2000 rows    → 20   (exact 1%)
        50000 rows   → 500  (within bounds)
        1000000 rows → 1000 (ceiling applied)
    """
    # empty table — nothing to sample
    if row_count == 0:
        return 0

    # calculate percentage based sample
    calculated = int(row_count * (sample_percentage / 100))

    # apply floor and ceiling
    return max(min_sample_rows, min(calculated, max_sample_rows))


def get_file_hash(path: str) -> str:
    # returns MD5 hash of file contents
    # used by has_file_changed and save_file_hash
    with open(path, 'rb') as f:
        return hashlib.md5(f.read()).hexdigest()


def has_file_changed(path: str, hash_store_path: str) -> bool:
    # True if file has changed since last parse or has never been parsed
    # compares current MD5 hash against stored hash from last parse
    # returns True if hash file does not exist — file never been parsed
    current_hash = get_file_hash(path)
    try:
        with open(hash_store_path, 'r') as f:
            stored_hash = f.read().strip()
        return current_hash != stored_hash
    except FileNotFoundError:
        # no stored hash — treat as changed
        return True


def save_file_hash(path: str, hash_store_path: str) -> None:
    # saves current file hash to disk after successful parse
    # creates hash directory if it does not exist
    os.makedirs(os.path.dirname(hash_store_path), exist_ok=True)
    with open(hash_store_path, 'w') as f:
        f.write(get_file_hash(path))