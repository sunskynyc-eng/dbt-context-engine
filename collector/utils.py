# utils.py
# Utility functions shared across collector modules.
# Functions here are stateless — no database connection or instance required.


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
        10 rows      → 20  (floor applied)
        500 rows     → 20  (floor applied)
        2000 rows    → 20  (exact 1%)
        50000 rows   → 500 (within bounds)
        1000000 rows → 1000 (ceiling applied)
    """
    # empty table — nothing to sample
    if row_count == 0:
        return 0

    # calculate percentage based sample
    calculated = int(row_count * (sample_percentage / 100))

    # apply floor and ceiling
    return max(min_sample_rows, min(calculated, max_sample_rows))