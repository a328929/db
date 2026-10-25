"""Configuration constants for channel inventory scanning.

This module centralizes all tunable parameters for the inventory scanning system,
making it easier to adjust behavior without modifying core logic.
"""

from __future__ import annotations

# Public entity batch processing
PUBLIC_ENTITY_BATCH_SIZE = 50
"""Number of public entities to resolve in a single batch API call.

Higher values reduce network overhead but may increase the risk of hitting
rate limits. Telegram's API generally handles batches of 50-100 well.
"""

# Public entity resolution limits
DEFAULT_PUBLIC_RESOLVE_LIMIT = 40
"""Default maximum number of uncached public entities to resolve per scan.

Candidates rotate by persisted attempt time, including failed attempts.
Unprobed account observations remain visible as historical results.
Zero disables username resolution; cached ID probes remain enabled.
"""

DEFAULT_PUBLIC_RESOLVE_GAP_SECONDS = 1.0
"""Default delay between consecutive public entity resolution attempts.

This gap reduces the likelihood of triggering Telegram's flood control.
Accounts with higher rate limits can reduce this value.
"""

# Default config values
DEFAULT_FLOOD_WAIT_THRESHOLD = 30
"""Default FloodWait threshold in seconds before switching accounts.

If a FloodWait error requires waiting more than this many seconds,
the scanner will switch to the next available account instead of blocking.
"""
