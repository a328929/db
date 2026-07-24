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

This limit prevents excessive API calls during a single scan. Public channels
not resolved in this scan will retain their previous scan results until a
future scan cycle.

WHY THIS LIMIT EXISTS:
- Resolving thousands of entities would take hours (1+ sec per entity with rate limiting)
- High risk of hitting Telegram flood control limits
- Potential account penalties for aggressive API usage
- Most channels don't change restriction status frequently

IMPLICATIONS:
- If the database has 1000+ public channels, most won't be checked each scan
- Risk flags for unresolved channels may become stale
- Access failures may not be detected until the channel is eventually resolved

CONFIGURATION GUIDANCE:
- Set to 0 to skip active resolution entirely (only use cached entities)
- Set to a high value (e.g., 10000) for exhaustive scans, but expect:
  * Long scan duration (potentially hours)
  * Increased risk of FloodWait errors
  * Higher API usage on your Telegram account
- Default (40) provides a balance: some coverage without excessive API calls

FUTURE IMPROVEMENTS:
- Implement rotation: resolve different subsets each scan
- Prioritize by activity: message_count DESC, last_message_ts DESC
- Track last_resolved_at per channel for guaranteed periodic coverage
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
