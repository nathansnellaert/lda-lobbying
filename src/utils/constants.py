"""Shared constants for LDA lobbying connector.

Centralizes configuration that's used across ingest and transform modules.
"""

# Senate LDA API base URL
API_BASE = "https://lda.senate.gov/api/v1"

# Rate limit: 15 requests/minute for unauthenticated = 4 seconds between requests
RATE_LIMIT_DELAY = 4.5

# Years to fetch - LDA filings available from 1999, LD-203 contributions from 2008
FILING_YEARS = list(range(2024, 1998, -1))  # 2024 down to 1999
CONTRIBUTION_YEARS = list(range(2024, 2007, -1))  # 2024 down to 2008

# For backwards compatibility with transforms that use YEARS
YEARS = FILING_YEARS
