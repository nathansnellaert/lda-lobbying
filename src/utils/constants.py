"""Shared constants for LDA lobbying connector.

Centralizes configuration that's used across ingest and transform modules.
"""

# Senate LDA API base URL
API_BASE = "https://lda.senate.gov/api/v1"

# Rate limit: 15 requests/minute for unauthenticated = 4 seconds between requests
RATE_LIMIT_DELAY = 4.5

# Years to fetch - LDA data available from 1999, LD-203 required since 2008
# This list is shared between ingest and transform modules
YEARS = [2024, 2023, 2022, 2021, 2020]
