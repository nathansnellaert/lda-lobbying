"""Fetch LDA filings (LD-1 registrations and LD-2 quarterly activity reports).

Uses the Senate LDA API to fetch filing data. Due to rate limits (15 req/min
for unauthenticated), we fetch a sample for each year.

API: https://lda.senate.gov/api/v1/filings/
"""

import time
from subsets_utils import get, save_raw_json, load_state, save_state

API_BASE = "https://lda.senate.gov/api/v1"

# Years to fetch - LDA data available from 1999
YEARS = [2024, 2023, 2022, 2021, 2020]

# Rate limit: 15 requests/minute for unauthenticated = 4 seconds between requests
RATE_LIMIT_DELAY = 4.5

# Pages per year to fetch (25 filings per page)
# 50 pages = 1250 filings per year, a good sample
MAX_PAGES_PER_YEAR = 50


def fetch_filings_for_year(year: int, max_pages: int) -> list[dict]:
    """Fetch filings for a given year with pagination."""
    all_filings = []
    url = f"{API_BASE}/filings/"
    params = {"filing_year": year}
    page = 1

    while url and page <= max_pages:
        print(f"    Page {page}/{max_pages}...")
        response = get(url, params=params if page == 1 else None)
        data = response.json()

        results = data.get("results", [])
        all_filings.extend(results)

        url = data.get("next")
        page += 1

        if url and page <= max_pages:
            time.sleep(RATE_LIMIT_DELAY)

    return all_filings


def run():
    """Fetch LDA filings by year."""
    state = load_state("filings")
    completed = set(state.get("completed_years", []))

    pending = [y for y in YEARS if y not in completed]

    if not pending:
        print("  All filing years up to date")
        return

    print(f"  Fetching filings for {len(pending)} years...")

    for year in pending:
        print(f"  [{year}] Fetching filings (up to {MAX_PAGES_PER_YEAR} pages)...")

        filings = fetch_filings_for_year(year, MAX_PAGES_PER_YEAR)
        print(f"    -> Total: {len(filings):,} filings")

        if filings:
            save_raw_json(filings, f"filings_{year}", compress=True)

        completed.add(year)
        save_state("filings", {"completed_years": list(completed)})

    print(f"  Completed fetching filings")


if __name__ == "__main__":
    run()
