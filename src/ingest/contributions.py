"""Fetch LDA contribution reports (LD-203).

Fetches semi-annual lobbying contribution reports from the Senate LDA API.
These track political contributions made by lobbyists and registrants.

API: https://lda.senate.gov/api/v1/contributions/
"""

import time
from subsets_utils import get, save_raw_json, load_state, save_state

API_BASE = "https://lda.senate.gov/api/v1"

# Years to fetch - LD-203 required since 2008
YEARS = [2024, 2023, 2022, 2021, 2020]

# Rate limit: 15 requests/minute for unauthenticated = 4 seconds between requests
RATE_LIMIT_DELAY = 4.5

# Pages per year (contribution reports are fewer than filings)
MAX_PAGES_PER_YEAR = 100


def fetch_contributions_for_year(year: int, max_pages: int) -> list[dict]:
    """Fetch contribution reports for a given year with pagination."""
    all_contributions = []
    url = f"{API_BASE}/contributions/"
    params = {"filing_year": year}
    page = 1

    while url and page <= max_pages:
        print(f"    Page {page}...")
        response = get(url, params=params if page == 1 else None)
        data = response.json()

        results = data.get("results", [])
        all_contributions.extend(results)

        url = data.get("next")
        page += 1

        if url and page <= max_pages:
            time.sleep(RATE_LIMIT_DELAY)

    return all_contributions


def run():
    """Fetch LDA contribution reports by year."""
    state = load_state("contributions")
    completed = set(state.get("completed_years", []))

    pending = [y for y in YEARS if y not in completed]

    if not pending:
        print("  All contribution years up to date")
        return

    print(f"  Fetching contributions for {len(pending)} years...")

    for year in pending:
        print(f"  [{year}] Fetching contributions...")

        contributions = fetch_contributions_for_year(year, MAX_PAGES_PER_YEAR)
        print(f"    -> Total: {len(contributions):,} reports")

        if contributions:
            save_raw_json(contributions, f"contributions_{year}", compress=True)

        completed.add(year)
        save_state("contributions", {"completed_years": list(completed)})

    print(f"  Completed fetching contributions")


if __name__ == "__main__":
    run()
