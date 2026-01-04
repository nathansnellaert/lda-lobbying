"""Fetch LDA filings (LD-1 registrations and LD-2 quarterly activity reports).

Uses the Senate LDA API to fetch filing data.

API: https://lda.senate.gov/api/v1/filings/
"""

import time
from subsets_utils import get, save_raw_json, load_state, save_state
from utils import FILING_YEARS, API_BASE, RATE_LIMIT_DELAY


def fetch_filings_for_year(year: int) -> list[dict]:
    """Fetch all filings for a given year with pagination."""
    all_filings = []
    url = f"{API_BASE}/filings/"
    params = {"filing_year": year}
    page = 1

    while url:
        print(f"    Page {page}...")
        response = get(url, params=params if page == 1 else None)
        data = response.json()

        results = data.get("results", [])
        all_filings.extend(results)

        url = data.get("next")
        page += 1

        if url:
            time.sleep(RATE_LIMIT_DELAY)

    return all_filings


def run():
    """Fetch LDA filings by year."""
    state = load_state("filings")
    completed = set(state.get("completed_years", []))

    pending = [y for y in FILING_YEARS if y not in completed]

    if not pending:
        print("  All filing years up to date")
        return

    print(f"  Fetching filings for {len(pending)} years...")

    for year in pending:
        print(f"  [{year}] Fetching filings...")

        filings = fetch_filings_for_year(year)
        print(f"    -> Total: {len(filings):,} filings")

        if filings:
            save_raw_json(filings, f"filings_{year}", compress=True)

        completed.add(year)
        save_state("filings", {"completed_years": list(completed)})

    print(f"  Completed fetching filings")


if __name__ == "__main__":
    run()
