"""Fetch LDA contribution reports (LD-203).

Fetches semi-annual lobbying contribution reports from the Senate LDA API.
These track political contributions made by lobbyists and registrants.

API: https://lda.senate.gov/api/v1/contributions/
"""

import time
from subsets_utils import get, save_raw_json, load_state, save_state
from utils import CONTRIBUTION_YEARS, API_BASE, RATE_LIMIT_DELAY


def fetch_contributions_for_year(year: int) -> list[dict]:
    """Fetch all contribution reports for a given year with pagination."""
    all_contributions = []
    url = f"{API_BASE}/contributions/"
    params = {"filing_year": year}
    page = 1

    while url:
        print(f"    Page {page}...")
        response = get(url, params=params if page == 1 else None)
        data = response.json()

        results = data.get("results", [])
        all_contributions.extend(results)

        url = data.get("next")
        page += 1

        if url:
            time.sleep(RATE_LIMIT_DELAY)

    return all_contributions


def run():
    """Fetch LDA contribution reports by year."""
    state = load_state("contributions")
    completed = set(state.get("completed_years", []))

    pending = [y for y in CONTRIBUTION_YEARS if y not in completed]

    if not pending:
        print("  All contribution years up to date")
        return

    print(f"  Fetching contributions for {len(pending)} years...")

    for year in pending:
        print(f"  [{year}] Fetching contributions...")

        contributions = fetch_contributions_for_year(year)
        print(f"    -> Total: {len(contributions):,} reports")

        if contributions:
            save_raw_json(contributions, f"contributions_{year}", compress=True)

        completed.add(year)
        save_state("contributions", {"completed_years": list(completed)})

    print(f"  Completed fetching contributions")


if __name__ == "__main__":
    run()
