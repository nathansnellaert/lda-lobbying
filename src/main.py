"""LDA Lobbying Disclosure Connector - Lobbying Disclosure Act data from Senate.

Data flow:
1. filings: Fetch lobbying registration and quarterly activity reports via API
2. contributions: Fetch contribution reports (LD-203)
3. transform: Clean and transform into datasets

Data source: https://lda.senate.gov/api/
License: US Government Public Domain
"""

import argparse
import os

os.environ['RUN_ID'] = os.getenv('RUN_ID', 'local-run')

from subsets_utils import validate_environment
from ingest import filings as ingest_filings
from ingest import contributions as ingest_contributions
from transforms.filings import main as transform_filings
from transforms.lobbying_activities import main as transform_activities


def main():
    parser = argparse.ArgumentParser(description="LDA Lobbying Disclosure Connector")
    parser.add_argument("--ingest-only", action="store_true", help="Only fetch data from LDA API")
    parser.add_argument("--transform-only", action="store_true", help="Only transform existing raw data")
    args = parser.parse_args()

    validate_environment([])

    should_ingest = not args.transform_only
    should_transform = not args.ingest_only

    if should_ingest:
        print("\n=== Phase 1: Ingest ===")
        print("\n--- Filings (LD-1/LD-2) ---")
        ingest_filings.run()
        print("\n--- Contributions (LD-203) ---")
        ingest_contributions.run()

    if should_transform:
        print("\n=== Phase 2: Transform ===")

        print("\n--- Filings ---")
        transform_filings.run()

        print("\n--- Lobbying Activities ---")
        transform_activities.run()


if __name__ == "__main__":
    main()
