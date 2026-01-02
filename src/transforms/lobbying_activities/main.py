"""Transform LDA filings into lda_lobbying_activities dataset.

Normalizes lobbying activities from filings. Each row represents one
lobbying activity (issue code + description) from a filing.
"""

import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from .test import test

# Must match ingest/filings.py YEARS
YEARS = [2024, 2023, 2022, 2021, 2020]

DATASET_ID = "lda_lobbying_activities"

METADATA = {
    "id": DATASET_ID,
    "title": "LDA Lobbying Activities",
    "description": "Lobbying activities from LDA filings. Each row is one lobbying activity from a filing, with issue code, description, and lobbyists involved. Use this to search for lobbying on specific issues.",
    "column_descriptions": {
        "filing_uuid": "Unique identifier of the parent filing",
        "filing_year": "Year of the filing",
        "registrant_name": "Name of the lobbying firm",
        "client_name": "Name of the client",
        "issue_code": "General issue area code (e.g., HCR for Healthcare)",
        "issue_area": "Human-readable issue area name",
        "description": "Description of the specific lobbying activity",
        "lobbyist_names": "Names of lobbyists who worked on this activity",
        "government_entities": "Government entities lobbied (if specified)",
    }
}


def run():
    """Transform, validate, and upload dataset."""
    all_records = []

    for year in YEARS:
        print(f"  Processing filings_{year}...")

        filings = load_raw_json(f"filings_{year}")

        file_records = 0
        for filing in filings:
            activities = filing.get("lobbying_activities") or []
            registrant = filing.get("registrant") or {}
            client = filing.get("client") or {}

            for activity in activities:
                # Extract lobbyist names
                lobbyist_entries = activity.get("lobbyists") or []
                lobbyist_names = []
                for entry in lobbyist_entries:
                    lobbyist = entry.get("lobbyist") or {}
                    first = lobbyist.get("first_name") or ""
                    last = lobbyist.get("last_name") or ""
                    if first or last:
                        lobbyist_names.append(f"{first} {last}".strip())

                # Extract government entities
                gov_entities = activity.get("government_entities") or []
                gov_names = [e.get("name") for e in gov_entities if e.get("name")]

                record = {
                    "filing_uuid": filing.get("filing_uuid"),
                    "filing_year": filing.get("filing_year"),
                    "registrant_name": registrant.get("name"),
                    "client_name": client.get("name"),
                    "issue_code": activity.get("general_issue_code"),
                    "issue_area": activity.get("general_issue_code_display"),
                    "description": activity.get("description"),
                    "lobbyist_names": lobbyist_names if lobbyist_names else None,
                    "government_entities": gov_names if gov_names else None,
                }
                all_records.append(record)
                file_records += 1

        print(f"    -> {file_records:,} activities")

    print(f"  Total: {len(all_records):,} records")

    table = pa.Table.from_pylist(all_records)

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
