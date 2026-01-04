"""Transform LDA filings into lda_filings dataset.

Extracts core filing information including registrant, client, and amounts.
Each row represents one lobbying disclosure filing.
"""

import pyarrow as pa
from subsets_utils import load_raw_json, upload_data, publish
from utils import YEARS
from .test import test

DATASET_ID = "lda_filings"

METADATA = {
    "id": DATASET_ID,
    "title": "LDA Lobbying Filings",
    "description": "Lobbying Disclosure Act filings from the US Senate. Each row is one filing (LD-1 registration or LD-2 quarterly report). Contains registrant (lobbying firm), client (who paid), and reported amounts. Foreign entities are excluded from this table.",
    "column_descriptions": {
        "filing_uuid": "Unique identifier for the filing",
        "filing_year": "Year of the filing",
        "filing_quarter": "Quarter (Q1, Q2, Q3, Q4, or null for registrations)",
        "filing_type": "Filing type code (RR=Registration, Q1-Q4=Quarterly, etc.)",
        "filing_type_display": "Human-readable filing type",
        "posted_date": "Date the filing was posted (YYYY-MM-DD)",
        "termination_date": "Date of termination if applicable (YYYY-MM-DD)",
        "registrant_id": "Unique ID of the registrant (lobbying firm)",
        "registrant_name": "Name of the registrant (lobbying firm)",
        "registrant_state": "State where registrant is located (2-letter)",
        "client_id": "Unique ID of the client",
        "client_name": "Name of the client (who paid for lobbying)",
        "client_state": "State where client is located (2-letter)",
        "client_country": "Country where client is located (2-letter ISO)",
        "income": "Income reported in USD (for firms lobbying on behalf of clients)",
        "expenses": "Expenses reported in USD (for organizations lobbying for themselves)",
    }
}


def parse_amount(val: str | None) -> float | None:
    """Parse amount string to float."""
    if not val:
        return None
    try:
        return float(val.replace(",", ""))
    except (ValueError, TypeError):
        return None


def parse_date(dt_str: str | None) -> str | None:
    """Extract YYYY-MM-DD from ISO datetime string."""
    if not dt_str:
        return None
    return dt_str[:10]


def extract_quarter(period: str | None) -> str | None:
    """Extract quarter from filing_period."""
    if not period:
        return None
    mapping = {
        "first_quarter": "Q1",
        "second_quarter": "Q2",
        "third_quarter": "Q3",
        "fourth_quarter": "Q4",
        "mid_year": None,  # Registrations
    }
    return mapping.get(period)


def run():
    """Transform, validate, and upload dataset."""
    all_records = []

    # Process all years
    for year in YEARS:
        print(f"  Processing filings_{year}...")

        filings = load_raw_json(f"filings_{year}")

        for filing in filings:
            registrant = filing.get("registrant") or {}
            client = filing.get("client") or {}

            record = {
                "filing_uuid": filing.get("filing_uuid"),
                "filing_year": filing.get("filing_year"),
                "filing_quarter": extract_quarter(filing.get("filing_period")),
                "filing_type": filing.get("filing_type"),
                "filing_type_display": filing.get("filing_type_display"),
                "posted_date": parse_date(filing.get("dt_posted")),
                "termination_date": filing.get("termination_date"),
                "registrant_id": registrant.get("id"),
                "registrant_name": registrant.get("name"),
                "registrant_state": registrant.get("state"),
                "client_id": client.get("id"),
                "client_name": client.get("name"),
                "client_state": client.get("state"),
                "client_country": client.get("country"),
                "income": parse_amount(filing.get("income")),
                "expenses": parse_amount(filing.get("expenses")),
            }
            all_records.append(record)

        print(f"    -> {len(filings):,} filings")

    print(f"  Total: {len(all_records):,} records")

    table = pa.Table.from_pylist(all_records)

    test(table)

    upload_data(table, DATASET_ID)
    publish(DATASET_ID, METADATA)


if __name__ == "__main__":
    run()
