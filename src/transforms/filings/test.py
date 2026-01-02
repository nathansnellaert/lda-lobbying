"""Validation for LDA filings dataset."""

import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_valid_date, assert_max_length, assert_in_range


def test(table: pa.Table) -> None:
    """Validate LDA filings output."""
    validate(table, {
        "columns": {
            "filing_uuid": "string",
            "filing_year": "int",
            "filing_quarter": "string",
            "filing_type": "string",
            "filing_type_display": "string",
            "posted_date": "string",
            "termination_date": "string",
            "registrant_id": "int",
            "registrant_name": "string",
            "registrant_state": "string",
            "client_id": "int",
            "client_name": "string",
            "client_state": "string",
            "client_country": "string",
            "income": "double",
            "expenses": "double",
        },
        "not_null": ["filing_uuid", "filing_year", "filing_type"],
        "unique": ["filing_uuid"],
        "min_rows": 100,
    })

    # Filing year should be reasonable
    assert_in_range(table, "filing_year", 1999, 2030)

    # Posted date format
    assert_valid_date(table, "posted_date")

    # Filing type should be short codes
    assert_max_length(table, "filing_type", 3)

    # State codes should be 2 characters
    states = [s for s in table.column("registrant_state").to_pylist() if s]
    for s in states[:100]:
        assert len(s) == 2, f"Invalid registrant_state: {s}"

    # Quarter format
    quarters = set(table.column("filing_quarter").to_pylist())
    valid_quarters = {"Q1", "Q2", "Q3", "Q4", None}
    assert quarters.issubset(valid_quarters), f"Invalid quarters: {quarters - valid_quarters}"

    print(f"    Validated {len(table):,} filings")
