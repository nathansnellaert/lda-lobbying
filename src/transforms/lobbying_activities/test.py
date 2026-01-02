"""Validation for LDA lobbying activities dataset."""

import pyarrow as pa
from subsets_utils import validate
from subsets_utils.testing import assert_max_length, assert_in_range


def test(table: pa.Table) -> None:
    """Validate LDA lobbying activities output."""
    validate(table, {
        "columns": {
            "filing_uuid": "string",
            "filing_year": "int",
            "registrant_name": "string",
            "client_name": "string",
            "issue_code": "string",
            "issue_area": "string",
            "description": "string",
            "lobbyist_names": "list",
            "government_entities": "list",
        },
        "not_null": ["filing_uuid", "filing_year", "issue_code"],
        "min_rows": 100,
    })

    # Filing year should be reasonable
    assert_in_range(table, "filing_year", 1999, 2030)

    # Issue codes are typically 3-letter codes
    assert_max_length(table, "issue_code", 5)

    # Issue area should be descriptive
    issue_areas = set(table.column("issue_area").to_pylist())
    issue_areas.discard(None)
    assert len(issue_areas) > 10, f"Expected more unique issue areas, got {len(issue_areas)}"

    print(f"    Validated {len(table):,} activities across {len(issue_areas)} issue areas")
