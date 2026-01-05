"""Tests for Supabase source analysis."""

import pytest

from core.sources.supabase import _map_postgres_type_to_contract_type, _validate_project_url


def test_validate_project_url_valid() -> None:
    """Test that valid Supabase URLs pass validation."""
    # Standard Supabase URL
    _validate_project_url("https://xxxxx.supabase.co")

    # With trailing slash
    _validate_project_url("https://xxxxx.supabase.co/")

    # With subdomain
    _validate_project_url("https://project-ref.supabase.co")


def test_validate_project_url_invalid_protocol() -> None:
    """Test that non-HTTPS URLs are rejected."""
    with pytest.raises(ValueError, match="must start with 'https://'"):
        _validate_project_url("http://xxxxx.supabase.co")

    with pytest.raises(ValueError, match="must start with 'https://'"):
        _validate_project_url("xxxxx.supabase.co")


def test_validate_project_url_invalid_domain() -> None:
    """Test that non-Supabase domains are rejected."""
    with pytest.raises(ValueError, match="must be a valid Supabase URL"):
        _validate_project_url("https://example.com")

    with pytest.raises(ValueError, match="must be a valid Supabase URL"):
        _validate_project_url("https://supabase.com")


def test_map_postgres_type_to_contract_type() -> None:
    """Test PostgreSQL type mapping to contract types."""
    # Integer types
    assert _map_postgres_type_to_contract_type("integer") == "integer"
    assert _map_postgres_type_to_contract_type("bigint") == "integer"
    assert _map_postgres_type_to_contract_type("smallint") == "integer"
    assert _map_postgres_type_to_contract_type("int4") == "integer"

    # Float types
    assert _map_postgres_type_to_contract_type("real") == "float"
    assert _map_postgres_type_to_contract_type("double precision") == "float"
    assert _map_postgres_type_to_contract_type("numeric") == "float"

    # Boolean
    assert _map_postgres_type_to_contract_type("boolean") == "boolean"
    assert _map_postgres_type_to_contract_type("bool") == "boolean"

    # Date/Time types
    assert _map_postgres_type_to_contract_type("date") == "date"
    assert _map_postgres_type_to_contract_type("timestamp") == "datetime"
    assert _map_postgres_type_to_contract_type("timestamptz") == "datetime"

    # Text types
    assert _map_postgres_type_to_contract_type("text") == "text"
    assert _map_postgres_type_to_contract_type("varchar") == "text"
    assert _map_postgres_type_to_contract_type("character varying") == "text"
    assert _map_postgres_type_to_contract_type("uuid") == "text"
    assert _map_postgres_type_to_contract_type("json") == "text"
    assert _map_postgres_type_to_contract_type("jsonb") == "text"

    # Array types
    assert _map_postgres_type_to_contract_type("integer[]") == "array[integer]"
    assert _map_postgres_type_to_contract_type("text[]") == "array[text]"
    assert _map_postgres_type_to_contract_type("boolean[]") == "array[boolean]"

    # Unknown types should default to text
    assert _map_postgres_type_to_contract_type("unknown_type") == "text"


@pytest.mark.skip(reason="Requires live Supabase connection - integration test only")
def test_analyze_supabase_table() -> None:
    """Test analyzing a Supabase table.

    This test is skipped by default as it requires a live Supabase connection.
    Run with pytest -v --run-integration to include integration tests.
    """
    from core.sources.supabase import analyze_supabase_table

    # This would require actual Supabase credentials
    # For integration testing, set environment variables:
    # SUPABASE_PROJECT_URL, SUPABASE_API_KEY, SUPABASE_TEST_TABLE
    project_url = "https://example.supabase.co"
    api_key = "fake-api-key"
    table_name = "test_table"

    with pytest.raises(ValueError):
        analyze_supabase_table(project_url, api_key, table_name)
