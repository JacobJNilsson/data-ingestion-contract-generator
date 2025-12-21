"""Tests for source database CLI commands."""

import json
import sqlite3
import tempfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from cli.commands.source import _format_table_text, app
from core.models import TableInfo

runner = CliRunner()


@pytest.fixture
def sqlite_db() -> str:  # type: ignore[misc]
    """Create a temporary SQLite database with test data"""
    # Create temporary database file
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".db") as temp_db:
        db_path = temp_db.name

    # Connect and create schema
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create test tables
    cursor.execute(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            email TEXT
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER,
            total REAL,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """
    )

    # Insert test data
    cursor.execute("INSERT INTO users (username, email) VALUES ('alice', 'alice@example.com')")
    cursor.execute("INSERT INTO users (username, email) VALUES ('bob', 'bob@example.com')")
    cursor.execute("INSERT INTO orders (user_id, total) VALUES (1, 100.50)")

    conn.commit()
    conn.close()

    yield f"sqlite:///{db_path}"

    # Cleanup
    Path(db_path).unlink(missing_ok=True)


def test_source_database_list_text_output(sqlite_db: str) -> None:
    """Test listing database tables with text output format"""
    result = runner.invoke(
        app,
        [
            "database",
            "list",
            sqlite_db,
            "--type",
            "sqlite",
            "--format",
            "text",
        ],
    )

    assert result.exit_code == 0
    assert "Tables" in result.stdout
    assert "users" in result.stdout
    assert "orders" in result.stdout
    # Should show column counts
    assert "columns" in result.stdout


def test_source_database_list_json_output(sqlite_db: str) -> None:
    """Test listing database tables with JSON output format"""
    result = runner.invoke(
        app,
        [
            "database",
            "list",
            sqlite_db,
            "--type",
            "sqlite",
            "--format",
            "json",
        ],
    )

    assert result.exit_code == 0
    # Parse JSON output - try to extract JSON array from output
    json_start = result.stdout.find("[")
    json_end = result.stdout.rfind("]") + 1
    if json_start >= 0 and json_end > json_start:
        tables = json.loads(result.stdout[json_start:json_end])
    else:
        # Fallback: try parsing entire stdout
        tables = json.loads(result.stdout.strip())

    assert isinstance(tables, list)
    assert len(tables) >= 2  # users and orders

    # Verify table_name field exists (not 'name')
    table_names = [t["table_name"] for t in tables]
    assert "users" in table_names
    assert "orders" in table_names

    # Verify structure
    users_table = next((t for t in tables if t["table_name"] == "users"), None)
    assert users_table is not None
    assert "table_name" in users_table
    assert "column_count" in users_table
    assert "type" in users_table


def test_format_table_text_with_table_name() -> None:
    """Test that _format_table_text correctly accesses table_name field"""
    table = TableInfo(
        table_name="test_table",
        column_count=5,
        type="table",
    )

    lines = _format_table_text(table, with_fields=False)

    assert len(lines) == 1
    assert "test_table" in lines[0]
    assert "5 columns" in lines[0]


def test_format_table_text_with_fields_flag() -> None:
    """Test _format_table_text with with_fields=True (currently columns not supported)"""
    table = TableInfo(
        table_name="test_table",
        column_count=2,
        type="table",
    )

    lines = _format_table_text(table, with_fields=True)

    # Currently only shows table name and column count
    assert len(lines) == 1
    assert "test_table" in lines[0]
    assert "2 columns" in lines[0]


def test_source_database_list_empty_database(tmp_path: Path) -> None:
    """Test listing tables from an empty database"""
    db_path = tmp_path / "empty.db"
    connection_string = f"sqlite:///{db_path}"

    # Create empty database
    conn = sqlite3.connect(str(db_path))
    conn.close()

    result = runner.invoke(
        app,
        [
            "database",
            "list",
            connection_string,
            "--type",
            "sqlite",
        ],
    )

    assert result.exit_code == 0
    assert "No tables found" in result.stdout
