"""Tests for CSV file analysis."""

from pathlib import Path

import pytest

from core.sources.csv import analyze_csv_file, detect_delimiter

# Path to the real transaction file (if it exists)
REAL_TRANSACTION_FILE = Path(__file__).parent.parent.parent.parent / "data" / "transaktioner_2018-10-15_2025-12-20.csv"


@pytest.fixture
def comma_csv_file(tmp_path: Path) -> Path:
    """Create a temporary CSV file with comma delimiter."""
    csv_file = tmp_path / "test_comma.csv"
    csv_file.write_text(
        "Name,Age,City\nAlice,30,Stockholm\nBob,25,Göteborg\nCharlie,35,Malmö\n",
        encoding="utf-8",
    )
    return csv_file


@pytest.fixture
def semicolon_csv_file(tmp_path: Path) -> Path:
    """Create a temporary CSV file with semicolon delimiter."""
    csv_file = tmp_path / "test_semicolon.csv"
    csv_file.write_text(
        "Datum;Konto;Typ av transaktion;Värdepapper/beskrivning;Antal;Kurs;Belopp\n"
        "2025-12-16;5395872;Utländsk källskatt;Utdelning GOOG.O;20;;-5,83\n"
        "2025-12-16;5395872;Utdelning;Alphabet Inc Class C;20;0,21;38,88\n"
        "2025-12-12;5395872;Köp;Microsoft;8;0,91;67,45\n",
        encoding="utf-8",
    )
    return csv_file


def test_detect_delimiter_comma(comma_csv_file: Path) -> None:
    """Test that comma delimiter is correctly detected."""
    delimiter = detect_delimiter(str(comma_csv_file), "utf-8")
    assert delimiter == ",", f"Expected comma delimiter, got {delimiter!r}"


def test_detect_delimiter_semicolon(semicolon_csv_file: Path) -> None:
    """Test that semicolon delimiter is correctly detected."""
    delimiter = detect_delimiter(str(semicolon_csv_file), "utf-8")
    assert delimiter == ";", f"Expected semicolon delimiter, got {delimiter!r}"


def test_analyze_csv_with_comma_delimiter(comma_csv_file: Path) -> None:
    """Test analyzing a CSV file with comma delimiter."""
    result = analyze_csv_file(comma_csv_file, sample_size=10)

    assert result.file_type == "csv"
    assert result.delimiter == ","
    assert result.encoding == "utf-8"
    assert result.has_header is True
    assert result.total_rows == 3
    assert result.sample_fields == ["Name", "Age", "City"]
    assert len(result.data_types) == 3
    assert result.data_types[0] == "text"  # Name
    assert result.data_types[1] == "numeric"  # Age
    assert result.data_types[2] == "text"  # City


def test_analyze_csv_with_semicolon_delimiter(semicolon_csv_file: Path) -> None:
    """Test analyzing a CSV file with semicolon delimiter."""
    result = analyze_csv_file(semicolon_csv_file, sample_size=10)

    assert result.file_type == "csv"
    assert result.delimiter == ";", f"Expected semicolon delimiter, got {result.delimiter!r}"
    assert result.encoding == "utf-8"
    assert result.has_header is True
    assert result.total_rows == 3
    assert result.sample_fields == [
        "Datum",
        "Konto",
        "Typ av transaktion",
        "Värdepapper/beskrivning",
        "Antal",
        "Kurs",
        "Belopp",
    ]
    assert len(result.data_types) == 7


def test_analyze_csv_empty_file(tmp_path: Path) -> None:
    """Test analyzing an empty CSV file."""
    empty_file = tmp_path / "empty.csv"
    empty_file.write_text("", encoding="utf-8")

    result = analyze_csv_file(empty_file, sample_size=10)

    assert result.file_type == "csv"
    assert result.has_header is False
    assert result.total_rows == 0
    assert result.sample_fields == []
    assert "File is empty" in result.issues


def test_analyze_csv_with_bom(tmp_path: Path) -> None:
    """Test analyzing a CSV file with UTF-8 BOM."""
    bom_file = tmp_path / "bom.csv"
    bom_file.write_text(
        "\ufeffName,Age,City\nAlice,30,Stockholm\n",
        encoding="utf-8",
    )

    result = analyze_csv_file(bom_file, sample_size=10)

    assert result.file_type == "csv"
    assert result.sample_fields == ["Name", "Age", "City"]
    assert any("BOM" in issue for issue in result.issues)


def test_analyze_csv_no_header(tmp_path: Path) -> None:
    """Test analyzing a CSV file without a header row."""
    no_header_file = tmp_path / "no_header.csv"
    no_header_file.write_text(
        "123,456,789\n111,222,333\n444,555,666\n",
        encoding="utf-8",
    )

    result = analyze_csv_file(no_header_file, sample_size=10)

    assert result.file_type == "csv"
    assert result.has_header is False
    assert result.sample_fields == ["column_1", "column_2", "column_3"]
    assert result.total_rows == 3


def test_analyze_csv_with_mixed_types(tmp_path: Path) -> None:
    """Test analyzing a CSV file with mixed data types."""
    mixed_file = tmp_path / "mixed.csv"
    mixed_file.write_text(
        "id,name,price,in_stock\n1,Apple,2.50,true\n2,Banana,1.75,false\n3,Orange,3.00,true\n",
        encoding="utf-8",
    )

    result = analyze_csv_file(mixed_file, sample_size=10)

    assert result.file_type == "csv"
    assert result.sample_fields == ["id", "name", "price", "in_stock"]
    assert result.data_types[0] == "numeric"  # id
    assert result.data_types[1] == "text"  # name
    assert result.data_types[2] == "numeric"  # price
    assert result.data_types[3] == "text"  # in_stock (no boolean detection, falls back to text)


@pytest.fixture
def semicolon_with_commas_csv_file(tmp_path: Path) -> Path:
    """Create a CSV file with semicolon delimiter but with commas in the data.

    This tests a challenging case where both delimiters appear in the file,
    but only semicolon is the true delimiter.
    """
    csv_file = tmp_path / "test_semicolon_with_commas.csv"
    csv_file.write_text(
        "Name;Age;Description;Amount\n"
        "Alice;30;Bought apples, oranges, bananas;125,50\n"
        "Bob;25;Paid rent, utilities;8500,00\n"
        "Charlie;35;Groceries: milk, bread, cheese;450,75\n",
        encoding="utf-8",
    )
    return csv_file


def test_detect_delimiter_semicolon_with_commas_in_data(semicolon_with_commas_csv_file: Path) -> None:
    """Test delimiter detection when semicolon is delimiter but commas appear in data."""
    delimiter = detect_delimiter(str(semicolon_with_commas_csv_file), "utf-8")
    assert delimiter == ";", f"Expected semicolon delimiter even with commas in data, got {delimiter!r}"


def test_analyze_csv_semicolon_with_commas_in_data(semicolon_with_commas_csv_file: Path) -> None:
    """Test analyzing CSV with semicolon delimiter and commas in data values."""
    result = analyze_csv_file(semicolon_with_commas_csv_file, sample_size=10)

    assert result.file_type == "csv"
    assert result.delimiter == ";", f"Expected semicolon delimiter, got {result.delimiter!r}"
    assert result.sample_fields == ["Name", "Age", "Description", "Amount"]
    assert len(result.data_types) == 4
    # Ensure data was parsed correctly (4 fields per row, not split on commas)
    if result.sample_data:
        assert len(result.sample_data[0]) == 4, "Data should have 4 fields, not split on commas"


@pytest.mark.skipif(not REAL_TRANSACTION_FILE.exists(), reason="Real transaction file not available")
def test_detect_delimiter_real_transaction_file() -> None:
    """Test delimiter detection on the actual Swedish transaction file with semicolon delimiter."""
    delimiter = detect_delimiter(str(REAL_TRANSACTION_FILE), "utf-8")
    assert delimiter == ";", f"Expected semicolon delimiter for real transaction file, got {delimiter!r}"


@pytest.mark.skipif(not REAL_TRANSACTION_FILE.exists(), reason="Real transaction file not available")
def test_analyze_real_transaction_file() -> None:
    """Test analyzing the actual Swedish transaction file.

    This is a regression test for the issue where semicolon delimiter was not correctly
    detected in the real transactions_2018-10-15_2025-12-20.csv file with Swedish text.
    """
    result = analyze_csv_file(REAL_TRANSACTION_FILE, sample_size=100)

    assert result.file_type == "csv"
    assert result.delimiter == ";", f"Expected semicolon delimiter, got {result.delimiter!r}"
    assert result.encoding == "utf-8"
    assert result.has_header is True
    assert "Datum" in result.sample_fields, "Should have 'Datum' field"
    assert "Konto" in result.sample_fields, "Should have 'Konto' field"
    assert "Typ av transaktion" in result.sample_fields, "Should have 'Typ av transaktion' field"
    assert "Värdepapper/beskrivning" in result.sample_fields, "Should have 'Värdepapper/beskrivning' field"
    assert len(result.sample_fields) >= 10, f"Expected at least 10 fields, got {len(result.sample_fields)}"
