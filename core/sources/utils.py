"""Common utilities for source analysis."""

from pathlib import Path

from core.models import FieldProfile, NumericFormatInfo


def detect_file_encoding(file_path: str) -> str:
    """Detect file encoding"""
    encodings = ["utf-8", "utf-8-sig", "latin-1", "cp1252", "iso-8859-1"]
    file_path_obj = Path(file_path)
    for encoding in encodings:
        try:
            with file_path_obj.open(encoding=encoding) as f:
                f.read()
        except (UnicodeDecodeError, UnicodeError):
            continue
        else:
            return encoding
    return "utf-8"  # Default fallback


def analyze_numeric_format(sample_value: str) -> NumericFormatInfo:
    """Analyze numeric format (e.g., European vs US format)"""
    has_comma_decimal = "," in sample_value.replace(",", "") and "." not in sample_value
    has_thousands_sep = "," in sample_value and "." in sample_value

    return NumericFormatInfo(
        has_comma_decimal=has_comma_decimal,
        has_thousands_sep=has_thousands_sep,
        format="european" if has_comma_decimal else "us",
    )


def is_numeric(value: str) -> bool:
    """Check if a value is numeric (handles US and European formats)

    Handles:
    - US format: 1234.56
    - European format: 1234,56
    - Negative numbers: -1234.56
    """
    value = value.strip()
    if not value:
        return False

    # Remove leading minus sign for negative numbers
    test_value = value.lstrip("-")

    # Check US format (dot as decimal separator)
    if "." in test_value and "," not in test_value:
        return test_value.replace(".", "").isdigit()

    # Check European format (comma as decimal separator)
    if "," in test_value and "." not in test_value:
        return test_value.replace(",", "").isdigit()

    # Check mixed format (e.g., 1,234.56 or 1.234,56)
    # Remove thousands separator and check decimal part
    if "," in test_value and "." in test_value:
        # Could be US (1,234.56) or European (1.234,56)
        # Try US format first
        us_format = test_value.replace(",", "")
        if us_format.replace(".", "").isdigit():
            return True
        # Try European format
        euro_format = test_value.replace(".", "")
        if euro_format.replace(",", "").isdigit():
            return True

    # Simple integer
    return test_value.isdigit()


def is_date(value: str) -> bool:
    """Check if a value is a date

    Handles:
    - ISO format: YYYY-MM-DD (2025-10-25)
    - Slash format: DD/MM/YYYY (25/10/2025)
    - US format: MM/DD/YYYY (10/25/2025)
    """
    value = value.strip()
    if not value:
        return False

    # Check ISO format: YYYY-MM-DD
    if "-" in value:
        parts = value.split("-")
        if len(parts) == 3 and all(part.isdigit() for part in parts):
            # Check if it looks like a valid date (rough check)
            year, month, day = parts
            if len(year) == 4 and 1 <= len(month) <= 2 and 1 <= len(day) <= 2:
                return True

    # Check slash format: DD/MM/YYYY or MM/DD/YYYY
    if "/" in value:
        parts = value.split("/")
        if len(parts) == 3 and all(part.isdigit() for part in parts):
            return True

    return False


def detect_data_types(sample_row: list[str]) -> list[str]:
    """Detect data types from sample row"""
    types = []
    for raw_value in sample_row:
        value = raw_value.strip()
        if not value:
            types.append("empty")
        elif is_date(value):
            types.append("date")
        elif is_numeric(value):
            types.append("numeric")
        else:
            types.append("text")
    return types


def detect_data_types_from_multiple_rows(data_rows: list[list[str]], num_columns: int) -> list[str]:
    """Detect data types by scanning multiple rows

    This handles sparse data where early rows may have empty values.
    Returns the most specific non-empty type found for each column.

    Priority: date > numeric > text > empty
    """
    # Initialize with "empty" for all columns
    final_types = ["empty"] * num_columns

    # Scan all available rows
    for row in data_rows:
        # Ensure row has enough columns (pad with empty strings if needed)
        padded_row = row + [""] * (num_columns - len(row))

        # Detect types for this row
        row_types = detect_data_types(padded_row[:num_columns])

        # Update final types with more specific types
        for col_idx, row_type in enumerate(row_types):
            if row_type == "empty":
                # Skip empty values
                continue
            elif final_types[col_idx] == "empty":
                # First non-empty value sets the type
                final_types[col_idx] = row_type
            elif final_types[col_idx] == "text":
                # Text is least specific, keep it
                continue
            elif row_type == "text":
                # If we see text but had numeric/date, downgrade to text
                final_types[col_idx] = "text"
            elif final_types[col_idx] == "numeric" and row_type == "date":
                # Date is more specific than numeric for date-like values
                final_types[col_idx] = "date"
            elif final_types[col_idx] == "date" and row_type == "numeric":
                # Keep date as it's more specific
                continue

    return final_types


def _normalize_mixed_separators(v: str) -> str:
    """Handle strings containing both commas and dots."""
    if v.rfind(",") > v.rfind("."):
        # Comma is decimal separator (European: 1.234,56)
        return v.replace(".", "").replace(",", ".")
    # Dot is decimal separator (US: 1,234.56)
    return v.replace(",", "")


def _normalize_comma_only(v: str) -> str:
    """Handle strings containing only commas."""
    # Only comma: check if it's a decimal or thousands separator
    if v.count(",") == 1 and (len(v) - v.rfind(",") <= 3):
        # Likely a decimal separator (European)
        return v.replace(",", ".")
    # Likely a thousands separator (mixed format)
    return v.replace(",", "")


def _normalize_dot_only(v: str) -> str:
    """Handle strings containing only dots."""
    # Only dot: check if it's a decimal or thousands separator
    if v.count(".") == 1 or (len(v) - v.rfind(".") <= 3):
        # Likely a decimal separator (US)
        return v
    # Likely a thousands separator (European)
    return v.replace(".", "")


def _normalize_numeric_string(v: str) -> str:
    """Normalize a potential numeric string to US format (dot as decimal separator)."""
    v = v.strip()
    if not v:
        return v

    has_comma = "," in v
    has_dot = "." in v

    if has_comma and has_dot:
        return _normalize_mixed_separators(v)
    if has_comma:
        return _normalize_comma_only(v)
    if has_dot:
        return _normalize_dot_only(v)

    return v


def _calculate_min_max(values: list[str]) -> tuple[str | None, str | None]:
    """Calculate min and max values from a list of strings, trying numeric conversion first."""
    if not values:
        return None, None

    try:
        # Attempt numeric comparison
        numeric_vals = []
        for v in values:
            clean_v = _normalize_numeric_string(v)
            if clean_v:
                numeric_vals.append(float(clean_v))

        if not numeric_vals:
            return None, None
        return str(min(numeric_vals)), str(max(numeric_vals))
    except (ValueError, TypeError):
        # Fallback to string min/max
        return min(values), max(values)


def profile_field_data(values: list[str]) -> FieldProfile:
    """Calculate profiling statistics for a list of values"""
    total = len(values)
    if total == 0:
        return FieldProfile()

    null_count = sum(1 for v in values if not v or not v.strip())
    non_null_values = [v.strip() for v in values if v and v.strip()]
    distinct_values = sorted(set(non_null_values))

    min_val, max_val = _calculate_min_max(non_null_values)

    return FieldProfile(
        null_count=null_count,
        null_percentage=round((null_count / total) * 100, 2),
        distinct_count=len(distinct_values),
        min_value=min_val,
        max_value=max_val,
        sample_values=distinct_values[:5],
    )
