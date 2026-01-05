"""Supabase table analysis using PostgREST OpenAPI schema.

Uses PostgREST's OpenAPI endpoint to read actual PostgreSQL schema definitions.

LIMITATIONS:
- Primary keys: Not exposed in OpenAPI schema (PostgREST limitation)
- Foreign keys: Not exposed in OpenAPI schema
- Default values: Not exposed in OpenAPI schema
- Row Level Security: Tables with RLS may not appear in schema if not accessible

ADVANTAGES over data inference:
- Works with empty tables
- Uses actual PostgreSQL data types (not inferred from values)
- Accurate nullable detection from schema
- No need to sample data
"""

import httpx
from postgrest.types import CountMethod
from supabase import Client, create_client

from core.models import (
    ColumnInfo,
    FieldConstraint,
    FieldDefinition,
    QualityObservation,
    SourceSchema,
    SupabaseMetadata,
)


def _map_openapi_type_to_contract(field_spec: dict[str, object]) -> str:
    """Map OpenAPI type specification to contract data type.

    Args:
        field_spec: OpenAPI property specification with type and format

    Returns:
        Contract data type string
    """
    if not isinstance(field_spec, dict):
        return "text"

    field_type = field_spec.get("type")
    if not isinstance(field_type, str):
        field_type = "string"

    field_format = field_spec.get("format")
    if not isinstance(field_format, str):
        field_format = None

    # Handle arrays
    if field_type == "array":
        items = field_spec.get("items")
        if isinstance(items, dict):
            element_type = _map_openapi_type_to_contract(items)
            return f"array[{element_type}]"
        return "array[text]"

    # Map based on type + format
    if field_type == "integer":
        if field_format == "int64":
            return "bigint"
        return "integer"

    if field_type == "number":
        if field_format == "double":
            return "double precision"
        return "numeric"

    if field_type == "boolean":
        return "boolean"

    if field_type == "string":
        # Use format to determine specific string types
        if field_format == "date":
            return "date"
        if field_format == "date-time":
            return "timestamp"
        if field_format == "time":
            return "time"
        if field_format == "uuid":
            return "uuid"
        if field_format == "binary":
            return "bytea"
        return "text"

    # Default to text for unknown types
    return "text"


def _validate_project_url(project_url: str) -> None:
    """Validate Supabase project URL format.

    Args:
        project_url: Supabase project URL

    Raises:
        ValueError: If URL format is invalid
    """
    if not project_url.startswith("https://"):
        raise ValueError(f"Project URL must start with 'https://': {project_url}")

    # Remove protocol and trailing slash for validation
    domain = project_url.replace("https://", "").rstrip("/")

    # Must end with .supabase.co (not just contain it)
    if not domain.endswith(".supabase.co"):
        raise ValueError(f"Project URL must be a valid Supabase URL (*.supabase.co): {project_url}")


def _fetch_sample_data(
    supabase: Client, table_name: str, sample_size: int
) -> tuple[list[str], list[dict[str, object]], int]:
    """Fetch sample data from Supabase table.

    Args:
        supabase: Supabase client
        table_name: Table to sample
        sample_size: Number of rows to fetch

    Returns:
        Tuple of (field_names, sample_rows, total_rows)

    Raises:
        ValueError: If table is empty or has unexpected format
    """
    # Get schema from first row
    schema_response = supabase.table(table_name).select("*").limit(1).execute()

    if not schema_response.data or len(schema_response.data) == 0:
        raise ValueError(
            f"Table '{table_name}' is empty. Cannot infer schema from empty table. "
            f"Note: If table exists but appears empty, check Row Level Security (RLS) policies "
            f"and ensure the API key has appropriate permissions."
        )

    # Get field names from first row
    first_row = schema_response.data[0]
    if not isinstance(first_row, dict):
        raise ValueError(f"Unexpected response format from Supabase: expected dict, got {type(first_row)}")
    field_names = list(first_row.keys())

    # Sample data for type inference and quality metrics
    sample_response = supabase.table(table_name).select("*").limit(sample_size).execute()
    sample_data = sample_response.data
    if not isinstance(sample_data, list):
        raise ValueError(f"Unexpected response format from Supabase: expected list, got {type(sample_data)}")

    # Validate all rows are dicts
    sample_rows: list[dict[str, object]] = []
    for row in sample_data:
        if not isinstance(row, dict):
            raise ValueError(f"Unexpected row format: expected dict, got {type(row)}")
        sample_rows.append(dict(row))

    # Get total row count
    count_response = supabase.table(table_name).select("*", count=CountMethod.exact).limit(0).execute()
    total_rows = count_response.count or len(sample_rows)

    return field_names, sample_rows, total_rows


def _infer_array_type(array_value: list[object]) -> str:
    """Infer array element type from first element.

    Args:
        array_value: Array to analyze

    Returns:
        Array type string (e.g., "array[integer]")
    """
    if not array_value:
        return "array[text]"
    if isinstance(array_value[0], int):
        return "array[integer]"
    if isinstance(array_value[0], float):
        return "array[float]"
    return "array[text]"


def _infer_field_type(sample_values: list[object]) -> str:
    """Infer field type from sample values.

    Args:
        sample_values: List of non-null sample values

    Returns:
        Inferred data type string
    """
    if not sample_values:
        return "text"

    first_value = sample_values[0]

    # Use type() to avoid bool/int confusion (bool is subclass of int)
    value_type = type(first_value)

    # Map Python types to contract types
    type_map = {
        bool: "boolean",
        int: "integer",
        float: "float",
        dict: "text",  # JSON/JSONB
    }

    if value_type in type_map:
        return type_map[value_type]

    if isinstance(first_value, list):
        return _infer_array_type(first_value)

    return "text"


def _build_field_definitions(
    field_names: list[str], sample_rows: list[dict[str, object]]
) -> tuple[list[FieldDefinition], list[str]]:
    """Build field definitions from sample data.

    Args:
        field_names: List of field names
        sample_rows: Sample data rows

    Returns:
        Tuple of (field_definitions, nullable_columns)
    """
    fields = []
    nullable_columns = []

    for field_name in field_names:
        # Get sample values for this field
        sample_values = [row.get(field_name) for row in sample_rows if row.get(field_name) is not None]

        # Determine if nullable
        has_nulls = any(row.get(field_name) is None for row in sample_rows)
        nullable = has_nulls or len(sample_values) < len(sample_rows)

        if nullable:
            nullable_columns.append(field_name)

        # Infer type
        data_type = _infer_field_type(sample_values)

        # Build constraints
        constraints = []
        if not nullable:
            constraints.append(FieldConstraint(type="not_null"))

        fields.append(FieldDefinition(name=field_name, data_type=data_type, nullable=nullable, constraints=constraints))

    return fields, nullable_columns


def _build_quality_observation(
    field_names: list[str], sample_rows: list[dict[str, object]], total_rows: int, nullable_columns: list[str]
) -> QualityObservation:
    """Build quality observation from sample data.

    Args:
        field_names: List of field names
        sample_rows: Sample data rows
        total_rows: Total row count
        nullable_columns: List of nullable column names

    Returns:
        QualityObservation instance
    """
    # Convert sample data to display format
    sample_data_display: list[list[str]] = []
    for sample_row in sample_rows[:10]:
        sample_data_display.append(
            [str(sample_row.get(field)) if sample_row.get(field) is not None else "" for field in field_names]
        )

    # Build issues list
    issues: list[str] = []
    if total_rows == 0:
        issues.append("Table is empty")
    if nullable_columns:
        issues.append(f"Nullable columns: {', '.join(nullable_columns[:5])}")

    return QualityObservation(
        total_rows=total_rows,
        sample_data=sample_data_display,
        issues=issues,
    )


# Note: This function is not currently used because PostgREST API doesn't expose
# PostgreSQL type information. We infer types from sample data instead.
# Keeping it for potential future use if schema introspection improves.
def _map_postgres_type_to_contract_type(pg_type: str) -> str:
    """Map PostgreSQL type to contract type.

    Args:
        pg_type: PostgreSQL data type

    Returns:
        Contract data type string
    """
    # Remove any array notation
    is_array = pg_type.endswith("[]")
    base_type = pg_type.rstrip("[]")

    # Map PostgreSQL types to contract types
    type_mapping = {
        "integer": "integer",
        "bigint": "integer",
        "smallint": "integer",
        "int2": "integer",
        "int4": "integer",
        "int8": "integer",
        "serial": "integer",
        "bigserial": "integer",
        "real": "float",
        "double precision": "float",
        "numeric": "float",
        "decimal": "float",
        "float4": "float",
        "float8": "float",
        "boolean": "boolean",
        "bool": "boolean",
        "date": "date",
        "timestamp": "datetime",
        "timestamptz": "datetime",
        "timestamp with time zone": "datetime",
        "timestamp without time zone": "datetime",
        "time": "time",
        "timetz": "time",
        "uuid": "text",
        "json": "text",
        "jsonb": "text",
        "text": "text",
        "varchar": "text",
        "character varying": "text",
        "char": "text",
        "character": "text",
    }

    contract_type = type_mapping.get(base_type.lower(), "text")

    if is_array:
        return f"array[{contract_type}]"

    return contract_type


def _build_supabase_metadata_from_fields(
    project_url: str,
    table_name: str,
    fields: list[FieldDefinition],
    nullable_columns: list[str],
    sample_size: int,
) -> SupabaseMetadata:
    """Build SupabaseMetadata from field definitions.

    Args:
        project_url: Supabase project URL
        table_name: Table name
        fields: Field definitions
        nullable_columns: List of nullable column names
        sample_size: Number of rows sampled

    Returns:
        SupabaseMetadata instance
    """
    column_details = [
        ColumnInfo(
            name=field.name,
            type=field.data_type,
            nullable=field.nullable,
            default=None,  # PostgREST doesn't easily expose defaults
        )
        for field in fields
    ]

    return SupabaseMetadata(
        project_url=project_url,
        table_name=table_name,
        primary_keys=[],  # PostgREST doesn't easily expose PKs
        column_count=len(fields),
        nullable_columns=nullable_columns,
        sample_size=sample_size,
        columns=column_details,
    )


def _handle_supabase_error(e: Exception, table_name: str) -> ValueError:
    """Convert Supabase exceptions to descriptive ValueErrors.

    Args:
        e: Original exception
        table_name: Table name being accessed

    Returns:
        ValueError with descriptive message
    """
    error_str = str(e).lower()
    if "404" in error_str or "not found" in error_str:
        return ValueError(f"Table '{table_name}' not found in Supabase project")
    if "401" in error_str or "unauthorized" in error_str:
        return ValueError(
            f"Authentication failed. Check that the API key is valid and has access to table '{table_name}'"
        )
    if "403" in error_str or "forbidden" in error_str:
        return ValueError(
            f"Access denied to table '{table_name}'. Check Row Level Security (RLS) policies and API key permissions"
        )
    return ValueError(f"Failed to analyze Supabase table '{table_name}': {e}")


def analyze_supabase_table(
    project_url: str,
    api_key: str,
    table_name: str,
    sample_size: int = 0,
) -> tuple[SourceSchema, QualityObservation, SupabaseMetadata]:
    """Analyze a Supabase table using OpenAPI schema definition.

    Reads actual PostgreSQL schema from PostgREST OpenAPI endpoint.
    Works with empty tables - no data sampling required for schema.

    Args:
        project_url: Supabase project URL (e.g., https://xxxxx.supabase.co)
        api_key: Supabase API key (anon or service_role key)
        table_name: Table name to analyze
        sample_size: Number of rows to sample for quality metrics (default: 0, schema only)

    Returns:
        Tuple of (SourceSchema, QualityObservation, SupabaseMetadata)

    Raises:
        ValueError: If table is not found, connection fails, or URL is invalid

    Note:
        - Uses actual PostgreSQL schema from OpenAPI (not inferred from data)
        - Works with empty tables
        - Primary keys not available in OpenAPI schema
        - If sample_size > 0, will fetch sample data for quality metrics
    """
    _validate_project_url(project_url)

    try:
        # Get schema from OpenAPI
        fields, metadata = analyze_supabase_table_from_schema(project_url, api_key, table_name)

        # Create source schema
        source_schema = SourceSchema(fields=fields)

        # Initialize empty quality observation
        quality_observation = QualityObservation(
            total_rows=0,
            expectation=None,
            observed_profiling={},
        )

        # Optionally sample data for quality metrics
        if sample_size > 0:
            try:
                supabase: Client = create_client(project_url, api_key)

                # Fetch sample data
                field_names, sample_rows, total_rows = _fetch_sample_data(supabase, table_name, sample_size)

                # Build quality observation with actual data
                nullable_columns = [f.name for f in fields if f.nullable]
                quality_observation = _build_quality_observation(field_names, sample_rows, total_rows, nullable_columns)

                # Update metadata with sample size
                metadata = SupabaseMetadata(
                    project_url=metadata.project_url,
                    table_name=metadata.table_name,
                    primary_keys=metadata.primary_keys,
                    column_count=metadata.column_count,
                    nullable_columns=metadata.nullable_columns,
                    sample_size=len(sample_rows),
                    columns=metadata.columns,
                )

            except Exception:
                # If sampling fails (e.g., empty table, RLS), continue with schema-only info
                pass

        return source_schema, quality_observation, metadata

    except Exception as e:
        raise _handle_supabase_error(e, table_name) from e


def _fetch_openapi_schema(project_url: str, api_key: str) -> dict[str, object]:
    """Fetch PostgREST OpenAPI schema.

    Args:
        project_url: Supabase project URL
        api_key: Supabase API key

    Returns:
        OpenAPI schema dictionary

    Raises:
        ValueError: If connection fails or auth fails
    """
    rest_url = f"{project_url.rstrip('/')}/rest/v1/"

    headers = {
        "apikey": api_key,
        "Authorization": f"Bearer {api_key}",
    }

    try:
        response = httpx.get(rest_url, headers=headers, timeout=10.0)
        response.raise_for_status()
        return response.json()  # type: ignore
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise ValueError("Authentication failed. Check that the API key is valid") from e
        if e.response.status_code == 403:
            raise ValueError("Access denied. Check API key permissions") from e
        raise ValueError(f"Failed to list Supabase tables: HTTP {e.response.status_code}") from e
    except httpx.RequestError as e:
        raise ValueError(f"Failed to connect to Supabase project at {project_url}: {e}") from e


def _extract_table_names(openapi_schema: dict[str, object]) -> list[str]:
    """Extract table names from OpenAPI schema.

    Args:
        openapi_schema: OpenAPI schema dictionary

    Returns:
        Sorted list of table names
    """
    tables: list[str] = []
    paths = openapi_schema.get("paths", {})
    if not isinstance(paths, dict):
        return tables

    for path in paths:
        if not isinstance(path, str):
            continue
        # Filter out non-table paths (RPC functions start with /rpc/)
        if path.startswith("/") and not path.startswith("/rpc/"):
            table_name = path.lstrip("/")
            # Only add if it's a simple table name (no slashes)
            if "/" not in table_name and table_name:
                tables.append(table_name)

    return sorted(tables)


def analyze_supabase_table_from_schema(
    project_url: str,
    api_key: str,
    table_name: str,
) -> tuple[list[FieldDefinition], SupabaseMetadata]:
    """Analyze Supabase table using OpenAPI schema definition.

    Reads actual PostgreSQL schema from PostgREST OpenAPI endpoint.
    Works with empty tables - no data sampling required.

    Args:
        project_url: Supabase project URL
        api_key: Supabase API key
        table_name: Table name to analyze

    Returns:
        Tuple of (field_definitions, metadata)

    Raises:
        ValueError: If table not found or not accessible
    """
    _validate_project_url(project_url)

    # Fetch OpenAPI schema
    openapi_schema = _fetch_openapi_schema(project_url, api_key)

    # Extract table schema from components.schemas
    components = openapi_schema.get("components", {})
    if not isinstance(components, dict):
        raise ValueError("Invalid OpenAPI schema: missing components")

    schemas = components.get("schemas", {})
    if not isinstance(schemas, dict):
        raise ValueError("Invalid OpenAPI schema: missing schemas")

    table_schema = schemas.get(table_name)
    if not table_schema or not isinstance(table_schema, dict):
        raise ValueError(
            f"Table '{table_name}' not found or not accessible. "
            f"Check that the table exists, is exposed through PostgREST, "
            f"and the API key has appropriate permissions."
        )

    # Parse properties and required fields
    properties = table_schema.get("properties", {})
    if not isinstance(properties, dict):
        raise ValueError(f"Invalid schema for table '{table_name}': missing properties")

    required = table_schema.get("required", [])
    if not isinstance(required, list):
        required = []

    # Convert to FieldDefinitions
    fields: list[FieldDefinition] = []
    nullable_columns: list[str] = []

    for field_name, field_spec in properties.items():
        if not isinstance(field_spec, dict):
            continue

        # Map OpenAPI type to contract type
        data_type = _map_openapi_type_to_contract(field_spec)

        # Nullable if not in required array
        nullable = field_name not in required
        if nullable:
            nullable_columns.append(field_name)

        # Build constraints
        constraints: list[FieldConstraint] = []
        if not nullable:
            constraints.append(FieldConstraint(type="not_null"))

        fields.append(
            FieldDefinition(
                name=field_name,
                data_type=data_type,
                nullable=nullable,
                constraints=constraints,
            )
        )

    # Build metadata
    column_details = [
        ColumnInfo(
            name=field.name,
            type=field.data_type,
            nullable=field.nullable,
            default=None,  # OpenAPI doesn't expose defaults
        )
        for field in fields
    ]

    metadata = SupabaseMetadata(
        project_url=project_url,
        table_name=table_name,
        primary_keys=[],  # OpenAPI doesn't expose PKs
        column_count=len(fields),
        nullable_columns=nullable_columns,
        sample_size=0,  # No sampling needed
        columns=column_details,
    )

    return fields, metadata


def list_supabase_tables(project_url: str, api_key: str) -> list[str]:
    """List all tables available in the Supabase project via PostgREST.

    Uses the PostgREST OpenAPI endpoint to discover available tables.

    Args:
        project_url: Supabase project URL (e.g., https://xxxxx.supabase.co)
        api_key: Supabase API key (anon or service_role key)

    Returns:
        List of table names accessible via PostgREST

    Raises:
        ValueError: If URL is invalid or connection fails

    Note:
        Only returns tables that are exposed through PostgREST and accessible
        with the provided API key. Tables may be hidden by:
        - Row Level Security (RLS) policies
        - PostgREST configuration (db-schemas)
        - PostgreSQL privileges
    """
    _validate_project_url(project_url)

    try:
        openapi_schema = _fetch_openapi_schema(project_url, api_key)
        return _extract_table_names(openapi_schema)
    except Exception as e:
        raise ValueError(f"Failed to list Supabase tables: {e}") from e


def validate_supabase_table_for_destination(
    project_url: str,
    api_key: str,
    table_name: str,
) -> SourceSchema:
    """Validate a Supabase table exists and return its schema for destination contract.

    This is a lighter version of analyze_supabase_table focused on validation
    for destination contracts. It verifies the table exists and is writable,
    and returns basic schema information.

    Args:
        project_url: Supabase project URL (e.g., https://xxxxx.supabase.co)
        api_key: Supabase API key (service_role key recommended for destinations)
        table_name: Table name to validate

    Returns:
        SourceSchema with field definitions inferred from sample data

    Raises:
        ValueError: If table is not found, connection fails, or URL is invalid

    Note:
        - Requires at least one row in the table to infer schema
        - Uses the same type inference as source contracts
        - Service role key recommended for full write access validation
    """
    _validate_project_url(project_url)

    try:
        # Create Supabase client
        supabase: Client = create_client(project_url, api_key)

        # Fetch schema by checking first row
        field_names, sample_rows, _ = _fetch_sample_data(supabase, table_name, sample_size=100)

        # Build field definitions
        fields, _ = _build_field_definitions(field_names, sample_rows)

        return SourceSchema(fields=fields)

    except Exception as e:
        raise _handle_supabase_error(e, table_name) from e
