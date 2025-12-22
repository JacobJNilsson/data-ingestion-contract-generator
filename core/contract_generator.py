"""Contract generation utilities for automated analysis

This module provides automated contract generation functionality.
It should only be used by the MCP server, not by ingestors directly.
"""

from pathlib import Path
from typing import Any

from core.models import (
    CSVSourceContract,
    DestinationContract,
    DestinationSchema,
    ExecutionPlan,
    FieldDefinition,
    JSONSourceContract,
    QualityObservation,
    SourceAnalysisResult,
    SourceSchema,
    TransformationContract,
)
from core.sources.csv import analyze_csv_file
from core.sources.json import analyze_json_file


def generate_source_analysis(source_path: str, sample_size: int = 1000) -> SourceAnalysisResult:
    """Generate automated source data analysis

    Args:
        source_path: Path to source data file
        sample_size: Number of rows to sample for analysis (default: 1000)

    Returns:
        Dictionary with analysis results
    """
    source_file = Path(source_path)
    if not source_file.exists():
        msg = f"Source file not found: {source_path}"
        raise FileNotFoundError(msg)

    # Determine file type by extension
    suffix = source_file.suffix.lower()
    if suffix in [".json", ".jsonl", ".ndjson"]:
        return analyze_json_file(source_file, sample_size=sample_size)

    # Default to CSV analysis
    return analyze_csv_file(source_file, sample_size=sample_size)


def generate_source_contract(
    source_path: str, source_id: str | None = None, config: dict[str, Any] | None = None
) -> CSVSourceContract | JSONSourceContract:
    """Generate a source contract describing a data source

    Args:
        source_path: Path to source data file
        source_id: Unique identifier for this source (e.g., 'swedish_bank_csv').
                   If not provided, will be auto-generated from the file name.
        config: Optional configuration dictionary (can include 'sample_size')

    Returns:
        CSVSourceContract or JSONSourceContract based on file type
    """
    # Extract sample_size from config, default to 1000
    sample_size = config.get("sample_size", 1000) if config else 1000

    source_analysis = generate_source_analysis(source_path, sample_size=sample_size)

    # Auto-generate source_id from file name if not provided
    if source_id is None:
        source_file = Path(source_path)
        # Use stem (filename without extension) and sanitize it
        source_id = source_file.stem.lower().replace(" ", "_").replace("-", "_")

    # Create field definitions
    fields = []
    for name, dtype in zip(source_analysis.sample_fields, source_analysis.data_types, strict=True):
        profile = source_analysis.field_profiles.get(name)
        fields.append(FieldDefinition(name=name, data_type=dtype, profiling=profile))

    # Create the appropriate contract subtype based on file type
    schema = SourceSchema(fields=fields)
    quality = QualityObservation(
        total_rows=source_analysis.total_rows,
        observed_profiling=source_analysis.field_profiles,
        sample_data=source_analysis.sample_data,
        issues=source_analysis.issues,
    )
    metadata = config or {}

    if source_analysis.file_type == "csv":
        return CSVSourceContract(
            source_id=source_id,
            source_path=str(source_path),
            encoding=source_analysis.encoding,
            delimiter=source_analysis.delimiter or ",",
            has_header=source_analysis.has_header if source_analysis.has_header is not None else True,
            schema=schema,
            quality=quality,
            metadata=metadata,
        )
    elif source_analysis.file_type == "json":
        # Determine if NDJSON based on file extension
        is_ndjson = Path(source_path).suffix.lower() in [".jsonl", ".ndjson"]
        return JSONSourceContract(
            source_id=source_id,
            source_path=str(source_path),
            encoding=source_analysis.encoding,
            is_ndjson=is_ndjson,
            schema=schema,
            quality=quality,
            metadata=metadata,
        )
    else:
        msg = f"Unsupported file type: {source_analysis.file_type}"
        raise ValueError(msg)


def generate_destination_contract(
    destination_id: str,
    schema: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
    connection_string: str | None = None,
    table_name: str | None = None,
    database_type: str | None = None,
    database_schema: str | None = None,
    schema_file: str | None = None,
    endpoint: str | None = None,
    http_method: str | None = None,
) -> DestinationContract:
    """Generate a destination contract describing a data destination

    Args:
        destination_id: Unique identifier for destination (e.g., 'dwh_transactions_table')
        schema: Schema definition with fields and types
        config: Optional configuration dictionary
        connection_string: Database connection string (optional)
        table_name: Database table name (optional)
        database_type: Database type - postgresql, mysql, or sqlite (required if connection_string provided)
        database_schema: Database schema name (optional, for databases that support schemas)
        schema_file: Path to OpenAPI/Swagger schema file (optional, for API destinations)
        endpoint: API endpoint path from schema (optional, required if schema_file provided)
        http_method: HTTP method for API (optional, e.g., POST, PUT, PATCH)

    Returns:
        Destination contract model
    """
    # Initialize metadata
    metadata = config.copy() if config else {}

    # If database info is provided, inspect the table
    if connection_string and table_name:
        if not database_type:
            raise ValueError("database_type is required when connection_string is provided")

        from core.sources.database import inspect_table_schema

        try:
            db_schema_info = inspect_table_schema(
                connection_string=connection_string,
                database_type=database_type,
                table_name=table_name,
                schema=database_schema,
            )
            # Convert to dict and merge with provided schema if any (provided schema takes precedence)
            db_schema = db_schema_info.model_dump(exclude_none=True)
            if schema:
                db_schema.update(schema)
            schema = db_schema
        except Exception as e:
            # If inspection fails, we might still want to proceed if a schema was manually provided
            # otherwise we re-raise
            if not schema:
                raise ValueError(f"Failed to inspect database table: {e}") from e

    # If API schema file is provided, introspect the schema
    if schema_file and endpoint:
        from pathlib import Path

        from core.sources.api import extract_endpoint_schema, parse_openapi_schema

        try:
            schema_path = Path(schema_file)
            openapi_spec = parse_openapi_schema(schema_path)
            api_schema_info = extract_endpoint_schema(
                openapi_spec,
                endpoint=endpoint,
                method=http_method or "POST",
            )
            api_schema = api_schema_info.model_dump(exclude_none=True)

            # Store API metadata
            metadata["destination_type"] = "api"
            metadata["endpoint"] = endpoint
            metadata["http_method"] = (http_method or "POST").upper()
            metadata["schema_file"] = str(schema_file)

            # Merge with provided schema if any (provided schema takes precedence)
            if schema:
                api_schema.update(schema)
            schema = api_schema
        except Exception as e:
            # If introspection fails, we might still want to proceed if a schema was manually provided
            # otherwise we re-raise
            if not schema:
                raise ValueError(f"Failed to introspect API schema: {e}") from e

    # Parse schema if provided
    dest_schema = DestinationSchema(fields=_parse_destination_fields(schema)) if schema else DestinationSchema()

    return DestinationContract(
        destination_id=destination_id,
        schema=dest_schema,
        metadata=metadata,
    )


def _create_destination_field(idx: int, field: Any, schema_types: list[str]) -> FieldDefinition:
    """Create a FieldDefinition from various input formats."""
    if isinstance(field, str):
        # Fallback to "string" only if schema_types was not provided or is shorter
        dtype = schema_types[idx] if idx < len(schema_types) else "string"
        return FieldDefinition(name=field, data_type=dtype)
    if isinstance(field, dict):
        return FieldDefinition(**field)
    if isinstance(field, FieldDefinition):
        return field
    return field  # type: ignore


def _parse_destination_fields(schema: dict[str, Any]) -> list[FieldDefinition]:
    """Parse legacy and new style destination fields into FieldDefinition objects."""
    schema_fields = schema.get("fields", [])
    schema_types = schema.get("types", [])

    # If types is provided, check for length mismatch
    mismatch = schema_types and (len(schema_types) != len(schema_fields))
    if mismatch and all(isinstance(f, str) for f in schema_fields):
        raise ValueError(
            f"Schema mismatch: 'fields' has {len(schema_fields)} names but 'types' has {len(schema_types)} types."
        )

    return [_create_destination_field(i, f, schema_types) for i, f in enumerate(schema_fields)]


def generate_transformation_contract(
    transformation_id: str,
    source_ref: str,
    destination_ref: str,
    config: dict[str, Any] | None = None,
) -> TransformationContract:
    """Generate a transformation contract mapping source to destination

    Args:
        transformation_id: Unique identifier for this transformation
        source_ref: Reference to source contract ID
        destination_ref: Reference to destination contract ID
        config: Optional configuration dictionary

    Returns:
        Transformation contract model
    """
    # Build execution plan from config
    exec_plan = ExecutionPlan(
        batch_size=config.get("batch_size", 100) if config else 100,
        error_threshold=config.get("error_threshold", 0.1) if config else 0.1,
    )

    return TransformationContract(
        transformation_id=transformation_id,
        source_ref=source_ref,
        destination_ref=destination_ref,
        execution_plan=exec_plan,
        metadata=config or {},
    )
