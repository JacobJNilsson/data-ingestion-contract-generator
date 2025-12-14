"""Contract generation utilities for automated analysis

This module provides automated contract generation functionality.
It should only be used by the MCP server, not by ingestors directly.
"""

from pathlib import Path
from typing import Any

from core.models import (
    DestinationContract,
    DestinationSchema,
    ExecutionPlan,
    QualityMetrics,
    SourceContract,
    SourceSchema,
    TransformationContract,
)
from core.sources.csv import analyze_csv_file
from core.sources.json import analyze_json_file


def generate_source_analysis(source_path: str) -> dict[str, Any]:
    """Generate automated source data analysis

    Args:
        source_path: Path to source data file

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
        return analyze_json_file(source_file)

    # Default to CSV analysis
    return analyze_csv_file(source_file)


def generate_source_contract(
    source_id: str,
    source_path: str | None = None,
    config: dict[str, Any] | None = None,
    schema_file: str | None = None,
    endpoint: str | None = None,
    http_method: str | None = None,
) -> SourceContract:
    """Generate a source contract describing a data source

    Args:
        source_id: Unique identifier for this source (e.g., 'swedish_bank_csv')
        source_path: Path to source data file (for file-based sources)
        config: Optional configuration dictionary
        schema_file: Path to OpenAPI/Swagger schema file (for API sources)
        endpoint: API endpoint path from schema (required if schema_file provided)
        http_method: HTTP method for API (e.g., GET, POST)

    Returns:
        Source contract model
    """
    # Initialize metadata
    metadata = config.copy() if config else {}

    # Handle API sources
    if schema_file and endpoint:
        from core.sources.api import extract_response_schema, parse_openapi_schema

        try:
            schema_path = Path(schema_file)
            openapi_spec = parse_openapi_schema(schema_path)
            api_schema = extract_response_schema(
                openapi_spec,
                endpoint=endpoint,
                method=http_method or "GET",
            )

            # Store API metadata
            metadata["source_type"] = "api"
            metadata["endpoint"] = endpoint
            metadata["http_method"] = (http_method or "GET").upper()
            metadata["schema_file"] = str(schema_file)

            # Build source contract from API schema
            # Extract fields and types (which are lists)
            fields = api_schema.get("fields", [])
            types = api_schema.get("types", [])

            # Ensure they are lists (not dicts)
            if not isinstance(fields, list):
                fields = []
            if not isinstance(types, list):
                types = []

            return SourceContract(
                source_id=source_id,
                source_path=f"api:{endpoint}",
                file_format="api",
                encoding="utf-8",
                schema=SourceSchema(
                    fields=fields,
                    data_types=types,
                ),
                quality_metrics=QualityMetrics(
                    total_rows=0,
                    sample_data=[],
                    issues=[],
                ),
                metadata=metadata,
            )
        except Exception as e:
            raise ValueError(f"Failed to introspect API schema: {e}") from e

    # Handle file-based sources
    if not source_path:
        raise ValueError("Either source_path or both schema_file and endpoint must be provided")

    source_analysis = generate_source_analysis(source_path)

    return SourceContract(
        source_id=source_id,
        source_path=str(source_path),
        file_format=source_analysis.get("file_type", "unknown"),
        encoding=source_analysis.get("encoding", "utf-8"),
        delimiter=source_analysis.get("delimiter"),
        has_header=source_analysis.get("has_header", True),
        schema=SourceSchema(
            fields=source_analysis.get("sample_fields", []),
            data_types=source_analysis.get("data_types", []),
        ),
        quality_metrics=QualityMetrics(
            total_rows=source_analysis.get("total_rows", 0),
            sample_data=source_analysis.get("sample_data", []),
            issues=source_analysis.get("issues", []),
        ),
        metadata=metadata,
    )


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
            db_schema = inspect_table_schema(
                connection_string=connection_string,
                database_type=database_type,
                table_name=table_name,
                schema=database_schema,
            )
            # Merge with provided schema if any (provided schema takes precedence)
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
            api_schema = extract_endpoint_schema(
                openapi_spec,
                endpoint=endpoint,
                method=http_method or "POST",
            )

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

    # Parse schema if provided, otherwise use defaults
    if schema:
        dest_schema = DestinationSchema(
            fields=schema.get("fields", []),
            types=schema.get("types", []),
            constraints=schema.get("constraints", {}),
        )
    else:
        dest_schema = DestinationSchema()

    return DestinationContract(
        destination_id=destination_id,
        schema=dest_schema,
        metadata=metadata,
    )


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
