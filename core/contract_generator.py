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
    source_type: str = "unknown",
    source_path: str | None = None,
    config: dict[str, Any] | None = None,
    connection_string: str | None = None,
    table_name: str | None = None,
    schema_file: str | None = None,
    endpoint: str | None = None,
    http_method: str = "GET",
) -> SourceContract:
    """
    Generate a source contract for a given source.

    Args:
        source_id: Unique identifier for the source
        source_type: Type of source (e.g. "postgresql", "api", "csv", "json")
        source_path: Path to source file (for CSV/JSON)
        config: Configuration dictionary (for CSV/JSON)
        connection_string: Connection string for database sources
        table_name: Table name for database sources
        schema_file: Path to OpenAPI/Swagger schema file for API sources
        endpoint: API endpoint path for API sources
        http_method: HTTP method for API sources (default: GET)
    """
    schema = SourceSchema(fields=[], data_types=[])
    metadata: dict[str, Any] = {"source_type": source_type}
    quality_metrics = QualityMetrics(
        total_rows=0,
        sample_data=[],
        issues=[],
    )

    if source_type == "api":
        if not schema_file or not endpoint:
            raise ValueError("schema_file and endpoint are required for API sources")

        from pathlib import Path

        from core.sources.api import extract_response_schema, parse_openapi_schema

        openapi_spec = parse_openapi_schema(Path(schema_file))
        api_schema = extract_response_schema(openapi_spec, endpoint, http_method)

        schema = SourceSchema(
            fields=list(api_schema["fields"]),
            data_types=list(api_schema["types"]),
        )
        metadata.update(
            {
                "endpoint": endpoint,
                "http_method": http_method,
                "schema_file": schema_file,
            }
        )

    elif connection_string and table_name:
        from core.sources.database.introspection import analyze_database_table

        analysis = analyze_database_table(connection_string, table_name, source_type)
        schema = SourceSchema(
            fields=list(analysis["fields"]),  # type: ignore[arg-type,call-overload]
            data_types=list(analysis["types"]),  # type: ignore[arg-type,call-overload]
        )
        metadata.update(
            {
                "connection_string": connection_string,
                "table_name": table_name,
            }
        )

    elif source_path:
        # CSV / JSON file analysis
        source_analysis = generate_source_analysis(source_path)

        schema = SourceSchema(
            fields=source_analysis.get("sample_fields", []),
            data_types=source_analysis.get("data_types", []),
        )

        quality_metrics = QualityMetrics(
            total_rows=source_analysis.get("total_rows", 0),
            sample_data=source_analysis.get("sample_data", []),
            issues=source_analysis.get("issues", []),
        )

        metadata.update(config or {})
        metadata.update(
            {
                "file_format": source_analysis.get("file_type", "unknown"),
                "encoding": source_analysis.get("encoding", "utf-8"),
                "delimiter": source_analysis.get("delimiter"),
                "has_header": source_analysis.get("has_header", True),
                "source_path": source_path,
            }
        )

    # Extract top-level fields from metadata or analysis
    contract_fields: dict[str, Any] = {
        "source_id": source_id,
        "schema": schema,
        "quality_metrics": quality_metrics,
        "metadata": metadata,
    }

    if source_type == "api":
        # API specific fields are in metadata, nothing top-level to map except maybe source_type
        pass
    elif connection_string:
        contract_fields.update(
            {
                # Actually source_type argument is 'postgresql', 'mysql' etc.
                "database_type": source_type if source_type in ["postgresql", "mysql", "sqlite"] else None,
                "source_type": "table",  # Defaulting to table for now
                "source_name": table_name,
            }
        )
    elif source_path and "file_format" in metadata:
        contract_fields.update(
            {
                "source_path": source_path,
                "file_format": metadata.get("file_format"),
                "encoding": metadata.get("encoding"),
                "delimiter": metadata.get("delimiter"),
                "has_header": metadata.get("has_header"),
            }
        )

    return SourceContract(**contract_fields)


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
