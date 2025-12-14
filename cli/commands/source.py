"""Source contract generation commands."""

from pathlib import Path

import typer

from cli.output import error_message, output_contract
from core.contract_generator import generate_source_contract
from core.sources.database.introspection import extract_table_list

app = typer.Typer(help="Generate source contracts from data sources")
database_app = typer.Typer(help="Generate source contracts from databases")
app.add_typer(database_app, name="database")
api_app = typer.Typer(help="Generate source contracts from API schemas")
app.add_typer(api_app, name="api")


@app.command("csv")
def source_csv(
    path: Path = typer.Argument(..., help="Path to CSV file", exists=True, dir_okay=False, resolve_path=True),
    source_id: str = typer.Option(..., "--id", help="Unique identifier for this source"),
    output: Path | None = typer.Option(
        None, "--output", "-o", help="Output file path (default: stdout)", dir_okay=False, resolve_path=True
    ),
    output_format: str | None = typer.Option(None, "--format", "-f", help="Output format: json or yaml"),
    delimiter: str | None = typer.Option(None, "--delimiter", help="CSV delimiter (default: auto-detect)"),
    encoding: str | None = typer.Option(None, "--encoding", help="File encoding (default: auto-detect)"),
    sample_size: int | None = typer.Option(None, "--sample-size", help="Number of rows to sample for analysis"),
    pretty: bool | None = typer.Option(None, "--pretty", help="Pretty-print JSON output"),
) -> None:
    """Generate source contract from CSV file.

    Example:
        contract-gen source csv data/transactions.csv --id transactions --output contracts/source.json --pretty
    """
    try:
        # Load config for defaults
        from cli.config import get_csv_defaults, get_output_defaults

        csv_defaults = get_csv_defaults()
        output_defaults = get_output_defaults()

        # Apply defaults from config if not specified via CLI
        if delimiter is None:
            delimiter = csv_defaults.delimiter
        if encoding is None:
            encoding = csv_defaults.encoding
        if sample_size is None:
            sample_size = csv_defaults.sample_size
        if output_format is None:
            output_format = output_defaults.format
        if pretty is None:
            pretty = output_defaults.pretty

        # Build config
        config_dict: dict[str, str | int] = {"sample_size": sample_size}
        if delimiter:
            config_dict["delimiter"] = delimiter
        if encoding:
            config_dict["encoding"] = encoding

        # Generate contract
        contract = generate_source_contract(source_id=source_id, source_path=str(path.absolute()), config=config_dict)

        # Output
        contract_json = contract.model_dump_json(by_alias=True)
        output_contract(contract_json, output_path=output, output_format=output_format, pretty=pretty)

    except FileNotFoundError as e:
        error_message(f"File not found: {path}", hint="Check the file path and try again")
        raise typer.Exit(1) from e
    except ValueError as e:
        error_message(str(e), hint="Check the file format and parameters")
        raise typer.Exit(1) from e
    except Exception as e:
        error_message(f"Failed to generate source contract: {e}")
        raise typer.Exit(1) from e


@app.command("json")
def source_json(
    path: Path = typer.Argument(..., help="Path to JSON/NDJSON file", exists=True, dir_okay=False, resolve_path=True),
    source_id: str = typer.Option(..., "--id", help="Unique identifier for this source"),
    output: Path | None = typer.Option(
        None, "--output", "-o", help="Output file path (default: stdout)", dir_okay=False, resolve_path=True
    ),
    output_format: str | None = typer.Option(None, "--format", "-f", help="Output format: json or yaml"),
    encoding: str | None = typer.Option(None, "--encoding", help="File encoding (default: auto-detect)"),
    sample_size: int | None = typer.Option(None, "--sample-size", help="Number of rows to sample for analysis"),
    pretty: bool | None = typer.Option(None, "--pretty", help="Pretty-print JSON output"),
) -> None:
    """Generate source contract from JSON or NDJSON file.

    Example:
        contract-gen source json data/users.json --id users --output contracts/source.json
    """
    try:
        # Load config for defaults
        from cli.config import get_json_defaults, get_output_defaults

        json_defaults = get_json_defaults()
        output_defaults = get_output_defaults()

        # Apply defaults from config if not specified via CLI
        if encoding is None:
            encoding = json_defaults.encoding
        if sample_size is None:
            sample_size = json_defaults.sample_size
        if output_format is None:
            output_format = output_defaults.format
        if pretty is None:
            pretty = output_defaults.pretty

        # Build config
        config_dict: dict[str, str | int] = {"sample_size": sample_size}
        if encoding:
            config_dict["encoding"] = encoding

        # Generate contract
        contract = generate_source_contract(source_id=source_id, source_path=str(path.absolute()), config=config_dict)

        # Output
        contract_json = contract.model_dump_json(by_alias=True)
        output_contract(contract_json, output_path=output, output_format=output_format, pretty=pretty)

    except FileNotFoundError as e:
        error_message(f"File not found: {path}", hint="Check the file path and try again")
        raise typer.Exit(1) from e
    except ValueError as e:
        error_message(str(e), hint="Check the file format and parameters")
        raise typer.Exit(1) from e
    except Exception as e:
        error_message(f"Failed to generate source contract: {e}")
        raise typer.Exit(1) from e


@api_app.command("generate")
def source_api(
    schema_file: Path = typer.Argument(..., exists=True, help="OpenAPI/Swagger schema file (JSON or YAML)"),
    endpoint: str = typer.Argument(..., help="API endpoint path (e.g. /users, /data)"),
    source_id: str = typer.Option(..., "--id", help="Unique identifier for this source"),
    method: str = typer.Option("GET", "--method", help="HTTP method (GET, POST, PUT, PATCH, DELETE)"),
    output: Path | None = typer.Option(
        None, "--output", "-o", help="Output file path (default: stdout)", dir_okay=False, resolve_path=True
    ),
    output_format: str | None = typer.Option(None, "--format", "-f", help="Output format: json or yaml"),
    pretty: bool | None = typer.Option(None, "--pretty", help="Pretty-print JSON output"),
) -> None:
    """Generate source contract from an OpenAPI/Swagger schema file.

    Example:
        contract-gen source api generate openapi.json /users --id users_api --method GET --pretty
    """
    try:
        # Load config for defaults
        from cli.config import get_output_defaults

        output_defaults = get_output_defaults()

        # Apply defaults from config if not specified via CLI
        if output_format is None:
            output_format = output_defaults.format
        if pretty is None:
            pretty = output_defaults.pretty

        # Generate contract
        contract = generate_source_contract(
            source_id=source_id,
            schema_file=str(schema_file),
            endpoint=endpoint,
            http_method=method.upper(),
        )

        # Output
        contract_json = contract.model_dump_json(by_alias=True)
        output_contract(contract_json, output_path=output, output_format=output_format, pretty=pretty)

    except ValueError as e:
        error_message(str(e), hint="Check your OpenAPI schema file and endpoint path")
        raise typer.Exit(1) from e
    except Exception as e:
        error_message(f"Failed to generate source contract: {e}")
        raise typer.Exit(1) from e


def _format_endpoint_text(ep: dict, with_fields: bool) -> list[str]:
    """Format a single endpoint for text output.

    Args:
        ep: Endpoint dictionary with method, path, fields, types, etc.
        with_fields: Whether field details should be included

    Returns:
        List of formatted lines for this endpoint
    """
    lines = [f"  {ep['method']:<6} {ep['path']}"]

    if not with_fields:
        return lines

    if "fields" in ep:
        fields = ep.get("fields", [])
        types = ep.get("types", [])

        if fields:
            lines.append("    Fields:")
            for i, field in enumerate(fields):
                field_type = types[i] if i < len(types) else "unknown"
                lines.append(f"      - {field} ({field_type})")
        else:
            lines.append("    (No fields)")
        lines.append("")
    elif "error" in ep:
        lines.append(f"    Error: {ep['error']}")
        lines.append("")
    else:
        lines.append("    (No fields info)")
        lines.append("")

    return lines


@api_app.command("list")
def list_api_responses(
    schema_file: Path = typer.Argument(..., exists=True, help="OpenAPI/Swagger schema file (JSON or YAML)"),
    with_fields: bool = typer.Option(False, "--with-fields", help="Include response body field details"),
    method: str | None = typer.Option(None, "--method", help="Filter by HTTP method"),
    output_format: str = typer.Option("text", "--format", "-f", help="Output format: text or json"),
) -> None:
    """List available API response endpoints in an OpenAPI specification.

    Example:
        contract-gen source api list openapi.yaml --method GET
    """
    try:
        import json

        from core.sources.api.introspection import extract_response_schema
        from core.sources.api.parser import parse_openapi_schema

        # Load schema using typed parser
        spec = parse_openapi_schema(schema_file)

        # Build endpoint list
        endpoints: list[dict[str, str | list[str] | dict[str, list[str]]]] = []

        if not spec.paths:
            if output_format == "json":
                typer.echo(json.dumps([], indent=2))
            else:
                typer.echo("No endpoints found.")
            return

        for path, path_item in spec.paths.items():
            if not path.startswith("/"):
                continue

            # Check all HTTP methods
            for method_lower in ["get", "post", "put", "patch", "delete", "head", "options", "trace"]:
                operation = getattr(path_item, method_lower, None)
                if not operation:
                    continue

                op_method = method_lower.upper()

                # Apply method filter if specified
                if method and op_method != method.upper():
                    continue

                endpoint_info: dict[str, str | list[str] | dict[str, list[str]]] = {
                    "method": op_method,
                    "path": path,
                    "summary": operation.summary or "",
                }

                if with_fields:
                    try:
                        schema = extract_response_schema(spec, path, op_method)
                        endpoint_info["fields"] = schema["fields"]
                        endpoint_info["types"] = schema["types"]
                        endpoint_info["constraints"] = schema.get("constraints", {})
                    except Exception:
                        endpoint_info["error"] = "Failed to extract response schema"

                endpoints.append(endpoint_info)

        if output_format == "json":
            typer.echo(json.dumps(endpoints, indent=2))
            return

        if not endpoints:
            typer.echo("No endpoints found.")
            return

        typer.echo(f"Endpoints ({len(endpoints)} total):")
        for ep in endpoints:
            for line in _format_endpoint_text(ep, with_fields):
                typer.echo(line)

    except Exception as e:
        error_message(f"Failed to list endpoints: {e}")
        raise typer.Exit(1) from e


def _format_table_text(table: dict, with_fields: bool) -> list[str]:
    """Format a single table for text output.

    Args:
        table: Table dictionary with name, columns, column_count, etc.
        with_fields: Whether column details should be included

    Returns:
        List of formatted lines for this table
    """
    col_count = table.get("column_count", 0)
    lines = [f"  {table['name']} ({col_count} columns)"]

    if not with_fields:
        return lines

    if "columns" in table:
        for col in table["columns"]:
            nullable = ", NOT NULL" if not col["nullable"] else ""
            lines.append(f"    - {col['name']} ({col['type']}{nullable})")
        lines.append("")
    elif "error" in table:
        lines.append(f"    Error: {table['error']}")
        lines.append("")

    return lines


@database_app.command("list")
def source_database_list(
    connection_string: str = typer.Argument(
        ..., help="Database connection string (e.g. postgresql://user:pass@host/db)"
    ),
    database_type: str = typer.Option(..., "--type", help="Database type: postgresql, mysql, or sqlite"),
    schema: str | None = typer.Option(
        None, "--schema", help="Database schema name (optional, defaults to 'public' for PostgreSQL)"
    ),
    with_fields: bool = typer.Option(False, "--with-fields", help="Include column details"),
    output_format: str = typer.Option("text", "--format", "-f", help="Output format: text or json"),
) -> None:
    """List tables in a database.

    Example:
        contract-gen source database list postgresql://user:pass@host/db --type postgresql
    """
    try:
        tables = extract_table_list(
            connection_string=connection_string,
            database_type=database_type,
            schema=schema,
            with_fields=with_fields,
        )

        if output_format == "json":
            import json

            typer.echo(json.dumps(tables, indent=2))
            return

        if not tables:
            typer.echo("No tables found.")
            return

        schema_msg = f" in schema '{schema}'" if schema else ""
        typer.echo(f"Tables{schema_msg} ({len(tables)} total):")

        for table in tables:
            for line in _format_table_text(table, with_fields):
                typer.echo(line)

    except Exception as e:
        error_message(f"Failed to list tables: {e}")
        raise typer.Exit(1) from e
