"""Destination contract generation commands."""

from pathlib import Path

import typer

from cli.output import error_message, handle_permission_error, output_contract
from core.contract_generator import generate_destination_contract, generate_supabase_destination_contract

app = typer.Typer(help="Generate destination contracts")


@app.command("csv")
def destination_csv(
    destination_id: str = typer.Option(..., "--id", help="Unique identifier for this destination"),
    output: Path | None = typer.Option(
        None, "--output", "-o", help="Output file path (default: stdout)", dir_okay=False, resolve_path=True
    ),
    output_format: str = typer.Option("json", "--format", "-f", help="Output format: json or yaml"),
    pretty: bool = typer.Option(False, "--pretty", help="Pretty-print JSON output"),
) -> None:
    """Generate destination contract for CSV output.

    Example:
        contract-gen destination csv --id output_transactions --output contracts/destination.json --pretty
    """
    try:
        # Generate contract
        contract = generate_destination_contract(destination_id=destination_id, config=None)

        # Output
        contract_json = contract.model_dump_json(by_alias=True, exclude_none=True)
        output_contract(contract_json, output_path=output, output_format=output_format, pretty=pretty)

    except ValueError as e:
        error_message(str(e), hint="Check the parameters")
        raise typer.Exit(1) from e
    except Exception as e:
        error_message(f"Failed to generate destination contract: {e}")
        raise typer.Exit(1) from e


@app.command("database")
def destination_database(
    connection_string: str = typer.Option(
        ..., "--conn", help="Database connection string (e.g. postgresql://user:pass@host/db)"
    ),
    table_name: str = typer.Option(..., "--table", help="Table name to inspect"),
    destination_id: str = typer.Option(..., "--id", help="Unique identifier for this destination"),
    database_type: str = typer.Option(..., "--type", help="Database type: postgresql, mysql, or sqlite"),
    database_schema: str | None = typer.Option(
        None, "--schema", help="Database schema name (optional, defaults to 'public' for PostgreSQL)"
    ),
    output: Path | None = typer.Option(
        None, "--output", "-o", help="Output file path (default: stdout)", dir_okay=False, resolve_path=True
    ),
    output_format: str | None = typer.Option(None, "--format", "-f", help="Output format: json or yaml"),
    pretty: bool | None = typer.Option(None, "--pretty", help="Pretty-print JSON output"),
) -> None:
    """Generate destination contract from a database table.

    Example:
        contract-gen destination database --conn postgresql://user:pass@localhost/db --table my_table --id my_dest --type postgresql
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
        contract = generate_destination_contract(
            destination_id=destination_id,
            connection_string=connection_string,
            table_name=table_name,
            database_type=database_type,
            database_schema=database_schema,
        )

        # Output
        contract_json = contract.model_dump_json(by_alias=True, exclude_none=True)
        output_contract(contract_json, output_path=output, output_format=output_format, pretty=pretty)

    except ValueError as e:
        error_message(str(e), hint="Check your connection string and table name")
        raise typer.Exit(1) from e
    except Exception as e:
        error_message(f"Failed to generate destination contract: {e}")
        raise typer.Exit(1) from e


@app.command("supabase")
def destination_supabase(
    project_url: str = typer.Option(..., "--url", help="Supabase project URL (e.g., https://xxxxx.supabase.co)"),
    api_key: str = typer.Option(
        ..., "--api-key", help="Supabase API key (service_role key recommended for write access)"
    ),
    table_name: str = typer.Option(..., "--table", help="Table name to use as destination"),
    destination_id: str = typer.Option(..., "--id", help="Unique identifier for this destination"),
    output: Path | None = typer.Option(
        None, "--output", "-o", help="Output file path (default: stdout)", dir_okay=False, resolve_path=True
    ),
    output_format: str | None = typer.Option(None, "--format", "-f", help="Output format: json or yaml"),
    pretty: bool | None = typer.Option(None, "--pretty", help="Pretty-print JSON output"),
) -> None:
    """Generate destination contract for Supabase table.

    Uses Supabase's PostgREST API to validate table access and infer schema.

    NOTES:
    - Service role key recommended for full write access validation
    - Schema inferred from sample data (requires at least one row in table)
    - For full schema introspection with primary keys, use database destination instead

    Example:
        contract-gen destination supabase --url https://xxxxx.supabase.co --api-key eyJhbGc... --table users --id dest_users --output contracts/dest.json
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
        contract = generate_supabase_destination_contract(
            destination_id=destination_id,
            project_url=project_url,
            api_key=api_key,
            table_name=table_name,
            config=None,
        )

        # Output
        contract_json = contract.model_dump_json(by_alias=True, exclude_none=True)
        output_contract(contract_json, output_path=output, output_format=output_format, pretty=pretty)

    except ValueError as e:
        error_message(str(e), hint="Check the project URL, API key, and table name")
        raise typer.Exit(1) from e
    except Exception as e:
        error_message(f"Failed to generate destination contract: {e}")
        raise typer.Exit(1) from e


# Create API group
api_app = typer.Typer(help="Generate destination contracts from API schemas")
app.add_typer(api_app, name="api")


@api_app.command("generate")
def generate_api_contract(
    schema_file: Path = typer.Argument(..., help="OpenAPI/Swagger schema file (JSON or YAML)", readable=False),
    endpoint: str = typer.Argument(..., help="API endpoint path (e.g. /users, /data)"),
    destination_id: str = typer.Option(..., "--id", help="Unique identifier for this destination"),
    method: str = typer.Option("POST", "--method", help="HTTP method (GET, POST, PUT, PATCH, DELETE)"),
    output: Path | None = typer.Option(
        None, "--output", "-o", help="Output file path (default: stdout)", dir_okay=False, resolve_path=True
    ),
    output_format: str | None = typer.Option(None, "--format", "-f", help="Output format: json or yaml"),
    pretty: bool | None = typer.Option(None, "--pretty", help="Pretty-print JSON output"),
) -> None:
    """Generate destination contract from an OpenAPI/Swagger schema file.

    Example:
        contract-gen destination api generate openapi.json /users --id users_api --method POST --pretty
    """
    try:
        # Check if file exists manually to handle macOS permission issues
        if not schema_file.exists():
            error_message(f"File not found: {schema_file}", hint="Check the file path and try again")
            raise typer.Exit(1)

        # Load config for defaults
        from cli.config import get_output_defaults

        output_defaults = get_output_defaults()

        # Apply defaults from config if not specified via CLI
        if output_format is None:
            output_format = output_defaults.format
        if pretty is None:
            pretty = output_defaults.pretty

        # Generate contract
        contract = generate_destination_contract(
            destination_id=destination_id,
            schema_file=str(schema_file),
            endpoint=endpoint,
            http_method=method.upper(),
        )

        # Output
        contract_json = contract.model_dump_json(by_alias=True, exclude_none=True)
        output_contract(contract_json, output_path=output, output_format=output_format, pretty=pretty)

    except ValueError as e:
        error_message(str(e), hint="Check your OpenAPI schema file and endpoint path")
        raise typer.Exit(1) from e
    except PermissionError as e:
        handle_permission_error(schema_file, e)
        raise typer.Exit(1) from e
    except Exception as e:
        error_message(f"Failed to generate destination contract: {e}")
        raise typer.Exit(1) from e


def _format_endpoint_text(ep: dict, with_fields: bool) -> list[str]:
    """Format a single endpoint for text output.

    Args:
        ep: Endpoint dictionary with method, path, fields, constraints, etc.
        with_fields: Whether field details should be included

    Returns:
        List of formatted lines for this endpoint
    """
    lines = [f"  {ep['method']:<6} {ep['path']}"]

    if not with_fields:
        return lines

    if "fields" in ep:
        fields = ep.get("fields", [])
        constraints = ep.get("constraints", {})

        if fields:
            lines.append("    Fields:")
            for field in fields:
                req = " (Required)" if "REQUIRED" in constraints.get(field, []) else ""
                lines.append(f"      - {field}{req}")
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
def list_api_endpoints(
    schema_file: Path = typer.Argument(..., help="OpenAPI/Swagger schema file (JSON or YAML)", readable=False),
    with_fields: bool = typer.Option(False, "--with-fields", help="Include request body field details"),
    method: str | None = typer.Option(None, "--method", help="Filter by HTTP method"),
    output_format: str = typer.Option("text", "--format", "-f", help="Output format: text or json"),
) -> None:
    """List available endpoints in an OpenAPI specification.

    Example:
        contract-gen destination api list openapi.yaml --method POST
    """
    try:
        # Check if file exists manually to handle macOS permission issues
        if not schema_file.exists():
            error_message(f"File not found: {schema_file}", hint="Check the file path and try again")
            raise typer.Exit(1)

        import json

        from core.sources.api.introspection import extract_endpoint_list
        from core.sources.api.parser import parse_openapi_schema

        # Load schema using typed parser
        spec = parse_openapi_schema(schema_file)

        endpoints = extract_endpoint_list(spec, with_fields=with_fields, method=method)

        # Convert to dicts for output handling
        endpoints_dicts = [ep.model_dump(exclude_none=True, by_alias=True) for ep in endpoints]

        if output_format == "json":
            typer.echo(json.dumps(endpoints_dicts, indent=2))
            return

        if not endpoints:
            typer.echo("No endpoints found.")
            return

        typer.echo(f"Endpoints ({len(endpoints)} total):")
        for ep in endpoints_dicts:
            for line in _format_endpoint_text(ep, with_fields):
                typer.echo(line)

    except PermissionError as e:
        handle_permission_error(schema_file, e)
        raise typer.Exit(1) from e
    except Exception as e:
        error_message(f"Failed to list endpoints: {e}")
        raise typer.Exit(1) from e
