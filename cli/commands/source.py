"""Source contract generation commands."""

from pathlib import Path

import typer

from cli.output import error_message, handle_permission_error, output_contract
from core.contract_generator import generate_source_contract
from core.models import TableInfo
from core.sources.database.relationships import list_database_tables

app = typer.Typer(help="Generate source contracts from data sources")
database_app = typer.Typer(help="Generate source contracts from databases")
app.add_typer(database_app, name="database")


@app.command("csv")
def source_csv(
    path: Path = typer.Argument(..., help="Path to CSV file", dir_okay=False, resolve_path=True, readable=False),
    source_id: str | None = typer.Option(
        None, "--id", help="Unique identifier for this source (default: derived from file name)"
    ),
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
        contract-gen source csv data/transactions.csv --output contracts/source.json --pretty
        contract-gen source csv data/transactions.csv --id transactions --output contracts/source.json --pretty
    """
    try:
        # Check if file exists manually to handle macOS permission issues
        if not path.exists():
            error_message(f"File not found: {path}", hint="Check the file path and try again")
            raise typer.Exit(1)

        # Load config for defaults
        from cli.config import get_csv_defaults, get_output_defaults

        csv_defaults = get_csv_defaults()
        output_defaults = get_output_defaults()

        # Apply defaults from config for sample_size and output settings
        # Note: delimiter and encoding are auto-detected, so we don't apply config defaults for them
        if sample_size is None:
            sample_size = csv_defaults.sample_size
        if output_format is None:
            output_format = output_defaults.format
        if pretty is None:
            pretty = output_defaults.pretty

        # Build config dict with sample_size for analysis
        # delimiter and encoding will be auto-detected by the analyzer
        config_dict: dict[str, str | int] = {"sample_size": sample_size}

        # If user explicitly provided delimiter or encoding, add them to config as overrides
        # These will be used by the analyzer instead of auto-detection
        if delimiter is not None:
            config_dict["delimiter"] = delimiter
        if encoding is not None:
            config_dict["encoding"] = encoding

        # Generate contract
        contract = generate_source_contract(source_path=str(path.absolute()), source_id=source_id, config=config_dict)

        # Output
        contract_json = contract.model_dump_json(by_alias=True, exclude_none=True)
        output_contract(contract_json, output_path=output, output_format=output_format, pretty=pretty)

    except FileNotFoundError:
        # This catch is mainly for errors that might occur during processing
        # path.exists() check above handles the initial case
        error_message(f"File not found: {path}", hint="Check the file path and try again")
        raise typer.Exit(1) from None
    except ValueError as e:
        error_message(str(e), hint="Check the file format and parameters")
        raise typer.Exit(1) from e
    except PermissionError as e:
        handle_permission_error(path, e)
        raise typer.Exit(1) from e
    except Exception as e:
        error_message(f"Failed to generate source contract: {e}")
        raise typer.Exit(1) from e


@app.command("json")
def source_json(
    path: Path = typer.Argument(
        ..., help="Path to JSON/NDJSON file", dir_okay=False, resolve_path=True, readable=False
    ),
    source_id: str | None = typer.Option(
        None, "--id", help="Unique identifier for this source (default: derived from file name)"
    ),
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
        contract-gen source json data/users.json --output contracts/source.json
        contract-gen source json data/users.json --id users --output contracts/source.json
    """
    try:
        # Check if file exists manually to handle macOS permission issues
        if not path.exists():
            error_message(f"File not found: {path}", hint="Check the file path and try again")
            raise typer.Exit(1)

        # Load config for defaults
        from cli.config import get_json_defaults, get_output_defaults

        json_defaults = get_json_defaults()
        output_defaults = get_output_defaults()

        # Apply defaults from config for sample_size and output settings
        # Note: encoding is auto-detected, so we don't apply config defaults for it
        if sample_size is None:
            sample_size = json_defaults.sample_size
        if output_format is None:
            output_format = output_defaults.format
        if pretty is None:
            pretty = output_defaults.pretty

        # Build config dict with sample_size for analysis
        # encoding will be auto-detected by the analyzer
        config_dict: dict[str, str | int] = {"sample_size": sample_size}

        # If user explicitly provided encoding, add it to config as an override
        # This will be used by the analyzer instead of auto-detection
        if encoding is not None:
            config_dict["encoding"] = encoding

        # Generate contract
        contract = generate_source_contract(source_path=str(path.absolute()), source_id=source_id, config=config_dict)

        # Output
        contract_json = contract.model_dump_json(by_alias=True, exclude_none=True)
        output_contract(contract_json, output_path=output, output_format=output_format, pretty=pretty)

    except FileNotFoundError:
        # This catch is mainly for errors that might occur during processing
        # path.exists() check above handles the initial case
        error_message(f"File not found: {path}", hint="Check the file path and try again")
        raise typer.Exit(1) from None
    except ValueError as e:
        error_message(str(e), hint="Check the file format and parameters")
        raise typer.Exit(1) from e
    except PermissionError as e:
        handle_permission_error(path, e)
        raise typer.Exit(1) from e
    except Exception as e:
        error_message(f"Failed to generate source contract: {e}")
        raise typer.Exit(1) from e


def _format_table_text(table: TableInfo, with_fields: bool) -> list[str]:
    """Format a single table for text output.

    Args:
        table: TableInfo object with table metadata
        with_fields: Whether column details should be included (currently not supported)

    Returns:
        List of formatted lines for this table
    """
    col_count = table.column_count or 0
    lines = [f"  {table.table_name} ({col_count} columns)"]

    if with_fields:
        # Note: TableInfo doesn't include column details, so we can't show them
        # This would require a different function or additional parameter
        pass

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
        table_infos = list_database_tables(
            connection_string=connection_string,
            database_type=database_type,
            schema=schema,
            include_views=True,
        )

        if output_format == "json":
            import json

            # Convert to dicts for JSON output
            tables_dict = [t.model_dump(exclude_none=True) for t in table_infos]
            typer.echo(json.dumps(tables_dict, indent=2))
            return

        if not table_infos:
            typer.echo("No tables found.")
            return

        schema_msg = f" in schema '{schema}'" if schema else ""
        typer.echo(f"Tables{schema_msg} ({len(table_infos)} total):")

        for table in table_infos:
            for line in _format_table_text(table, with_fields):
                typer.echo(line)

    except Exception as e:
        error_message(f"Failed to list tables: {e}")
        raise typer.Exit(1) from e
