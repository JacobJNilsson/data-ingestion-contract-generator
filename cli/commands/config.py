"""Configuration management commands."""

import typer
import yaml

from cli.config import get_config_path, init_config, load_config, validate_config
from cli.output import error_message, success_message

app = typer.Typer(help="Manage configuration file")


@app.command("init")
def config_init(
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing config file"),
) -> None:
    """Initialize configuration file with default values.

    Creates ~/.contract-gen.yaml (or path from CONTRACT_GEN_CONFIG env var).

    Example:
        contract-gen config init
        contract-gen config init --force
    """
    try:
        config_path = init_config(force=force)
        success_message(f"Created config file: {config_path}")
        typer.echo("Edit this file to customize your defaults and connections.")
    except FileExistsError as e:
        error_message(
            f"Config file already exists: {get_config_path()}",
            hint="Use --force to overwrite, or edit the existing file",
        )
        raise typer.Exit(1) from e
    except Exception as e:
        error_message(f"Failed to create config file: {e}")
        raise typer.Exit(1) from e


@app.command("show")
def config_show() -> None:
    """Display current configuration.

    Example:
        contract-gen config show
    """
    try:
        config_path = get_config_path()
        config = load_config()

        typer.echo(f"Config file: {config_path}")
        if not config_path.exists():
            typer.echo("(using built-in defaults, file does not exist)")
        typer.echo("")

        # Pretty-print the config
        typer.echo(yaml.safe_dump(config, default_flow_style=False, sort_keys=False))
    except Exception as e:
        error_message(f"Failed to load config: {e}")
        raise typer.Exit(1) from e


@app.command("validate")
def config_validate() -> None:
    """Validate configuration file syntax and structure.

    Example:
        contract-gen config validate
    """
    try:
        config_path = get_config_path()

        if not config_path.exists():
            error_message(f"Config file does not exist: {config_path}", hint="Run 'contract-gen config init' first")
            raise typer.Exit(1)

        config = load_config()
        errors = validate_config(config)

        if errors:
            error_message("Config validation failed:")
            for error in errors:
                typer.echo(f"  - {error}")
            raise typer.Exit(1)

        success_message("Config file is valid")
    except Exception as e:
        error_message(f"Failed to validate config: {e}")
        raise typer.Exit(1) from e


@app.command("path")
def config_path() -> None:
    """Show path to configuration file.

    Example:
        contract-gen config path
    """
    config_path = get_config_path()
    typer.echo(str(config_path))
