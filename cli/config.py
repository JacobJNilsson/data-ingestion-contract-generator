"""Configuration file management for CLI tool."""

import os
from pathlib import Path

import yaml
from pydantic import BaseModel, Field

DEFAULT_CONFIG_PATH = Path.home() / ".contract-gen.yaml"


class CSVDefaults(BaseModel):
    """Default values for CSV operations."""

    delimiter: str = ","
    encoding: str = "utf-8"
    sample_size: int = 1000


class OutputDefaults(BaseModel):
    """Default values for output formatting."""

    format: str = "json"
    pretty: bool = False


class Defaults(BaseModel):
    """Default values for all commands."""

    csv: CSVDefaults = Field(default_factory=CSVDefaults)
    output: OutputDefaults = Field(default_factory=OutputDefaults)


class Config(BaseModel):
    """Configuration file structure."""

    version: str = "1.0"
    connections: dict[str, str] = Field(default_factory=dict)
    defaults: Defaults = Field(default_factory=Defaults)


def get_config_path() -> Path:
    """Get the config file path.

    Checks for CONTRACT_GEN_CONFIG environment variable,
    otherwise uses default path ~/.contract-gen.yaml
    """
    env_path = os.environ.get("CONTRACT_GEN_CONFIG")
    if env_path:
        return Path(env_path)
    return DEFAULT_CONFIG_PATH


def load_config() -> Config:
    """Load configuration from file.

    Returns built-in defaults if config file doesn't exist.
    """
    config_path = get_config_path()

    if not config_path.exists():
        return Config()

    try:
        with config_path.open("r") as f:
            data = yaml.safe_load(f) or {}

        # Parse with Pydantic for validation
        return Config.model_validate(data)
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in config file {config_path}: {e}") from e
    except Exception as e:
        raise ValueError(f"Failed to load config file {config_path}: {e}") from e


def save_config(config: Config) -> None:
    """Save configuration to file."""
    config_path = get_config_path()

    try:
        # Create parent directory if it doesn't exist
        config_path.parent.mkdir(parents=True, exist_ok=True)

        with config_path.open("w") as f:
            # Convert to dict for YAML serialization
            data = config.model_dump(mode="python")
            yaml.safe_dump(data, f, default_flow_style=False, sort_keys=False)
    except Exception as e:
        raise ValueError(f"Failed to save config file {config_path}: {e}") from e


def init_config(force: bool = False) -> Path:
    """Initialize config file with default values.

    Args:
        force: Overwrite existing config file if True

    Returns:
        Path to created config file

    Raises:
        FileExistsError: If config file exists and force is False
    """
    config_path = get_config_path()

    if config_path.exists() and not force:
        raise FileExistsError(f"Config file already exists: {config_path}")

    save_config(Config())
    return config_path


def get_connection(name: str, config: Config | None = None) -> str:
    """Get a named connection from config.

    Args:
        name: Connection name (without @ prefix)
        config: Config object, or None to load from file

    Returns:
        Connection string

    Raises:
        KeyError: If connection name not found
    """
    if config is None:
        config = load_config()

    if name not in config.connections:
        raise KeyError(f"Connection '{name}' not found in config")

    return config.connections[name]


def resolve_connection(value: str, config: Config | None = None) -> str:
    """Resolve connection string, handling @name references.

    Args:
        value: Connection string or @name reference
        config: Config object, or None to load from file

    Returns:
        Resolved connection string
    """
    if value.startswith("@"):
        connection_name = value[1:]  # Remove @ prefix
        return get_connection(connection_name, config)
    return value


def get_csv_defaults(config: Config | None = None) -> CSVDefaults:
    """Get CSV default values from config.

    Args:
        config: Config object, or None to load from file

    Returns:
        CSV defaults
    """
    if config is None:
        config = load_config()

    return config.defaults.csv


def get_output_defaults(config: Config | None = None) -> OutputDefaults:
    """Get output default values from config.

    Args:
        config: Config object, or None to load from file

    Returns:
        Output defaults
    """
    if config is None:
        config = load_config()

    return config.defaults.output


def validate_config(config: Config) -> list[str]:
    """Validate config file structure.

    Returns:
        List of validation errors (empty if valid)
    """
    errors = []

    # Check version
    if not config.version:
        errors.append("Missing 'version' field")

    # Check output format
    if config.defaults.output.format not in ["json", "yaml"]:
        errors.append("'defaults.output.format' must be 'json' or 'yaml'")

    return errors
