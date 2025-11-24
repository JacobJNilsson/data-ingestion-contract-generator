"""API schema parsing utilities."""

import json
from pathlib import Path
from typing import Any

import yaml


def parse_openapi_schema(schema_file: Path) -> dict[str, Any]:
    """Parse an OpenAPI/Swagger schema file.

    Args:
        schema_file: Path to OpenAPI schema file (JSON or YAML)

    Returns:
        Parsed OpenAPI specification as dictionary

    Raises:
        FileNotFoundError: If schema file doesn't exist
        ValueError: If schema file cannot be parsed
    """
    if not schema_file.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_file}")

    try:
        content = schema_file.read_text()
        # Try JSON first
        if schema_file.suffix.lower() in [".json"]:
            return dict(json.loads(content))
        # Try YAML
        elif schema_file.suffix.lower() in [".yaml", ".yml"]:
            return dict(yaml.safe_load(content))
        else:
            # Try both formats
            try:
                return dict(json.loads(content))
            except json.JSONDecodeError:
                return dict(yaml.safe_load(content))
    except (json.JSONDecodeError, yaml.YAMLError) as e:
        raise ValueError(f"Failed to parse schema file: {e}") from e
