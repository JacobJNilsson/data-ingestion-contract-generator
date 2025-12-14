"""API schema parsing utilities."""

import json
from pathlib import Path

import yaml
from openapi_pydantic import OpenAPI
from openapi_pydantic.v3.v3_0 import OpenAPI as OpenAPI30


def _add_default_responses(spec_dict: dict) -> dict:
    """Add default responses to operations that don't have them.

    openapi-pydantic requires a responses field, but many OpenAPI specs
    omit it. This function adds a default empty responses object.

    Args:
        spec_dict: OpenAPI specification dictionary

    Returns:
        Modified specification dictionary with default responses
    """
    paths = spec_dict.get("paths", {})
    http_methods = ["get", "post", "put", "patch", "delete", "head", "options", "trace"]

    for path_item in paths.values():
        if not isinstance(path_item, dict):
            continue
        for method in http_methods:
            if method in path_item:
                operation = path_item[method]
                if isinstance(operation, dict) and "responses" not in operation:
                    operation["responses"] = {}

    return spec_dict


def parse_openapi_schema(schema_file: Path) -> OpenAPI | OpenAPI30:
    """Parse an OpenAPI/Swagger schema file.

    Supports both OpenAPI 3.0 and 3.1.x specifications.

    Args:
        schema_file: Path to OpenAPI schema file (JSON or YAML)

    Returns:
        Parsed OpenAPI specification as typed OpenAPI object

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
            spec_dict = json.loads(content)
        # Try YAML
        elif schema_file.suffix.lower() in [".yaml", ".yml"]:
            spec_dict = yaml.safe_load(content)
        else:
            # Try both formats
            try:
                spec_dict = json.loads(content)
            except json.JSONDecodeError:
                spec_dict = yaml.safe_load(content)

        # Add default responses to operations that don't have them
        # (openapi-pydantic requires responses, but many specs omit them)
        spec_dict = _add_default_responses(spec_dict)

        # Determine OpenAPI version and parse accordingly
        openapi_version = spec_dict.get("openapi", "")
        if openapi_version.startswith("3.0"):
            # Use OpenAPI 3.0 parser
            return OpenAPI30.model_validate(spec_dict)
        else:
            # Use OpenAPI 3.1 parser (default)
            return OpenAPI.model_validate(spec_dict)
    except (json.JSONDecodeError, yaml.YAMLError) as e:
        raise ValueError(f"Failed to parse schema file: {e}") from e
