"""Tests for destination API command with OpenAPI schemas."""

import json
from pathlib import Path

from typer.testing import CliRunner

from cli.commands.destination import app

runner = CliRunner()


def test_destination_api_cli_with_openapi_schema(tmp_path: Path) -> None:
    """Test generating destination contract from OpenAPI schema file."""
    openapi_schema = {
        "openapi": "3.0.0",
        "info": {"title": "API", "version": "1.0.0"},
        "paths": {
            "/users": {
                "post": {
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["name", "email"],
                                    "properties": {
                                        "name": {"type": "string", "minLength": 1, "maxLength": 100},
                                        "email": {"type": "string", "format": "email"},
                                        "age": {"type": "integer", "minimum": 0, "maximum": 150},
                                        "active": {"type": "boolean"},
                                    },
                                }
                            }
                        },
                    }
                }
            }
        },
    }

    schema_file = tmp_path / "openapi.json"
    schema_file.write_text(json.dumps(openapi_schema))

    output_file = tmp_path / "contract.json"

    result = runner.invoke(
        app,
        [
            "api",
            "generate",
            str(schema_file),
            "/users",
            "--id",
            "users_api",
            "--method",
            "POST",
            "--output",
            str(output_file),
        ],
    )

    assert result.exit_code == 0, f"Command failed: {result.stderr}"
    assert output_file.exists()

    contract = json.loads(output_file.read_text())

    expected_contract = {
        "contract_type": "destination",
        "contract_version": "2.0",
        "destination_id": "users_api",
        "schema": {
            "fields": [
                {
                    "name": "name",
                    "type": "text",
                    "constraints": ["REQUIRED", "MIN_LENGTH: 1", "MAX_LENGTH: 100"],
                },
                {
                    "name": "email",
                    "type": "email",
                    "constraints": ["REQUIRED"],
                },
                {
                    "name": "age",
                    "type": "integer",
                    "constraints": ["REQUIRED", "MIN: 0", "MAX: 150"],
                },
                {
                    "name": "active",
                    "type": "boolean",
                    "constraints": ["REQUIRED"],
                },
            ],
        },
        "metadata": {
            "destination_type": "api",
            "endpoint": "/users",
            "http_method": "POST",
            "schema_file": str(schema_file),
        },
        "validation_rules": {"rules": []},
    }

    assert contract == expected_contract


def test_destination_api_cli_with_yaml_schema(tmp_path: Path) -> None:
    """Test generating destination contract from YAML OpenAPI schema."""
    yaml_content = """
openapi: 3.0.0
info:
  title: API
  version: 1.0.0
paths:
  /data:
    post:
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                id:
                  type: string
                  format: uuid
                value:
                  type: number
"""

    schema_file = tmp_path / "openapi.yaml"
    schema_file.write_text(yaml_content)

    output_file = tmp_path / "contract.json"

    result = runner.invoke(
        app,
        [
            "api",
            "generate",
            str(schema_file),
            "/data",
            "--id",
            "data_api",
            "--output",
            str(output_file),
        ],
    )

    assert result.exit_code == 0, f"Command failed: {result.stderr}"
    assert output_file.exists()

    contract = json.loads(output_file.read_text())

    expected_contract = {
        "contract_type": "destination",
        "contract_version": "2.0",
        "destination_id": "data_api",
        "schema": {
            "fields": [
                {
                    "name": "id",
                    "type": "uuid",
                },
                {
                    "name": "value",
                    "type": "float",
                },
            ],
        },
        "metadata": {
            "destination_type": "api",
            "endpoint": "/data",
            "http_method": "POST",
            "schema_file": str(schema_file),
        },
        "validation_rules": {"rules": []},
    }

    assert contract == expected_contract


def test_destination_api_cli_endpoint_not_found(tmp_path: Path) -> None:
    """Test error handling when endpoint is not found in schema."""
    openapi_schema = {
        "openapi": "3.0.0",
        "info": {"title": "API", "version": "1.0.0"},
        "paths": {"/users": {"get": {}}},
    }

    schema_file = tmp_path / "openapi.json"
    schema_file.write_text(json.dumps(openapi_schema))

    result = runner.invoke(
        app,
        [
            "api",
            "generate",
            str(schema_file),
            "/missing",
            "--id",
            "test_api",
        ],
    )

    assert result.exit_code == 1
    assert "not found in schema" in result.stderr


def test_destination_api_cli_method_not_found(tmp_path: Path) -> None:
    """Test error handling when method is not found for endpoint."""
    openapi_schema = {
        "openapi": "3.0.0",
        "info": {"title": "API", "version": "1.0.0"},
        "paths": {"/users": {"get": {}}},
    }

    schema_file = tmp_path / "openapi.json"
    schema_file.write_text(json.dumps(openapi_schema))

    result = runner.invoke(
        app,
        [
            "api",
            "generate",
            str(schema_file),
            "/users",
            "--id",
            "test_api",
            "--method",
            "POST",
        ],
    )

    assert result.exit_code == 1
    assert "not found for endpoint" in result.stderr


def test_destination_api_cli_list_text_output(tmp_path: Path) -> None:
    """Test listing available endpoints in text format."""
    openapi_schema = {
        "openapi": "3.0.0",
        "info": {"title": "API", "version": "1.0.0"},
        "paths": {
            "/users": {"get": {}, "post": {}},
            "/products": {"get": {}},
        },
    }

    schema_file = tmp_path / "openapi.json"
    schema_file.write_text(json.dumps(openapi_schema))

    result = runner.invoke(app, ["api", "list", str(schema_file)])

    assert result.exit_code == 0, f"Command failed: {result.stderr}"
    # Text output should contain these strings
    assert "/users" in result.stdout
    assert "/products" in result.stdout
    assert "GET" in result.stdout
    assert "POST" in result.stdout


def test_destination_api_cli_list_json_output(tmp_path: Path) -> None:
    """Test listing available endpoints in JSON format."""
    openapi_schema = {
        "openapi": "3.0.0",
        "info": {"title": "API", "version": "1.0.0"},
        "paths": {
            "/users": {"get": {"summary": "Get users"}, "post": {"summary": "Create user"}},
            "/products": {"get": {"summary": "List products"}},
        },
    }

    schema_file = tmp_path / "openapi.json"
    schema_file.write_text(json.dumps(openapi_schema))

    result = runner.invoke(app, ["api", "list", str(schema_file), "--format", "json"])

    assert result.exit_code == 0
    output = json.loads(result.stdout)
    output_sorted = sorted(output, key=lambda x: (x["path"], x["method"]))

    expected = [
        {"method": "GET", "path": "/products", "summary": "List products"},
        {"method": "GET", "path": "/users", "summary": "Get users"},
        {"method": "POST", "path": "/users", "summary": "Create user"},
    ]

    assert output_sorted == expected


def test_destination_api_cli_list_filter_by_method(tmp_path: Path) -> None:
    """Test filtering endpoint list by HTTP method."""
    openapi_schema = {
        "openapi": "3.0.0",
        "info": {"title": "API", "version": "1.0.0"},
        "paths": {
            "/users": {"get": {"summary": "Get users"}, "post": {"summary": "Create user"}},
            "/products": {"get": {"summary": "List products"}},
        },
    }

    schema_file = tmp_path / "openapi.json"
    schema_file.write_text(json.dumps(openapi_schema))

    result = runner.invoke(app, ["api", "list", str(schema_file), "--method", "POST", "--format", "json"])

    assert result.exit_code == 0
    output = json.loads(result.stdout)

    expected = [{"method": "POST", "path": "/users", "summary": "Create user"}]

    assert output == expected
