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
    assert contract["destination_id"] == "users_api"

    fields = contract["schema"]["fields"]
    assert len(fields) == 4

    # Check 'name' field
    name_field = next(f for f in fields if f["name"] == "name")
    assert name_field["data_type"] == "text"
    assert not name_field["nullable"]
    constraints = {c["type"] for c in name_field["constraints"]}
    assert "not_null" in constraints
    assert "pattern" in constraints

    # Check 'email' field
    email_field = next(f for f in fields if f["name"] == "email")
    assert email_field["data_type"] == "email"
    assert not email_field["nullable"]
    constraints = {c["type"] for c in email_field["constraints"]}
    assert "not_null" in constraints

    # Check 'age' field
    age_field = next(f for f in fields if f["name"] == "age")
    assert age_field["data_type"] == "integer"
    assert not age_field["nullable"]
    constraints = {c["type"] for c in age_field["constraints"]}
    assert "not_null" in constraints
    assert "range" in constraints

    # Check 'active' field
    active_field = next(f for f in fields if f["name"] == "active")
    assert active_field["data_type"] == "boolean"
    assert not active_field["nullable"]
    constraints = {c["type"] for c in active_field["constraints"]}
    assert "not_null" in constraints


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

    field_names = [f["name"] for f in contract["schema"]["fields"]]
    assert "id" in field_names
    assert "value" in field_names

    field_types = {f["name"]: f["data_type"] for f in contract["schema"]["fields"]}
    assert field_types["id"] == "uuid"
    assert field_types["value"] == "float"


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
        {"method": "GET", "path": "/products", "summary": "List products", "fields": []},
        {"method": "GET", "path": "/users", "summary": "Get users", "fields": []},
        {"method": "POST", "path": "/users", "summary": "Create user", "fields": []},
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

    expected = [{"method": "POST", "path": "/users", "summary": "Create user", "fields": []}]

    assert output == expected
