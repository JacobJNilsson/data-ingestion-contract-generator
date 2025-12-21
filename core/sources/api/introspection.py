"""API endpoint schema introspection."""

from openapi_pydantic import OpenAPI, Operation, PathItem, Reference, RequestBody, Schema
from openapi_pydantic.v3.v3_0 import OpenAPI as OpenAPI30
from openapi_pydantic.v3.v3_0 import Operation as Operation30
from openapi_pydantic.v3.v3_0 import PathItem as PathItem30
from openapi_pydantic.v3.v3_0 import Reference as Reference30
from openapi_pydantic.v3.v3_0 import RequestBody as RequestBody30
from openapi_pydantic.v3.v3_0 import Schema as Schema30

from core.models import EndpointInfo, FieldConstraint, FieldDefinition, SchemaInfo


def extract_endpoint_schema(
    openapi_spec: OpenAPI | OpenAPI30,
    endpoint: str,
    method: str = "POST",
) -> SchemaInfo:
    """Extract schema for a specific API endpoint.

    Args:
        openapi_spec: Parsed OpenAPI specification
        endpoint: API endpoint path (e.g., '/users', '/data')
        method: HTTP method (GET, POST, PUT, PATCH, DELETE)

    Returns:
        Dictionary containing fields, types, and constraints

    Raises:
        ValueError: If endpoint or schema not found
    """
    method = method.upper()

    # Get paths from OpenAPI spec
    if not openapi_spec.paths:
        raise ValueError("No paths found in OpenAPI specification")

    if endpoint not in openapi_spec.paths:
        available = list(openapi_spec.paths.keys())
        raise ValueError(f"Endpoint '{endpoint}' not found in schema. Available endpoints: {available}")

    # Get path item
    path_item = openapi_spec.paths[endpoint]

    # Get operation by method
    operation = _get_operation(path_item, method)
    if operation is None:
        available_methods = _get_available_methods(path_item)
        raise ValueError(
            f"Method '{method}' not found for endpoint '{endpoint}'. Available methods: {available_methods}"
        )

    # Get request body schema
    schema_dict, body_required = _extract_request_body_schema(openapi_spec, operation)

    if not schema_dict:
        return SchemaInfo(fields=[])

    # Extract fields and types from schema
    field_definitions = _extract_fields_from_schema(schema_dict, body_required)
    return SchemaInfo(fields=field_definitions)


# Valid HTTP methods for OpenAPI PathItem
_HTTP_METHODS: set[str] = {"get", "post", "put", "patch", "delete", "head", "options", "trace"}


def _get_operation(path_item: PathItem | PathItem30, method: str) -> Operation | Operation30 | None:
    """Get operation from path item by HTTP method.

    Args:
        path_item: PathItem object
        method: HTTP method (uppercase)

    Returns:
        Operation object or None if not found
    """
    method_lower = method.lower()
    if method_lower in _HTTP_METHODS:
        return getattr(path_item, method_lower, None)
    return None


def _get_available_methods(path_item: PathItem | PathItem30) -> list[str]:
    """Get list of available HTTP methods for a path item.

    Args:
        path_item: PathItem object

    Returns:
        List of available HTTP methods (uppercase)
    """
    methods = []
    for method_lower in _HTTP_METHODS:
        if getattr(path_item, method_lower, None):
            methods.append(method_lower.upper())
    return methods


def _resolve_request_body(
    openapi_spec: OpenAPI | OpenAPI30, request_body_raw: RequestBody | RequestBody30 | Reference | Reference30
) -> RequestBody | RequestBody30:
    """Resolve request body, handling references if needed.

    Args:
        openapi_spec: Full OpenAPI specification
        request_body_raw: RequestBody or Reference

    Returns:
        Resolved RequestBody object

    Raises:
        ValueError: If reference cannot be resolved or type is unexpected
    """
    if isinstance(request_body_raw, (Reference, Reference30)):
        resolved = _resolve_reference(openapi_spec, request_body_raw.ref)
        if not isinstance(resolved, (RequestBody, RequestBody30)):
            raise ValueError(f"Expected RequestBody, got {type(resolved)}")
        return resolved
    if isinstance(request_body_raw, (RequestBody, RequestBody30)):
        return request_body_raw
    raise ValueError(f"Unexpected requestBody type: {type(request_body_raw)}")


def _get_content_schema(
    openapi_spec: OpenAPI | OpenAPI30, media_type_schema: Schema | Schema30 | Reference | Reference30 | None
) -> dict | None:
    """Extract schema dictionary from media type schema.

    Args:
        openapi_spec: Full OpenAPI specification
        media_type_schema: Schema, Reference, or None

    Returns:
        Schema as dictionary or None

    Raises:
        ValueError: If schema type is unexpected
    """
    if not media_type_schema:
        return None

    if isinstance(media_type_schema, (Reference, Reference30)):
        return _resolve_schema_reference(openapi_spec, media_type_schema.ref)
    if isinstance(media_type_schema, (Schema, Schema30)):
        return _schema_to_dict(media_type_schema)
    raise ValueError(f"Unexpected schema type: {type(media_type_schema)}")


def _extract_request_body_schema(
    openapi_spec: OpenAPI | OpenAPI30, operation: Operation | Operation30
) -> tuple[dict | None, bool]:
    """Extract request body schema from operation.

    Args:
        openapi_spec: Full OpenAPI specification
        operation: Operation object

    Returns:
        Tuple of (schema_dict, body_required)
    """
    if not operation.requestBody:
        return None, False

    request_body = _resolve_request_body(openapi_spec, operation.requestBody)

    if not request_body.content:
        return None, request_body.required or False

    # Get content schema (typically application/json)
    json_content = request_body.content.get("application/json") or request_body.content.get(
        "application/x-www-form-urlencoded"
    )

    if not json_content:
        return None, request_body.required or False

    schema_dict = _get_content_schema(openapi_spec, json_content.media_type_schema)
    return schema_dict, request_body.required or False


def _resolve_reference(
    openapi_spec: OpenAPI | OpenAPI30, ref: str
) -> RequestBody | RequestBody30 | Schema | Schema30 | object:
    """Resolve a $ref pointer in the OpenAPI spec.

    Args:
        openapi_spec: Full OpenAPI specification
        ref: Reference string (e.g., '#/components/schemas/User')

    Returns:
        Resolved object (could be RequestBody, Schema, or other OpenAPI objects)
    """
    if not ref.startswith("#/"):
        raise ValueError(f"Only internal references are supported: {ref}")

    parts = ref[2:].split("/")
    current: object = openapi_spec

    for part in parts:
        if not hasattr(current, part):
            raise ValueError(f"Reference not found: {ref}")
        current = getattr(current, part)

    return current


def _resolve_schema_reference(openapi_spec: OpenAPI | OpenAPI30, ref: str) -> dict:
    """Resolve a schema $ref pointer in the OpenAPI spec.

    Args:
        openapi_spec: Full OpenAPI specification
        ref: Reference string (e.g., '#/components/schemas/User')

    Returns:
        Resolved schema as dictionary
    """
    resolved = _resolve_reference(openapi_spec, ref)
    if isinstance(resolved, Schema):
        return _schema_to_dict(resolved)
    if isinstance(resolved, dict):
        return resolved
    raise ValueError(f"Unexpected type for schema reference: {type(resolved)}")


def _schema_to_dict(schema: Schema | Schema30) -> dict:
    """Convert Schema object to dictionary.

    Args:
        schema: Schema object

    Returns:
        Schema as dictionary
    """
    # Use model_dump to convert to dict, using aliases for OpenAPI field names
    return schema.model_dump(by_alias=True, exclude_none=True)


def _extract_string_constraints(field_schema: dict) -> list[FieldConstraint]:
    """Extract constraints for string type fields."""
    constraints: list[FieldConstraint] = []
    if "minLength" in field_schema:
        constraints.append(FieldConstraint(type="pattern", value=f"minLength: {field_schema['minLength']}"))
    if "maxLength" in field_schema:
        constraints.append(FieldConstraint(type="pattern", value=f"maxLength: {field_schema['maxLength']}"))
    if "pattern" in field_schema:
        constraints.append(FieldConstraint(type="pattern", value=field_schema["pattern"]))
    return constraints


def _extract_numeric_constraints(field_schema: dict) -> list[FieldConstraint]:
    """Extract constraints for integer/number type fields."""
    constraints: list[FieldConstraint] = []
    if "minimum" in field_schema:
        constraints.append(FieldConstraint(type="range", min_value=field_schema["minimum"]))
    if "maximum" in field_schema:
        constraints.append(FieldConstraint(type="range", max_value=field_schema["maximum"]))
    return constraints


def _extract_field_constraints(
    field_name: str, field_schema: dict, field_type: str, required_fields: set[str], body_required: bool
) -> list[FieldConstraint]:
    """Extract all constraints for a field."""
    constraints: list[FieldConstraint] = []

    if field_name in required_fields or body_required:
        constraints.append(FieldConstraint(type="not_null"))

    if "enum" in field_schema:
        constraints.append(FieldConstraint(type="enum", value=field_schema["enum"]))

    if field_type == "string":
        constraints.extend(_extract_string_constraints(field_schema))
    elif field_type in ["integer", "number"]:
        constraints.extend(_extract_numeric_constraints(field_schema))

    return constraints


def _extract_fields_from_schema(schema: dict, body_required: bool = False) -> list[FieldDefinition]:
    """Extract fields from a JSON schema into FieldDefinition objects."""
    field_definitions: list[FieldDefinition] = []

    required_fields = set(schema.get("required", []))
    properties = schema.get("properties", {})

    for field_name, field_schema in properties.items():
        if not isinstance(field_schema, dict):
            continue

        field_type = field_schema.get("type", "string")
        field_format = field_schema.get("format")
        contract_type = _map_json_type_to_contract_type(field_type, field_format)

        field_constraints = _extract_field_constraints(
            field_name, field_schema, field_type, required_fields, body_required
        )

        nullable = field_name not in required_fields and not body_required

        field_definitions.append(
            FieldDefinition(
                name=field_name,
                data_type=contract_type,
                nullable=nullable,
                constraints=field_constraints,
                description=field_schema.get("description"),
            )
        )

    return field_definitions


def _map_json_type_to_contract_type(json_type: str, format_type: str | None = None) -> str:
    """Map JSON schema type to contract type."""
    if format_type:
        format_mapping = {
            "date-time": "datetime",
            "date": "date",
            "time": "time",
            "email": "email",
            "uri": "url",
            "uuid": "uuid",
            "int32": "integer",
            "int64": "bigint",
            "float": "float",
            "double": "double",
        }
        if format_type in format_mapping:
            return format_mapping[format_type]

    type_mapping = {
        "string": "text",
        "integer": "integer",
        "number": "float",
        "boolean": "boolean",
        "array": "array",
        "object": "json",
        "null": "null",
    }

    return type_mapping.get(json_type, "text")


def _is_valid_http_method(op_method: str) -> bool:
    """Check if the operation method is a valid HTTP method (not metadata)."""
    return op_method.lower() not in ["parameters", "$ref", "summary", "description"]


def _build_endpoint_info(
    openapi_spec: OpenAPI | OpenAPI30,
    path: str,
    op_method: str,
    operation: Operation | Operation30,
    with_fields: bool,
) -> EndpointInfo:
    """Build endpoint info for a single operation."""
    from typing import Any

    endpoint_info_dict: dict[str, Any] = {
        "method": op_method,
        "path": path,
        "summary": operation.summary or "",
    }

    if with_fields:
        try:
            schema_info = extract_endpoint_schema(openapi_spec, path, op_method)
            endpoint_info_dict["fields"] = schema_info.fields
        except Exception:
            endpoint_info_dict["error"] = "Failed to extract schema"

    return EndpointInfo.model_validate(endpoint_info_dict)


def _get_path_operations(path_item: PathItem | PathItem30) -> list[tuple[str, Operation | Operation30]]:
    """Get all operations from a path item.

    Args:
        path_item: PathItem object

    Returns:
        List of (method, operation) tuples
    """
    operations = []
    for method_lower in _HTTP_METHODS:
        operation = getattr(path_item, method_lower, None)
        if operation:
            operations.append((method_lower.upper(), operation))
    return operations


def extract_endpoint_list(
    openapi_spec: OpenAPI | OpenAPI30,
    with_fields: bool = False,
    method: str | None = None,
) -> list[EndpointInfo]:
    """List API endpoints with optional field details.

    Args:
        openapi_spec: Parsed OpenAPI specification
        with_fields: Whether to include field details
        method: Filter by HTTP method (optional)

    Returns:
        List of endpoint details
    """
    if not openapi_spec.paths:
        return []

    results: list[EndpointInfo] = []
    method_filter = method.upper() if method else None

    for path, path_item in openapi_spec.paths.items():
        if not path.startswith("/"):
            continue

        operations = _get_path_operations(path_item)

        for op_method, operation in operations:
            if method_filter and op_method != method_filter:
                continue

            endpoint_info = _build_endpoint_info(openapi_spec, path, op_method, operation, with_fields)
            results.append(endpoint_info)

    return results
