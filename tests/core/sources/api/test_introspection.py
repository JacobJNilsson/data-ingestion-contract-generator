from openapi_pydantic import OpenAPI

from core.sources.api.introspection import extract_endpoint_list


def test_extract_endpoint_list_basic() -> None:
    """Test extract_endpoint_list returns all endpoints with method and path."""
    spec_dict = {
        "openapi": "3.1.0",
        "info": {"title": "Test API", "version": "1.0.0"},
        "paths": {
            "/users": {
                "get": {"summary": "Get users"},
                "post": {"summary": "Create user"},
            },
            "/users/{id}": {"get": {"summary": "Get user"}},
        },
    }
    spec = OpenAPI.model_validate(spec_dict)

    result = extract_endpoint_list(spec)

    # Sort for consistent comparison (dict ordering may vary)
    # Convert to dicts for comparison
    result_dicts = [ep.model_dump(exclude_none=True) for ep in result]
    result_sorted = sorted(result_dicts, key=lambda x: (x["path"], x["method"]))

    assert result_sorted == [
        {"method": "GET", "path": "/users", "summary": "Get users"},
        {"method": "POST", "path": "/users", "summary": "Create user"},
        {"method": "GET", "path": "/users/{id}", "summary": "Get user"},
    ]


def test_extract_endpoint_list_filter_by_method() -> None:
    """Test extract_endpoint_list filters by HTTP method."""
    spec_dict = {
        "openapi": "3.1.0",
        "info": {"title": "Test API", "version": "1.0.0"},
        "paths": {
            "/users": {"get": {"summary": "Get users"}, "post": {"summary": "Create user"}},
            "/products": {"get": {"summary": "Get products"}},
        },
    }
    spec = OpenAPI.model_validate(spec_dict)

    result = extract_endpoint_list(spec, method="POST")

    # Convert to dicts for comparison
    result_dicts = [ep.model_dump(exclude_none=True) for ep in result]

    assert result_dicts == [
        {"method": "POST", "path": "/users", "summary": "Create user"},
    ]


def test_extract_endpoint_list_with_fields() -> None:
    """Test extract_endpoint_list includes field details when with_fields=True."""
    spec_dict = {
        "openapi": "3.1.0",
        "info": {"title": "Test API", "version": "1.0.0"},
        "paths": {
            "/users": {
                "post": {
                    "summary": "Create user",
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "name": {"type": "string"},
                                        "email": {"type": "string", "format": "email"},
                                    },
                                }
                            }
                        }
                    },
                },
            },
        },
    }
    spec = OpenAPI.model_validate(spec_dict)

    result = extract_endpoint_list(spec, with_fields=True, method="POST")

    # Convert to dicts for comparison
    result_dicts = [ep.model_dump(exclude_none=True) for ep in result]

    assert result_dicts == [
        {
            "method": "POST",
            "path": "/users",
            "summary": "Create user",
            "fields": ["name", "email"],
            "types": ["text", "email"],
            "constraints": {},
        },
    ]
