from core.sources.api.introspection import extract_endpoint_list


def test_extract_endpoint_list_basic() -> None:
    """Test extract_endpoint_list returns all endpoints with method and path."""
    spec = {
        "paths": {
            "/users": {
                "get": {"summary": "Get users"},
                "post": {"summary": "Create user"},
            },
            "/users/{id}": {"get": {"summary": "Get user"}},
        }
    }

    result = extract_endpoint_list(spec)

    # Sort for consistent comparison (dict ordering may vary)
    result_sorted = sorted(result, key=lambda x: (x["path"], x["method"]))

    assert result_sorted == [
        {"method": "GET", "path": "/users", "summary": "Get users"},
        {"method": "POST", "path": "/users", "summary": "Create user"},
        {"method": "GET", "path": "/users/{id}", "summary": "Get user"},
    ]


def test_extract_endpoint_list_filter_by_method() -> None:
    """Test extract_endpoint_list filters by HTTP method."""
    spec = {
        "paths": {
            "/users": {"get": {"summary": "Get users"}, "post": {"summary": "Create user"}},
            "/products": {"get": {"summary": "Get products"}},
        }
    }

    result = extract_endpoint_list(spec, method="POST")

    assert result == [
        {"method": "POST", "path": "/users", "summary": "Create user"},
    ]


def test_extract_endpoint_list_with_fields() -> None:
    """Test extract_endpoint_list includes field details when with_fields=True."""
    spec = {
        "paths": {
            "/users": {
                "post": {
                    "summary": "Create user",
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "properties": {
                                        "name": {"type": "string"},
                                        "email": {"type": "string", "format": "email"},
                                    }
                                }
                            }
                        }
                    },
                },
            },
        }
    }

    result = extract_endpoint_list(spec, with_fields=True, method="POST")

    assert result == [
        {
            "method": "POST",
            "path": "/users",
            "summary": "Create user",
            "fields": ["name", "email"],
            "types": ["text", "email"],
            "constraints": {},
        },
    ]
