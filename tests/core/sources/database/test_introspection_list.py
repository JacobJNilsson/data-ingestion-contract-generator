from unittest.mock import MagicMock, patch

from core.models import ColumnInfo
from core.sources.database.introspection import extract_table_list


@patch("core.sources.database.introspection.create_database_engine")
@patch("core.sources.database.introspection.inspect")
def test_extract_table_list_basic(mock_inspect: MagicMock, mock_create_engine: MagicMock) -> None:
    """Test extract_table_list returns table names with column counts."""
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine

    mock_inspector = MagicMock()
    mock_inspect.return_value = mock_inspector

    mock_inspector.get_table_names.return_value = ["table1", "table2"]
    mock_inspector.get_columns.side_effect = [
        [{"name": "id", "type": "INTEGER", "nullable": False}],
        [{"name": "name", "type": "VARCHAR", "nullable": True}],
    ]

    result = extract_table_list("connection", "postgresql")

    assert result == [
        {"name": "table1", "column_count": 1},
        {"name": "table2", "column_count": 1},
    ]


@patch("core.sources.database.introspection.create_database_engine")
@patch("core.sources.database.introspection.inspect")
def test_extract_table_list_with_fields(mock_inspect: MagicMock, mock_create_engine: MagicMock) -> None:
    """Test extract_table_list with with_fields=True includes column details."""
    mock_engine = MagicMock()
    mock_create_engine.return_value = mock_engine

    mock_inspector = MagicMock()
    mock_inspect.return_value = mock_inspector

    mock_inspector.get_table_names.return_value = ["users", "orders"]
    mock_inspector.get_columns.side_effect = [
        [
            {"name": "id", "type": "INTEGER", "nullable": False},
            {"name": "email", "type": "VARCHAR", "nullable": False},
        ],
        [
            {"name": "id", "type": "INTEGER", "nullable": False},
            {"name": "total", "type": "DECIMAL", "nullable": True},
        ],
    ]

    result = extract_table_list("connection", "postgresql", with_fields=True)

    assert result == [
        {
            "name": "users",
            "column_count": 2,
            "columns": [
                ColumnInfo(name="id", type="INTEGER", nullable=False),
                ColumnInfo(name="email", type="VARCHAR", nullable=False),
            ],
        },
        {
            "name": "orders",
            "column_count": 2,
            "columns": [
                ColumnInfo(name="id", type="INTEGER", nullable=False),
                ColumnInfo(name="total", type="DECIMAL", nullable=True),
            ],
        },
    ]
