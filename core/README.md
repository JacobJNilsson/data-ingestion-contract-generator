# Contract Generator Core

This library provides the core logic and models for generating three types of contracts for data ingestion pipelines:

1. **Source Contracts** - Describe data sources (schema, format, quality)
2. **Destination Contracts** - Define data destinations (schema, constraints, validation)
3. **Transformation Contracts** - Map source to destination (field mappings, transformations, enrichment)

These capabilities are exposed via a CLI tool (`contract-gen`) and an MCP server for AI integration.

## Architecture

The three-contract architecture separates concerns:

- **Source Contract**: Automated analysis of source data files or databases
- **Destination Contract**: Definition of target schema and rules
- **Transformation Contract**: References both, defines how to move data from source to destination

## AI Integration (MCP)

To use these tools with Cursor, add to `.cursor/mcp.json`:

```json
{
  "contract-generator": {
    "command": "uv",
    "args": [
      "--directory",
      "/absolute/path/to/project",
      "run",
      "mcp_server/server.py"
    ]
  }
}
```

## Available Functionality

### Contract Generation

1. **generate_csv_source_contract** - Generate a source contract from a CSV file
   - Analyzes CSV format, encoding, delimiter, schema, and quality
   - Returns: CSVSourceContract

2. **generate_json_source_contract** - Generate a source contract from a JSON/NDJSON file
   - Analyzes JSON format, encoding, schema, and quality
   - Returns: JSONSourceContract

3. **generate_source_analysis** - Analyze a file to determine its type and extract metadata
   - Content-based file type detection
   - Returns: SourceAnalysisResult with file_type, schema, and quality metrics

4. **generate_database_source_contract** - Generate a source contract from a database table or query
   - Supports PostgreSQL, MySQL, and SQLite
   - Analyzes schema, types, and samples data
   - Returns: JSON contract with `contract_type: "source"`

5. **generate_destination_contract** - Generate a destination contract
   - Define target schema, validation rules, and constraints
   - Returns: JSON contract with `contract_type: "destination"`

6. **generate_transformation_contract** - Generate a transformation contract
   - Maps source to destination with transformation rules
   - Returns: JSON contract with `contract_type: "transformation"`

### Database Discovery

1. **list_database_tables** - List all tables in a database with metadata
   - Returns table names, row counts, column counts, and primary key information
   - Helps discover available tables before generating contracts

2. **generate_database_multi_source_contracts** - Generate contracts for multiple tables with relationship analysis
   - Automatically detects foreign key relationships between tables
   - Calculates optimal load order using topological sort
   - Includes relationship metadata in contracts (dependencies, referenced-by)
   - Returns: List of JSON contracts with relationship information

### Analysis & Validation

1. **analyze_source** - Analyze a source file and return raw metadata
2. **validate_contract** - Validate any contract type (source, destination, or transformation)

## Example Workflows

### Discovering Database Tables

```python
# 1. List all tables in a database
tables = list_database_tables(
    connection_string="postgresql://user:pass@localhost:5432/mydb",
    database_type="postgresql",
    schema="public"
)

# Returns:
# {
#   "tables": [
#     {
#       "table_name": "users",
#       "schema": "public",
#       "type": "table",
#       "has_primary_key": true,
#       "primary_key_columns": ["id"],
#       "row_count": 10000,
#       "column_count": 8
#     },
#     {
#       "table_name": "orders",
#       "schema": "public",
#       "type": "table",
#       "has_primary_key": true,
#       "primary_key_columns": ["order_id"],
#       "row_count": 50000,
#       "column_count": 12
#     }
#   ],
#   "count": 2
# }

# 2. Then generate contracts for selected tables
contract = generate_database_source_contract(
    source_id="users_table",
    connection_string="postgresql://user:pass@localhost:5432/mydb",
    database_type="postgresql",
    source_type="table",
    source_name="users",
    schema="public"
)
```

### File-Based Source

```python
# 1. Generate source contract from CSV file
source_contract = generate_csv_source_contract(
    source_path="/path/to/data.csv",
    source_id="swedish_bank_csv"
)

# Or for JSON files:
# source_contract = generate_json_source_contract(
#     source_path="/path/to/data.json",
#     source_id="swedish_bank_json"
# )

# 2. Generate destination contract
dest_contract = generate_destination_contract(
    destination_id="dwh_transactions_table",
    schema={
        "fields": [
            {"name": "id", "data_type": "uuid"},
            {"name": "date", "data_type": "date"},
            {"name": "amount", "data_type": "decimal"}
        ]
    }
)

# 3. Generate transformation contract
transform_contract = generate_transformation_contract(
    transformation_id="swedish_to_dwh",
    source_ref="swedish_bank_csv",
    destination_ref="dwh_transactions_table"
)

# 4. LLM fills in field_mappings (list of FieldMapping objects) in transform_contract
# 5. Validate and execute
```

### Database-Based Source

```python
# 1. Generate source contract from PostgreSQL table
source_contract = generate_database_source_contract(
    source_id="orders_table",
    connection_string="postgresql://user:pass@localhost:5432/mydb",
    database_type="postgresql",
    source_type="table",
    source_name="orders",
    schema="public"
)

# 2. Generate source contract from SQL query
query_contract = generate_database_source_contract(
    source_id="active_users",
    connection_string="postgresql://user:pass@localhost:5432/mydb",
    database_type="postgresql",
    source_type="query",
    query="SELECT user_id, email, created_at FROM users WHERE status = 'active'"
)

# 3. Continue with destination and transformation contracts as above
```

### Multi-Table Analysis with Relationships

```python
# Analyze multiple related tables at once
contracts = generate_database_multi_source_contracts(
    connection_string="postgresql://user:pass@localhost:5432/mydb",
    database_type="postgresql",
    schema="public",
    include_relationships=True  # Detect foreign keys and calculate load order
)

# Returns a list of contracts with relationship metadata:
# {
#   "contracts": [
#     {
#       "contract_type": "source",
#       "source_id": "users",
#       "source_name": "users",
#       "database_type": "postgresql",
#       "schema": {...},
#       "metadata": {
#         "relationships": {
#           "foreign_keys": [],  # Tables this table references
#           "referenced_by": [   # Tables that reference this table
#             {
#               "table": "orders",
#               "columns": ["user_id"],
#               "referred_columns": ["id"]
#             }
#           ]
#         },
#         "load_order": 1,       # Load this table first
#         "depends_on": []       # No dependencies
#       }
#     },
#     {
#       "contract_type": "source",
#       "source_id": "orders",
#       "source_name": "orders",
#       "database_type": "postgresql",
#       "schema": {...},
#       "metadata": {
#         "relationships": {
#           "foreign_keys": [    # This table references users
#             {
#               "columns": ["user_id"],
#               "referred_table": "users",
#               "referred_columns": ["id"]
#             }
#           ],
#           "referenced_by": []
#         },
#         "load_order": 2,       # Load after users
#         "depends_on": ["users"]
#       }
#     }
#   ],
#   "count": 2
# }

# Analyze specific tables only
contracts = generate_database_multi_source_contracts(
    connection_string="sqlite:///mydb.db",
    database_type="sqlite",
    tables=["users", "orders", "products"],  # Specific tables
    include_relationships=True
)

# Skip relationship detection for faster analysis
contracts = generate_database_multi_source_contracts(
    connection_string="mysql://user:pass@localhost:3306/mydb",
    database_type="mysql",
    include_relationships=False  # No FK detection or load order
)
```

**Use Cases:**

- **Database Migration**: Analyze entire schema and understand table dependencies
- **Data Warehouse ETL**: Generate contracts for all source tables with correct load order
- **Schema Documentation**: Document relationships and dependencies across tables
- **Incremental Loading**: Use load_order to load dependent tables after their parents

## Contract Types

### Source Contract

```json
{
  "contract_type": "source",
  "contract_version": "2.0",
  "source_id": "...",
  "file_format": "csv",
  "schema": {
    "fields": [
      {
        "name": "id",
        "data_type": "integer",
        "profiling": {
          "null_count": 0,
          "distinct_count": 1000,
          "min_value": "1",
          "max_value": "1000"
        }
      }
    ]
  },
  "quality": {
    "total_rows": 1000,
    "sample_data": [["1"], ["2"]],
    "issues": []
  }
}
```

### Destination Contract

```json
{
  "contract_type": "destination",
  "contract_version": "2.0",
  "destination_id": "...",
  "schema": {
    "fields": [
      {
        "name": "id",
        "data_type": "uuid",
        "constraints": [
          {"type": "primary_key"},
          {"type": "not_null"}
        ]
      }
    ]
  },
  "validation_rules": {
    "required_fields": ["id"],
    "unique_constraints": ["id"]
  }
}
```

### Transformation Contract

```json
{
  "contract_type": "transformation",
  "contract_version": "2.0",
  "transformation_id": "...",
  "source_ref": "source_id",
  "destination_ref": "destination_id",
  "field_mappings": [
    {
      "source_field": "src_id",
      "destination_field": "id",
      "transformations": [
        {"type": "cast", "params": {"target_type": "uuid"}}
      ]
    }
  ],
  "business_rules": [],
  "execution_plan": {
    "batch_size": 100,
    "error_threshold": 0.1
  }
}
```
