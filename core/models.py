"""Pydantic models for ingestion contracts"""

from typing import Any, Literal

from pydantic import BaseModel, Field

# ============================================================================
# Utility Models
# ============================================================================


class NumericFormatInfo(BaseModel):
    """Information about numeric format detected in data"""

    has_comma_decimal: bool = Field(description="Whether the number uses comma as decimal separator")
    has_thousands_sep: bool = Field(description="Whether the number uses thousands separator")
    format: Literal["european", "us"] = Field(description="Detected numeric format (european or us)")


class ColumnInfo(BaseModel):
    """Information about a database column"""

    name: str = Field(description="Column name")
    type: str = Field(description="Column data type")
    nullable: bool = Field(description="Whether the column allows NULL values")
    default: str | None = Field(default=None, description="Default value for the column")


class TableInfo(BaseModel):
    """Information about a database table"""

    table_name: str = Field(description="Table or view name")
    db_schema: str | None = Field(default=None, description="Database schema name", alias="schema")
    type: str = Field(description="Type of object (table or view)")
    has_primary_key: bool = Field(default=False, description="Whether the table has a primary key")
    primary_key_columns: list[str] = Field(default_factory=list, description="List of primary key column names")
    row_count: int | None = Field(default=None, description="Number of rows in the table")
    column_count: int | None = Field(default=None, description="Number of columns in the table")

    model_config = {"populate_by_name": True}


class ForeignKeyInfo(BaseModel):
    """Information about a foreign key constraint"""

    constraint_name: str | None = Field(default=None, description="Name of the foreign key constraint")
    columns: list[str] = Field(description="Columns in this table that form the foreign key")
    referred_table: str | None = Field(default=None, description="Table that is referenced (None if invalid FK)")
    referred_columns: list[str] = Field(description="Columns in the referred table")
    referred_schema: str | None = Field(default=None, description="Schema of the referred table")


class ReferencedByInfo(BaseModel):
    """Information about tables that reference this table via foreign keys"""

    constraint_name: str | None = Field(default=None, description="Name of the foreign key constraint")
    table: str = Field(description="Table that references this table")
    columns: list[str] = Field(description="Columns in the referencing table")
    referred_columns: list[str] = Field(description="Columns in this table that are referenced")


class RelationshipInfo(BaseModel):
    """Information about foreign key relationships for a table"""

    foreign_keys: list[ForeignKeyInfo] = Field(default_factory=list, description="Foreign keys from this table")
    referenced_by: list[ReferencedByInfo] = Field(default_factory=list, description="Tables that reference this table")


class TableMetadata(BaseModel):
    """Metadata for a database table analysis"""

    database_type: str = Field(description="Database type (postgresql, mysql, sqlite)")
    table_name: str = Field(description="Table name")
    db_schema: str | None = Field(default=None, description="Database schema name")
    primary_keys: list[str] = Field(default_factory=list, description="List of primary key column names")
    column_count: int = Field(description="Number of columns in the table")
    nullable_columns: list[str] = Field(default_factory=list, description="List of nullable column names")
    sample_size: int = Field(description="Number of rows sampled for analysis")
    columns: list[ColumnInfo] = Field(default_factory=list, description="Detailed column information")


class QueryMetadata(BaseModel):
    """Metadata for a database query analysis"""

    database_type: str = Field(description="Database type (postgresql, mysql, sqlite)")
    query: str = Field(description="SQL query that was analyzed")
    column_count: int = Field(description="Number of columns in the query result")
    sample_size: int = Field(description="Number of rows sampled for analysis")


class SchemaInfo(BaseModel):
    """Schema information extracted from a source (table or API)"""

    fields: list[str] = Field(description="List of column/field names")
    types: list[str] = Field(description="List of data types")
    constraints: dict[str, list[str]] = Field(
        default_factory=dict, description="Constraints (field_name -> list of constraint strings)"
    )


class EndpointInfo(BaseModel):
    """Information about an API endpoint"""

    method: str = Field(description="HTTP method")
    path: str = Field(description="Endpoint path")
    summary: str | None = Field(default=None, description="Endpoint summary/description")
    fields: list[str] | None = Field(default=None, description="List of field names (if schema extracted)")
    types: list[str] | None = Field(default=None, description="List of data types")
    constraints: dict[str, list[str]] | None = Field(default=None, description="Field constraints")
    error: str | None = Field(default=None, description="Error message if schema extraction failed")

    model_config = {"populate_by_name": True}


class SourceAnalysisResult(BaseModel):
    """Result of analyzing a data source file (CSV or JSON)"""

    file_type: str = Field(description="Type of file (csv or json)")
    encoding: str = Field(description="Detected file encoding")
    delimiter: str | None = Field(default=None, description="CSV delimiter (None for JSON)")
    has_header: bool | None = Field(default=None, description="Whether CSV has header row (None for JSON)")
    total_rows: int = Field(description="Total number of rows/objects in the file")
    sample_fields: list[str] = Field(description="List of field/column names")
    sample_data: list[list[str]] = Field(default_factory=list, description="Sample data rows (first 5 rows)")
    data_types: list[str] = Field(description="Detected data types for each field")
    issues: list[str] = Field(default_factory=list, description="Issues or warnings detected")


# ============================================================================
# Source Contract Models
# ============================================================================


class SourceSchema(BaseModel):
    """Schema definition for a data source"""

    fields: list["FieldDefinition"] = Field(default_factory=list, description="List of field definitions")


class QualityMetrics(BaseModel):
    """Quality metrics for a data source"""

    observed: "ObservedQuality | None" = Field(
        default=None, description="Observed data quality metrics (profiling output)"
    )
    expected: "ExpectedQuality | None" = Field(
        default=None, description="Expected data quality targets for validation"
    )


class FieldDefinition(BaseModel):
    """Schema definition for a single field"""

    name: str = Field(description="Field name")
    type: str = Field(description="Field data type")
    nullable: bool | None = Field(default=None, description="Whether the field allows NULL values")
    description: str | None = Field(default=None, description="Optional field description")
    examples: list[str] | None = Field(default=None, description="Example values for the field")
    constraints: list[str] | None = Field(default=None, description="Field constraints (e.g., primary_key)")
    classification: list[str] | None = Field(default=None, description="Data classification tags (e.g., pii)")
    sensitivity: str | None = Field(default=None, description="Sensitivity label (e.g., high, medium)")


class FieldMetric(BaseModel):
    """Profiling metrics for a single field"""

    field: str = Field(description="Field name")
    null_rate: float | None = Field(default=None, ge=0.0, le=1.0, description="Observed null rate")
    distinct_count: int | None = Field(default=None, ge=0, description="Observed distinct count")
    min: str | None = Field(default=None, description="Observed minimum value")
    max: str | None = Field(default=None, description="Observed maximum value")


class ObservedQuality(BaseModel):
    """Observed data quality metrics from profiling"""

    total_rows: int = Field(ge=0, description="Total number of rows in the source")
    sample_data: list[list[str]] = Field(default_factory=list, description="Sample data rows")
    issues: list[str] = Field(default_factory=list, description="List of quality issues detected")
    field_metrics: list[FieldMetric] | None = Field(
        default=None, description="Per-field profiling metrics"
    )


class ExpectedQuality(BaseModel):
    """Expected quality targets for validation"""

    required_fields: list[str] = Field(default_factory=list, description="Fields that must be present")
    max_missing_rate: float | None = Field(
        default=None, ge=0.0, le=1.0, description="Maximum allowed missing rate across fields"
    )
    rules: list[str] = Field(default_factory=list, description="Additional quality rules or expectations")


class SourceContract(BaseModel):
    """Contract describing a data source"""

    contract_version: str = Field(default="2.0", description="Version of the contract schema")
    contract_type: Literal["source"] = Field(default="source", description="Type of contract")
    source_id: str = Field(description="Unique identifier for this source (auto-generated if not provided)")
    # File-based sources
    source_path: str | None = Field(default=None, description="Path to the source data file")
    file_format: str | None = Field(default=None, description="File format (csv, json, parquet, etc.)")
    encoding: str | None = Field(default="utf-8", description="File encoding")
    delimiter: str | None = Field(default=None, description="Delimiter for CSV files")
    has_header: bool | None = Field(default=True, description="Whether the file has a header row")
    # Database-based sources
    database_type: str | None = Field(default=None, description="Database type (postgresql, mysql, sqlite)")
    source_type: str | None = Field(default=None, description="Source type (table, view, query)")
    source_name: str | None = Field(default=None, description="Table or view name")
    database_schema: str | None = Field(default=None, description="Database schema name")
    # Common fields
    data_schema: SourceSchema = Field(description="Schema information", alias="schema")
    quality_metrics: QualityMetrics = Field(description="Quality assessment")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

    model_config = {"populate_by_name": True}


# ============================================================================
# Destination Contract Models
# ============================================================================


class DestinationSchema(BaseModel):
    """Schema definition for a data destination"""

    fields: list[FieldDefinition] = Field(default_factory=list, description="List of field definitions")


class ValidationRule(BaseModel):
    """Validation rule for data"""

    id: str = Field(description="Rule identifier")
    type: str = Field(description="Rule type (e.g., range, format, uniqueness)")
    field: str | None = Field(default=None, description="Field the rule applies to")
    params: dict[str, Any] = Field(default_factory=dict, description="Rule parameters")
    severity: Literal["error", "warning"] = Field(default="error", description="Validation severity")


class ValidationRules(BaseModel):
    """Validation rules for data"""

    rules: list[ValidationRule] = Field(default_factory=list, description="Validation rules to apply")


class DestinationContract(BaseModel):
    """Contract describing a data destination"""

    contract_version: str = Field(default="2.0", description="Version of the contract schema")
    contract_type: Literal["destination"] = Field(default="destination", description="Type of contract")
    destination_id: str = Field(description="Unique identifier for this destination")
    data_schema: DestinationSchema = Field(description="Schema definition", alias="schema")
    validation_rules: ValidationRules = Field(default_factory=ValidationRules, description="Validation rules to apply")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

    model_config = {"populate_by_name": True}


# ============================================================================
# Transformation Contract Models
# ============================================================================


class FieldMapping(BaseModel):
    """Mapping definition between source and destination fields"""

    destination_field: str = Field(description="Destination field name")
    source_fields: list[str] = Field(description="Source field names")
    operation: str = Field(default="direct", description="Mapping operation (e.g., direct, concat, add)")
    params: dict[str, Any] = Field(default_factory=dict, description="Operation parameters")
    notes: str | None = Field(default=None, description="Optional mapping notes")


class TransformationStep(BaseModel):
    """Structured transformation step"""

    id: str = Field(description="Transformation step identifier")
    type: str = Field(description="Transformation type (e.g., cast, parse, normalize)")
    inputs: list[str] = Field(description="Input fields for the transformation")
    outputs: list[str] = Field(description="Output fields for the transformation")
    params: dict[str, Any] = Field(default_factory=dict, description="Transformation parameters")
    notes: str | None = Field(default=None, description="Optional transformation notes")


class ExecutionPlan(BaseModel):
    """Execution plan for data transformation"""

    batch_size: int = Field(default=100, ge=1, description="Number of records to process per batch")
    error_threshold: float = Field(default=0.1, ge=0.0, le=1.0, description="Maximum allowed error rate")
    validation_enabled: bool = Field(default=True, description="Whether to validate data")
    rollback_on_error: bool = Field(default=False, description="Whether to rollback on errors")


class TransformationContract(BaseModel):
    """Contract describing a data transformation from source to destination"""

    contract_version: str = Field(default="2.0", description="Version of the contract schema")
    contract_type: Literal["transformation"] = Field(default="transformation", description="Type of contract")
    transformation_id: str = Field(description="Unique identifier for this transformation")
    source_ref: str = Field(description="Reference to source contract ID")
    destination_ref: str = Field(description="Reference to destination contract ID")
    mapping: list["FieldMapping"] = Field(
        default_factory=list, description="Field mappings from source to destination", alias="field_mappings"
    )
    transformations: list["TransformationStep"] = Field(
        default_factory=list, description="Transformations to apply as structured steps"
    )
    business_rules: list[str] = Field(default_factory=list, description="Business rules to apply")
    execution_plan: ExecutionPlan = Field(default_factory=ExecutionPlan, description="Execution configuration")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")


# ============================================================================
# Type Alias for Any Contract
# ============================================================================

Contract = SourceContract | DestinationContract | TransformationContract
