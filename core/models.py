"""Pydantic models for ingestion contracts"""

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

# ============================================================================
# Utility Models
# ============================================================================


class NumericFormatInfo(BaseModel):
    """Information about numeric format detected in data"""

    has_comma_decimal: bool = Field(description="Whether the number uses comma as decimal separator")
    has_thousands_sep: bool = Field(description="Whether the number uses thousands separator")
    format: Literal["european", "us"] = Field(description="Detected numeric format (european or us)")


# ============================================================================
# Field Property Models
# ============================================================================


class FieldConstraint(BaseModel):
    """Specific constraint for a field"""

    type: Literal["not_null", "unique", "range", "pattern", "enum", "foreign_key", "primary_key"] = Field(
        description="Type of constraint"
    )
    value: Any | None = Field(default=None, description="Constraint value (e.g., regex pattern, range, enum list)")
    min_value: Any | None = Field(default=None, description="Minimum value for range constraint")
    max_value: Any | None = Field(default=None, description="Maximum value for range constraint")
    referred_table: str | None = Field(default=None, description="Table referenced by foreign key")
    referred_column: str | None = Field(default=None, description="Column referenced by foreign key")


class FieldProfile(BaseModel):
    """Profiling statistics for a field"""

    null_count: int = Field(default=0, ge=0, description="Number of null values")
    null_percentage: float = Field(default=0.0, ge=0.0, le=100.0, description="Percentage of null values")
    distinct_count: int = Field(default=0, ge=0, description="Number of distinct values")
    min_value: Any | None = Field(default=None, description="Minimum value found")
    max_value: Any | None = Field(default=None, description="Maximum value found")
    sample_values: list[Any] = Field(default_factory=list, description="Sample of values found in the field")


class FieldDefinition(BaseModel):
    """Complete definition of a field/column"""

    name: str = Field(description="Field name")
    data_type: str = Field(description="Data type (e.g., string, integer, date)")
    nullable: bool = Field(default=True, description="Whether the field can be null")
    description: str | None = Field(default=None, description="Field description/business meaning")
    constraints: list[FieldConstraint] = Field(default_factory=list, description="List of constraints for this field")
    profiling: FieldProfile | None = Field(default=None, description="Profiling information")


class FieldTransformation(BaseModel):
    """Definition of a data transformation"""

    type: Literal["rename", "cast", "format", "lookup", "calculate", "default"] = Field(
        description="Type of transformation"
    )
    parameters: dict[str, Any] = Field(default_factory=dict, description="Transformation-specific parameters")


class FieldMapping(BaseModel):
    """Mapping from destination field to source field(s)"""

    destination_field: str = Field(description="Name of the destination field")
    source_field: str | None = Field(default=None, description="Name of the source field (None for computed fields)")
    transformation: FieldTransformation | None = Field(default=None, description="Transformation to apply")


# ============================================================================
# Quality Models
# ============================================================================


class QualityExpectation(BaseModel):
    """Expected quality thresholds and rules"""

    max_null_percentage: float | None = Field(
        default=None, ge=0.0, le=100.0, description="Maximum allowed null percentage"
    )
    min_distinct_count: int | None = Field(default=None, ge=0, description="Minimum allowed distinct values")
    allowed_values: list[Any] | None = Field(default=None, description="List of allowed values (enum)")


class QualityObservation(BaseModel):
    """Actual observed quality vs expectation"""

    total_rows: int = Field(default=0, ge=0, description="Total number of rows analyzed")
    expectation: QualityExpectation | None = Field(
        default=None, description="The expectations this was checked against"
    )
    observed_profiling: dict[str, FieldProfile] = Field(
        default_factory=dict, description="Observed profiling per field"
    )
    issues: list[str] = Field(default_factory=list, description="List of quality issues or violations detected")
    sample_data: list[list[Any]] = Field(default_factory=list, description="Sample data rows")


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


class SupabaseMetadata(BaseModel):
    """Metadata for a Supabase table analysis"""

    project_url: str = Field(description="Supabase project URL")
    table_name: str = Field(description="Table name")
    primary_keys: list[str] = Field(default_factory=list, description="List of primary key column names")
    column_count: int = Field(description="Number of columns in the table")
    nullable_columns: list[str] = Field(default_factory=list, description="List of nullable column names")
    sample_size: int = Field(description="Number of rows sampled for analysis")
    columns: list[ColumnInfo] = Field(default_factory=list, description="Detailed column information")


class SchemaInfo(BaseModel):
    """Schema information extracted from a source (table or API)"""

    fields: list[FieldDefinition] = Field(description="List of field definitions")


class EndpointInfo(BaseModel):
    """Information about an API endpoint"""

    method: str = Field(description="HTTP method")
    path: str = Field(description="Endpoint path")
    summary: str | None = Field(default=None, description="Endpoint summary/description")
    fields: list[FieldDefinition] = Field(default_factory=list, description="List of field definitions")
    error: str | None = Field(default=None, description="Error message if schema extraction failed")

    model_config = {"populate_by_name": True}


class SourceAnalysisResult(BaseModel):
    """Result of analyzing a data source file (CSV or JSON)"""

    file_type: str = Field(description="Type of file (csv or json)")
    encoding: str = Field(description="Detected file encoding")
    delimiter: str | None = Field(default=None, description="CSV delimiter (None for JSON)")
    has_header: bool | None = Field(default=None, description="Whether CSV has header row (None for JSON)")
    total_rows: int = Field(description="Total number of rows/objects in the file")
    field_profiles: dict[str, FieldProfile] = Field(default_factory=dict, description="Profiling information per field")
    sample_fields: list[str] = Field(description="List of field/column names")
    sample_data: list[list[str]] = Field(default_factory=list, description="Sample data rows (first 5 rows)")
    data_types: list[str] = Field(description="Detected data types for each field")
    issues: list[str] = Field(default_factory=list, description="Issues or warnings detected")


# ============================================================================
# Source Contract Models
# ============================================================================


class SourceSchema(BaseModel):
    """Schema definition for a data source"""

    fields: list[FieldDefinition] = Field(description="List of field definitions")


class QualityMetrics(BaseModel):
    """Quality metrics for a data source"""

    total_rows: int = Field(ge=0, description="Total number of rows in the source")
    sample_data: list[list[str]] = Field(default_factory=list, description="Sample data rows")
    issues: list[str] = Field(default_factory=list, description="List of quality issues detected")


class BaseSourceContract(BaseModel):
    """Base contract for all data sources"""

    contract_type: Literal["source"] = Field(default="source", description="Type of contract")
    source_format: str = Field(description="Format of the data source (csv, json, database)")
    source_id: str = Field(description="Unique identifier for this source")
    data_schema: SourceSchema = Field(description="Schema information", alias="schema")
    quality: QualityObservation = Field(description="Quality assessment")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
    )

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """Custom model_dump to exclude None and empty collections by default"""
        kwargs.setdefault("exclude_none", True)
        kwargs.setdefault("exclude_unset", False)
        kwargs.setdefault("exclude_defaults", False)
        kwargs.setdefault("by_alias", True)
        return super().model_dump(*args, **kwargs)


class CSVSourceContract(BaseSourceContract):
    """Contract for CSV data sources"""

    source_format: Literal["csv"] = "csv"
    source_path: str = Field(description="Path to the CSV file")
    encoding: str = Field(default="utf-8", description="File encoding")
    delimiter: str = Field(description="CSV delimiter character")
    has_header: bool = Field(default=True, description="Whether the file has a header row")


class JSONSourceContract(BaseSourceContract):
    """Contract for JSON/NDJSON data sources"""

    source_format: Literal["json"] = "json"
    source_path: str = Field(description="Path to the JSON file")
    encoding: str = Field(default="utf-8", description="File encoding")
    is_ndjson: bool = Field(default=False, description="Whether file is newline-delimited JSON")


class DatabaseSourceContract(BaseSourceContract):
    """Contract for database data sources"""

    source_format: Literal["database"] = "database"
    database_type: Literal["postgresql", "mysql", "sqlite"] = Field(description="Database type")
    source_type: Literal["table", "view", "query"] = Field(description="Type of database source")
    source_name: str = Field(description="Table, view, or query name")
    database_schema: str | None = Field(default=None, description="Database schema name (if applicable)")


class SupabaseSourceContract(BaseSourceContract):
    """Contract for Supabase data sources"""

    source_format: Literal["supabase"] = "supabase"
    project_url: str = Field(description="Supabase project URL")
    table_name: str = Field(description="Table name")


# Source contract discriminated union - Pydantic automatically dispatches based on source_format
SourceContract = Annotated[
    CSVSourceContract | JSONSourceContract | DatabaseSourceContract | SupabaseSourceContract,
    Field(discriminator="source_format"),
]

# TypeAdapter for validating SourceContract discriminated union
_source_contract_adapter: TypeAdapter[
    CSVSourceContract | JSONSourceContract | DatabaseSourceContract | SupabaseSourceContract
] = TypeAdapter(SourceContract)


def validate_source_contract(
    data: dict[str, Any] | str,
) -> CSVSourceContract | JSONSourceContract | DatabaseSourceContract | SupabaseSourceContract:
    """Validate and parse a source contract from dict or JSON string.

    Uses Pydantic's TypeAdapter to properly handle the discriminated union.

    Args:
        data: Contract data as dict or JSON string

    Returns:
        Validated source contract (CSV, JSON, or Database type)

    Raises:
        ValidationError: If the data doesn't match the contract schema
    """
    if isinstance(data, str):
        return _source_contract_adapter.validate_json(data)
    return _source_contract_adapter.validate_python(data)


# ============================================================================
# Destination Contract Models
# ============================================================================


class DestinationSchema(BaseModel):
    """Schema definition for a data destination"""

    fields: list[FieldDefinition] = Field(default_factory=list, description="List of field definitions")


class ValidationRules(BaseModel):
    """Validation rules for data"""

    required_fields: list[str] = Field(default_factory=list, description="Fields that must be present")
    unique_constraints: list[str] = Field(default_factory=list, description="Fields that must be unique")
    data_range_checks: dict[str, Any] = Field(default_factory=dict, description="Range checks for numeric fields")
    format_validation: dict[str, Any] = Field(default_factory=dict, description="Format validation patterns")


class DestinationContract(BaseModel):
    """Contract describing a data destination"""

    contract_type: Literal["destination"] = Field(default="destination", description="Type of contract")
    destination_id: str = Field(description="Unique identifier for this destination")
    data_schema: DestinationSchema = Field(description="Schema definition", alias="schema")
    validation_rules: ValidationRules = Field(default_factory=ValidationRules, description="Validation rules to apply")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
    )

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """Custom model_dump to exclude None and empty collections by default"""
        kwargs.setdefault("exclude_none", True)
        kwargs.setdefault("exclude_unset", False)
        kwargs.setdefault("exclude_defaults", False)
        kwargs.setdefault("by_alias", True)
        return super().model_dump(*args, **kwargs)


# ============================================================================
# Transformation Contract Models
# ============================================================================


class ExecutionPlan(BaseModel):
    """Execution plan for data transformation"""

    batch_size: int = Field(default=100, ge=1, description="Number of records to process per batch")
    error_threshold: float = Field(default=0.1, ge=0.0, le=1.0, description="Maximum allowed error rate")
    validation_enabled: bool = Field(default=True, description="Whether to validate data")
    rollback_on_error: bool = Field(default=False, description="Whether to rollback on errors")


class TransformationContract(BaseModel):
    """Contract describing a data transformation from source to destination"""

    contract_type: Literal["transformation"] = Field(default="transformation", description="Type of contract")
    transformation_id: str = Field(description="Unique identifier for this transformation")
    source_ref: str = Field(description="Reference to source contract ID")
    destination_ref: str = Field(description="Reference to destination contract ID")
    field_mappings: list[FieldMapping] = Field(
        default_factory=list, description="Mapping from destination fields to source fields"
    )
    business_rules: list[Any] = Field(default_factory=list, description="Business rules to apply")
    execution_plan: ExecutionPlan = Field(default_factory=ExecutionPlan, description="Execution configuration")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

    model_config = ConfigDict(
        populate_by_name=True,
        extra="ignore",
    )

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """Custom model_dump to exclude None and empty collections by default"""
        kwargs.setdefault("exclude_none", True)
        kwargs.setdefault("exclude_unset", False)
        kwargs.setdefault("exclude_defaults", False)
        kwargs.setdefault("by_alias", True)
        return super().model_dump(*args, **kwargs)


# ============================================================================
# Type Alias for Any Contract
# ============================================================================

Contract = SourceContract | DestinationContract | TransformationContract
