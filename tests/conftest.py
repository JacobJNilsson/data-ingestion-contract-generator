"""Pytest configuration and shared fixtures"""

from pathlib import Path

import pytest

from core.models import (
    DestinationContract,
    DestinationSchema,
    ExecutionPlan,
    FieldDefinition,
    FieldMapping,
    QualityObservation,
    SourceContract,
    SourceSchema,
    TransformationContract,
    ValidationRules,
)


@pytest.fixture
def fixtures_dir() -> Path:
    """Return the path to the fixtures directory"""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_csv_path(fixtures_dir: Path) -> Path:
    """Return the path to the sample CSV file"""
    return fixtures_dir / "sample_transactions.csv"


@pytest.fixture
def temp_contract_path(tmp_path: Path) -> Path:
    """Return a temporary path for contract files"""
    return tmp_path / "test_contract.json"


@pytest.fixture
def sample_source_contract() -> SourceContract:
    """Return a sample source contract"""
    return SourceContract(
        source_id="test_source",
        source_path="/path/to/test.csv",
        file_format="csv",
        encoding="utf-8",
        delimiter=",",
        has_header=True,
        schema=SourceSchema(
            fields=[
                FieldDefinition(name="date", data_type="date"),
                FieldDefinition(name="amount", data_type="numeric"),
                FieldDefinition(name="description", data_type="text"),
            ]
        ),
        quality=QualityObservation(
            total_rows=100,
            sample_data=[],
            issues=[],
        ),
        metadata={},
    )


@pytest.fixture
def sample_destination_contract() -> DestinationContract:
    """Return a sample destination contract"""
    return DestinationContract(
        destination_id="test_destination",
        schema=DestinationSchema(
            fields=[
                FieldDefinition(name="id", data_type="uuid"),
                FieldDefinition(name="date", data_type="date"),
                FieldDefinition(name="amount", data_type="decimal"),
            ]
        ),
        validation_rules=ValidationRules(
            required_fields=["id", "date"],
            unique_constraints=["id"],
            data_range_checks={},
            format_validation={},
        ),
        metadata={},
    )


@pytest.fixture
def sample_transformation_contract() -> TransformationContract:
    """Return a sample transformation contract"""
    return TransformationContract(
        transformation_id="test_transformation",
        source_ref="test_source",
        destination_ref="test_destination",
        field_mappings=[
            FieldMapping(source_field="date", destination_field="date"),
            FieldMapping(source_field="amount", destination_field="amount"),
        ],
        business_rules=[],
        execution_plan=ExecutionPlan(
            batch_size=100,
            error_threshold=0.1,
            validation_enabled=True,
            rollback_on_error=False,
        ),
        metadata={},
    )


@pytest.fixture
def saved_source_contract(tmp_path: Path, sample_source_contract: SourceContract) -> Path:
    """Save a sample source contract and return its path"""
    contract_path = tmp_path / "source_contract.json"
    with contract_path.open("w") as f:
        f.write(sample_source_contract.model_dump_json(indent=2, by_alias=True))
    return contract_path


@pytest.fixture
def saved_destination_contract(tmp_path: Path, sample_destination_contract: DestinationContract) -> Path:
    """Save a sample destination contract and return its path"""
    contract_path = tmp_path / "destination_contract.json"
    with contract_path.open("w") as f:
        f.write(sample_destination_contract.model_dump_json(indent=2, by_alias=True))
    return contract_path


@pytest.fixture
def saved_transformation_contract(tmp_path: Path, sample_transformation_contract: TransformationContract) -> Path:
    """Save a sample transformation contract and return its path"""
    contract_path = tmp_path / "transformation_contract.json"
    with contract_path.open("w") as f:
        f.write(sample_transformation_contract.model_dump_json(indent=2, by_alias=True))
    return contract_path
