# base.py
# Core data structures and abstract base class for the dbt-context-engine collector.
# Defines the schema for column and table metadata, and the interface
# every database collector must implement.
# Supports: DuckDB, Snowflake, BigQuery, Postgres, and any SQLAlchemy-compatible database.

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


@dataclass
class ColumnMetadata:
    # Represents a single column — structure only, no data values

    # Required
    name: str
    dtype: str

    # Optional — defaults used when information is unavailable
    nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_ref: Optional[str] = None  # e.g. "orders.customer_id"
    description: Optional[str] = None      # from dbt catalog

    def __post_init__(self):
        # Normalise to uppercase so type detection works across all databases
        self.dtype = self.dtype.upper()

    @property
    def is_numeric(self) -> bool:
        # Sampling: min, max, avg, stddev
        return self.dtype in [
            'INTEGER', 'FLOAT', 'DECIMAL', 'NUMERIC',
            'BIGINT', 'SMALLINT', 'TINYINT', 'HUGEINT',  # HUGEINT: DuckDB
            'DOUBLE', 'REAL', 'NUMBER'                    # NUMBER: Snowflake
        ]

    @property
    def is_text(self) -> bool:
        # Sampling: sample values, pattern detection, distinct count
        return self.dtype in [
            'VARCHAR', 'TEXT', 'STRING', 'CHAR',
            'NVARCHAR', 'NCHAR',  # SQL Server
            'CLOB',               # Oracle
            'BPCHAR'              # PostgreSQL internal name for CHAR
        ]

    @property
    def is_date(self) -> bool:
        # Sampling: min date, max date, date range
        # Also used by timezone inconsistency detector
        return self.dtype in [
            'DATE', 'TIMESTAMP', 'DATETIME',
            'TIMESTAMP_NTZ', 'TIMESTAMP_TZ', 'TIMESTAMP_LTZ',  # Snowflake
            'TIME', 'INTERVAL', 'TIMETZ'
        ]

    @property
    def is_boolean(self) -> bool:
        # Sampling: count true, false, null
        # Used by soft delete detector — is_deleted, is_active, is_current
        return self.dtype in [
            'BOOLEAN', 'BOOL'
        ]

    @property
    def is_semi_structured(self) -> bool:
        # Sampling: extract raw values and attempt key extraction
        return self.dtype in [
            'JSON', 'JSONB',          # PostgreSQL
            'VARIANT', 'OBJECT',      # Snowflake
            'ARRAY'                   # DuckDB and Snowflake
        ]

    @property
    def is_binary(self) -> bool:
        # Sampling: skip entirely — cannot be read as text
        return self.dtype in [
            'BLOB', 'BYTES', 'BINARY', 'VARBINARY'
        ]


@dataclass
class TableMetadata:
    # Represents a table or dbt model
    # Combines: database schema, manifest.json, catalog.json, run_results.json

    # Core — always populated
    name: str
    schema: str
    row_count: int
    columns: List[ColumnMetadata] = field(default_factory=list)  # fresh list per instance

    # dbt identity — None for raw tables
    is_dbt_model: bool = False
    dbt_description: Optional[str] = None
    dbt_lineage: Optional[List[str]] = None       # immediate upstream only
    dbt_materialization: Optional[str] = None     # table / view / incremental / ephemeral
    dbt_owner: Optional[str] = None               # technical owner
    dbt_tags: Optional[List[str]] = None          # e.g. ['finance', 'core']

    # Refresh metrics — from run_results.json
    dbt_last_refreshed: Optional[str] = None
    dbt_rows_added_last_refresh: Optional[int] = None   # incremental batch size for incremental models
    dbt_refresh_duration_seconds: Optional[float] = None

    # Data quality — from run_results.json test results
    dbt_tests_defined: Optional[int] = None
    dbt_tests_passing: Optional[int] = None
    dbt_test_last_run: Optional[str] = None

    # Sensitivity — dbt_contains_pii set by inference layer, not manually
    dbt_contains_pii: Optional[bool] = None
    dbt_access_level: Optional[str] = None        # public / internal / restricted / confidential

    # Documentation health — low values signal black box models
    dbt_has_description: Optional[bool] = None
    dbt_columns_documented_pct: Optional[float] = None
    dbt_last_modified_by: Optional[str] = None

    # Relationships — high downstream count signals critical infrastructure
    dbt_upstream_count: Optional[int] = None
    dbt_downstream_count: Optional[int] = None

    # Business context — requires human annotation, cannot be inferred
    dbt_business_owner: Optional[str] = None
    dbt_sla_hours: Optional[int] = None           # flag if last_refreshed exceeds this
    dbt_approximate_size_mb: Optional[float] = None

    def __str__(self) -> str:
        # e.g. main.orders (99 rows, dbt model)
        source = "dbt model" if self.is_dbt_model else "raw table"
        return (
            f"{self.schema}.{self.name} "
            f"({self.row_count:,} rows, {source})"
        )

    @property
    def column_names(self) -> List[str]:
        # Quick membership check — if 'is_deleted' in table.column_names
        return [c.name for c in self.columns]

    @property
    def primary_keys(self) -> List[ColumnMetadata]:
        # Used by grain mismatch detector
        return [c for c in self.columns if c.is_primary_key]


class BaseCollector(ABC):
    # Abstract base — interface is fixed, implementation varies per database
    # Pattern: Template Method — collect_all defines the algorithm, subclasses implement steps

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self._connected = False  # internal state — do not set externally

    @abstractmethod
    def test_connection(self) -> bool:
        # Verify database is reachable — must be implemented by every subclass
        pass

    @abstractmethod
    def collect_metadata(self) -> List[TableMetadata]:
        # Extract table and column structure — no data values
        pass

    @abstractmethod
    def collect_samples(
        self,
        schema_name: str,       # required — no default
        table_name: str,        # required — no default
        n: int = 20             # optional — has default
    ) -> List[Dict[str, Any]]:
        pass

    def collect_all(self) -> List[TableMetadata]:
        # Orchestrates full collection — fail loudly on connection failure
        if not self.test_connection():
            raise ConnectionError(
                f"Cannot connect to {self.connection_string}"
            )
        return self.collect_metadata()

    def __repr__(self) -> str:
        # Returns actual subclass name e.g. DuckDBCollector(connected=True)
        return (
            f"{self.__class__.__name__}"
            f"(connected={self._connected})"
        )