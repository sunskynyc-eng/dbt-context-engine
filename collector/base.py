from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any


@dataclass
class ColumnMetadata:
    name: str
    dtype: str
    nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_ref: Optional[str] = None
    description: Optional[str] = None

    def __post_init__(self):
        self.dtype = self.dtype.upper()

    @property
    def is_numeric(self) -> bool:
        return self.dtype in [
            'INTEGER', 'FLOAT', 'DECIMAL',
            'NUMERIC', 'BIGINT'
        ]

    @property
    def is_text(self) -> bool:
        return self.dtype in [
            'VARCHAR', 'TEXT', 'STRING', 'CHAR'
        ]


@dataclass
class TableMetadata:
    name: str
    schema: str
    row_count: int
    columns: List[ColumnMetadata] = field(default_factory=list)
    is_dbt_model: bool = False
    dbt_description: Optional[str] = None
    dbt_lineage: Optional[List[str]] = None

    def __str__(self) -> str:
        source = "dbt model" if self.is_dbt_model else "raw table"
        return (
            f"{self.schema}.{self.name} "
            f"({self.row_count:,} rows, {source})"
        )

    @property
    def column_names(self) -> List[str]:
        return [c.name for c in self.columns]

    @property
    def primary_keys(self) -> List[ColumnMetadata]:
        return [c for c in self.columns if c.is_primary_key]


class BaseCollector(ABC):
    """
    Abstract base for all database collectors.

    Why abstract: each database engine requires different
    connection handling and introspection queries.
    The interface is fixed. The implementation varies.
    """

    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self._connected = False

    @abstractmethod
    def test_connection(self) -> bool:
        pass

    @abstractmethod
    def collect_metadata(self) -> List[TableMetadata]:
        pass

    @abstractmethod
    def collect_samples(
        self,
        table_name: str,
        n: int = 20
    ) -> List[Dict[str, Any]]:
        pass

    def collect_all(self) -> Dict[str, Any]:
        if not self.test_connection():
            raise ConnectionError(
                f"Cannot connect to {self.connection_string}"
            )
        metadata = self.collect_metadata()
        return {
            'tables': metadata,
            'table_count': len(metadata),
            'total_columns': sum(
                len(t.columns) for t in metadata
            ),
            'dbt_model_count': sum(
                1 for t in metadata if t.is_dbt_model
            )
        }

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}"
            f"(connected={self._connected})"
        )