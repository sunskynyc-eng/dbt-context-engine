from typing import List, Dict, Any
import sqlalchemy as sa
from .base import BaseCollector, TableMetadata, ColumnMetadata


class DuckDBCollector(BaseCollector):
    """
    Collects metadata from a DuckDB database.
    Used for local development against Jaffle Shop.
    """

    def __init__(self, database_path: str):
        connection_string = f"duckdb:///{database_path}"
        super().__init__(connection_string)
        self.database_path = database_path
        self._engine = None

    def test_connection(self) -> bool:
        try:
            self._engine = sa.create_engine(
                self.connection_string
            )
            with self._engine.connect() as conn:
                conn.execute(sa.text("SELECT 1"))
            self._connected = True
            return True
        except Exception as e:
            print(f"Connection failed: {e}")
            return False

    def collect_metadata(self) -> List[TableMetadata]:
        raise NotImplementedError("Coming next session")

    def collect_samples(
        self,
        table_name: str,
        n: int = 20
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError("Coming next session")