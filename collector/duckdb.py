# duckdb.py
# DuckDB implementation of BaseCollector.
# Used for local development against Jaffle Shop.
# Production databases use their own collector subclass.

import logging
from typing import List, Dict, Any

import sqlalchemy as sa
from sqlalchemy import inspect, text

from .base import BaseCollector, TableMetadata, ColumnMetadata
from .utils import calculate_sample_size

logger = logging.getLogger(__name__)


class DuckDBCollector(BaseCollector):

    def __init__(self, database_path: str):
        connection_string = f"duckdb:///{database_path}"
        super().__init__(connection_string)
        self.database_path = database_path
        self._engine = None

    def test_connection(self) -> bool:
        try:
            self._engine = sa.create_engine(self.connection_string)
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self._connected = True
            logger.info(f"Connected to DuckDB at {self.database_path}")
            return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False

    def collect_metadata(self) -> List[TableMetadata]:
        # check connection before proceeding
        if not self._connected:
            raise ConnectionError(
                "Not connected. Call test_connection() first."
            )

        tables = []
        inspector = inspect(self._engine)

        for table_name in inspector.get_table_names():
            logger.info(f"Collecting metadata for: {table_name}")

            try:
                # build primary key set
                pk_columns = set(
                    inspector.get_pk_constraint(
                        table_name
                    ).get('constrained_columns', [])
                )

                # build foreign key lookup
                fk_lookup = {}
                for fk in inspector.get_foreign_keys(table_name):
                    for col in fk['constrained_columns']:
                        referred_table = fk['referred_table']
                        referred_col = fk['referred_columns'][0]
                        fk_lookup[col] = (
                            f"{referred_table}.{referred_col}"
                        )

                # build column metadata
                columns = []
                for col in inspector.get_columns(table_name):
                    col_name = col['name']
                    columns.append(ColumnMetadata(
                        name=col_name,
                        dtype=str(col['type']),
                        nullable=col.get('nullable', True),
                        is_primary_key=col_name in pk_columns,
                        is_foreign_key=col_name in fk_lookup,
                        foreign_key_ref=fk_lookup.get(col_name)
                    ))

                # get row count
                row_count = self._get_row_count(table_name)

                # calculate sample size based on row count
                sample_size = calculate_sample_size(row_count)

                # get sample rows
                samples = self.collect_samples(
                    table_name, n=sample_size
                )

                tables.append(TableMetadata(
                    name=table_name,
                    schema='main',
                    row_count=row_count,
                    columns=columns,
                    is_dbt_model=False  # merger sets this later
                ))

                logger.info(
                    f"{table_name}: {len(columns)} columns, "
                    f"{row_count} rows, {len(samples)} samples"
                )

            except Exception as e:
                # one table failing does not crash the whole collection
                logger.error(
                    f"Failed to collect {table_name}: {e}"
                )
                continue

        logger.info(f"Collection complete: {len(tables)} tables")
        return tables

    def collect_samples(
        self,
        table_name: str,
        n: int = 20
    ) -> List[Dict[str, Any]]:
        if not self._connected:
            raise ConnectionError(
                "Not connected. Call test_connection() first."
            )
        try:
            with self._engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT * FROM {table_name} LIMIT {n}")
                )
                rows = result.fetchall()
                columns = result.keys()
                return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error(f"Failed to sample {table_name}: {e}")
            return []

    def _get_row_count(self, table_name: str) -> int:
        # get exact row count for sample size calculation
        try:
            with self._engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT COUNT(*) FROM {table_name}")
                )
                return result.scalar()
        except Exception as e:
            logger.error(
                f"Failed to get row count for {table_name}: {e}"
            )
            return 0