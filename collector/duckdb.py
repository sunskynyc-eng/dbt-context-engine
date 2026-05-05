# duckdb.py
# DuckDB implementation of BaseCollector.
# Used for local development against Jaffle Shop.
# Production databases use their own collector subclass.

import logging
import os
from datetime import datetime
from typing import List, Dict, Any, Optional

import sqlalchemy as sa
from sqlalchemy import text

from .base import BaseCollector, TableMetadata, ColumnMetadata
from .sample_store import SampleStore
from .utils import calculate_sample_size

logger = logging.getLogger(__name__)


class DuckDBCollector(BaseCollector):

    def __init__(self, database_path: str, config: dict):
        connection_string = f"duckdb:///{database_path}"
        super().__init__(connection_string, config)
        self.database_path = database_path
        self._engine = None

    def test_connection(self) -> bool:
        try:
            self._engine = sa.create_engine(self.connection_string)
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self._connected = True
            logger.info(
                f"Connected to DuckDB at {self.database_path}"
            )
            return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False

    def collect_metadata(
        self,
        sample_store: SampleStore
    ) -> List[TableMetadata]:
        # check connection before proceeding
        if not self._connected:
            raise ConnectionError(
                "Not connected. Call test_connection() first."
            )

        tables = []

        with self._engine.connect() as conn:
            # get all schemas from DuckDB information_schema
            schemas = conn.execute(text("""
                SELECT DISTINCT table_schema
                FROM information_schema.tables
                WHERE table_type = 'BASE TABLE'
            """)).fetchall()

            for (schema_name,) in schemas:
                if self._should_skip_schema(schema_name):
                    continue

                logger.info(f"Collecting schema: {schema_name}")

                # get all tables in schema
                table_names = conn.execute(text("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = :schema
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """), {'schema': schema_name}).fetchall()

                for (table_name,) in table_names:
                    logger.info(
                        f"Collecting: {schema_name}.{table_name}"
                    )
                    try:
                        # get column info
                        col_rows = conn.execute(text("""
                            SELECT
                                column_name,
                                data_type,
                                is_nullable
                            FROM information_schema.columns
                            WHERE table_schema = :schema
                            AND table_name = :table
                            ORDER BY ordinal_position
                        """), {
                            'schema': schema_name,
                            'table': table_name
                        }).fetchall()

                        # get primary keys
                        pk_rows = conn.execute(text("""
                            SELECT kcu.column_name
                            FROM information_schema.table_constraints tc
                            JOIN information_schema.key_column_usage kcu
                                ON tc.constraint_name = kcu.constraint_name
                                AND tc.table_schema = kcu.table_schema
                            WHERE tc.constraint_type = 'PRIMARY KEY'
                            AND tc.table_schema = :schema
                            AND tc.table_name = :table
                        """), {
                            'schema': schema_name,
                            'table': table_name
                        }).fetchall()
                        pk_columns = {row[0] for row in pk_rows}

                        # get foreign keys
                        fk_rows = conn.execute(text("""
                            SELECT
                                kcu.column_name,
                                ccu.table_name AS referred_table,
                                ccu.column_name AS referred_column
                            FROM information_schema.table_constraints tc
                            JOIN information_schema.key_column_usage kcu
                                ON tc.constraint_name = kcu.constraint_name
                                AND tc.table_schema = kcu.table_schema
                            JOIN information_schema.constraint_column_usage ccu
                                ON ccu.constraint_name = tc.constraint_name
                            WHERE tc.constraint_type = 'FOREIGN KEY'
                            AND tc.table_schema = :schema
                            AND tc.table_name = :table
                        """), {
                            'schema': schema_name,
                            'table': table_name
                        }).fetchall()
                        fk_lookup = {
                            row[0]: f"{row[1]}.{row[2]}"
                            for row in fk_rows
                        }

                        # build column metadata
                        columns = []
                        for col_name, data_type, is_nullable in col_rows:
                            columns.append(ColumnMetadata(
                                name=col_name,
                                dtype=data_type,
                                nullable=is_nullable == 'YES',
                                is_primary_key=col_name in pk_columns,
                                is_foreign_key=col_name in fk_lookup,
                                foreign_key_ref=fk_lookup.get(col_name)
                            ))

                        # get row count
                        row_count = self._get_row_count(
                            schema_name, table_name
                        )

                        # calculate sample size
                        sample_size = calculate_sample_size(row_count)

                        # collect samples
                        samples = self.collect_samples(
                            schema_name=schema_name,
                            table_name=table_name,
                            n=sample_size
                        )
                        sample_store.write(
                            schema_name, table_name, samples
                        )

                        tables.append(TableMetadata(
                            name=table_name,
                            schema=schema_name,
                            row_count=row_count,
                            columns=columns,
                            is_dbt_model=False,
                            last_modified=self._get_last_modified(
                                schema_name, table_name
                            )
                        ))

                        logger.info(
                            f"{schema_name}.{table_name}: "
                            f"{len(columns)} columns, "
                            f"{row_count} rows, "
                            f"{len(samples)} samples written to store"
                        )

                    except Exception as e:
                        logger.error(
                            f"Failed to collect "
                            f"{schema_name}.{table_name}: {e}"
                        )
                        continue

        logger.info(f"Collection complete: {len(tables)} tables")
        return tables

    def collect_samples(
        self,
        schema_name: str,
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
                    text(
                        f"SELECT * FROM "
                        f"{schema_name}.{table_name} LIMIT {n}"
                    )
                )
                rows = result.fetchall()
                columns = result.keys()
                return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error(
                f"Failed to sample "
                f"{schema_name}.{table_name}: {e}"
            )
            return []

    def _get_row_count(
        self,
        schema_name: str,
        table_name: str
    ) -> int:
        # uses exact COUNT(*) — acceptable for DuckDB local development
        # at scale use database statistics instead of full table scan:
        # Snowflake: information_schema.tables.row_count
        # PostgreSQL: pg_class.reltuples
        # BigQuery: INFORMATION_SCHEMA.TABLE_STORAGE.row_count
        try:
            with self._engine.connect() as conn:
                result = conn.execute(
                    text(
                        f"SELECT COUNT(*) FROM "
                        f"{schema_name}.{table_name}"
                    )
                )
                return result.scalar()
        except Exception as e:
            logger.error(
                f"Failed to get row count for "
                f"{schema_name}.{table_name}: {e}"
            )
            return 0

    def _get_last_modified(
        self,
        schema_name: str,
        table_name: str
    ) -> Optional[str]:
        # DuckDB does not expose per-table DML timestamps natively
        # file modification time used as proxy for local development
        # at scale use database-specific metadata:
        # Snowflake: information_schema.tables.last_altered
        # BigQuery:  INFORMATION_SCHEMA.TABLE_STORAGE.last_modified_time
        # PostgreSQL: pg_stat_user_tables.last_autoanalyze
        try:
            mtime = os.path.getmtime(self.database_path)
            return datetime.fromtimestamp(mtime).isoformat()
        except Exception as e:
            logger.error(
                f"Failed to get last modified for "
                f"{schema_name}.{table_name}: {e}"
            )
            return None