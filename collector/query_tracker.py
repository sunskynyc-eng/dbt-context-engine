# query_tracker.py
# Tracks query frequency per model using a local SQLite database.
# Three table schema:
#   query_log        — OLTP — one row per query, feedback updated in place
#   query_model_refs — OLTP — one row per model per query
#   query_counts     — OLAP — pre-computed moving averages, read by importance ranker
#
# OLAP process recalculates query_counts from the two OLTP tables.
# Run after every N queries or on a schedule.

import sqlite3
import logging
from datetime import datetime
from typing import List, Tuple, Optional

logger = logging.getLogger(__name__)


class QueryTracker:

    def __init__(self, db_path: str):
        # path to the SQLite database file
        # e.g. cache/query_tracker.db
        self.db_path = db_path
        self._initialise_db()

    def _get_connection(self) -> sqlite3.Connection:
        # creates a new connection for each operation
        # WAL mode for safer concurrent writes
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.row_factory = sqlite3.Row
        return conn

    def _initialise_db(self) -> None:
        # creates all three tables if they do not exist
        # safe to call multiple times — IF NOT EXISTS guards
        with self._get_connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS query_log (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    question            TEXT,
                    sql_generated       TEXT,
                    execution_status    TEXT,
                    confidence_score    REAL,
                    was_correct         INTEGER,
                    incorrect_reason    TEXT,
                    queried_at          TEXT NOT NULL,
                    feedback_at         TEXT
                );

                CREATE TABLE IF NOT EXISTS query_model_refs (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    query_id    INTEGER NOT NULL,
                    model_name  TEXT NOT NULL,
                    schema_name TEXT NOT NULL,
                    FOREIGN KEY (query_id) REFERENCES query_log(id)
                );

                CREATE TABLE IF NOT EXISTS query_counts (
                    model_name          TEXT NOT NULL,
                    schema_name         TEXT NOT NULL,
                    query_count_total   INTEGER DEFAULT 0,
                    avg_queries_7d      REAL DEFAULT 0.0,
                    avg_queries_30d     REAL DEFAULT 0.0,
                    query_trend         REAL DEFAULT 0.0,
                    error_rate          REAL DEFAULT 0.0,
                    last_queried        TEXT,
                    first_queried       TEXT,
                    last_computed       TEXT,
                    PRIMARY KEY (model_name, schema_name)
                );
            """)
        logger.info(f"QueryTracker initialised at {self.db_path}")

    def log_query(
        self,
        question: str,
        sql_generated: str,
        execution_status: str,
        confidence_score: Optional[float] = None
    ) -> int:
        # inserts one row into query_log
        # returns the new row id for use in log_model_refs
        queried_at = datetime.now().isoformat()
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO query_log (
                    question,
                    sql_generated,
                    execution_status,
                    confidence_score,
                    queried_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    question,
                    sql_generated,
                    execution_status,
                    confidence_score,
                    queried_at
                )
            )
            query_id = cursor.lastrowid
            logger.info(f"Logged query id={query_id}")
            return query_id

    def log_model_refs(
        self,
        query_id: int,
        models: List[Tuple[str, str]]
    ) -> None:
        # inserts one row per model referenced in the query
        # models is a list of (model_name, schema_name) tuples
        # extracted from the generated SQL via sqlglot
        with self._get_connection() as conn:
            conn.executemany(
                """
                INSERT INTO query_model_refs (
                    query_id,
                    model_name,
                    schema_name
                ) VALUES (?, ?, ?)
                """,
                [
                    (query_id, model_name, schema_name)
                    for model_name, schema_name in models
                ]
            )
        logger.info(
            f"Logged {len(models)} model refs for query id={query_id}"
        )

    def update_feedback(
        self,
        query_id: int,
        was_correct: bool,
        incorrect_reason: Optional[str] = None
    ) -> None:
        # updates was_correct and feedback_at for an existing query
        # called when analyst provides feedback after seeing the result
        feedback_at = datetime.now().isoformat()
        with self._get_connection() as conn:
            conn.execute(
                """
                UPDATE query_log
                SET
                    was_correct = ?,
                    incorrect_reason = ?,
                    feedback_at = ?
                WHERE id = ?
                """,
                (
                    1 if was_correct else 0,
                    incorrect_reason,
                    feedback_at,
                    query_id
                )
            )
        logger.info(
            f"Updated feedback for query id={query_id} "
            f"was_correct={was_correct}"
        )

    def refresh_counts(self) -> None:
        # OLAP process — recomputes query_counts from OLTP tables
        # run after every N queries or on a schedule
        # moving averages: 7d and 30d
        # query_trend: 7d minus 30d
        # error_rate: percentage of queries marked incorrect
        with self._get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO query_counts (
                    model_name,
                    schema_name,
                    query_count_total,
                    avg_queries_7d,
                    avg_queries_30d,
                    query_trend,
                    error_rate,
                    last_queried,
                    first_queried,
                    last_computed
                )
                SELECT
                    r.model_name,
                    r.schema_name,

                    COUNT(*) as query_count_total,

                    -- 7 day moving average
                    ROUND(
                        SUM(CASE
                            WHEN q.queried_at >= datetime('now', '-7 days')
                            THEN 1 ELSE 0
                        END) / 7.0,
                        2
                    ) as avg_queries_7d,

                    -- 30 day moving average
                    ROUND(
                        SUM(CASE
                            WHEN q.queried_at >= datetime('now', '-30 days')
                            THEN 1 ELSE 0
                        END) / 30.0,
                        2
                    ) as avg_queries_30d,

                    -- trend: 7d minus 30d
                    -- positive = increasing usage
                    -- negative = declining usage
                    ROUND(
                        SUM(CASE
                            WHEN q.queried_at >= datetime('now', '-7 days')
                            THEN 1 ELSE 0
                        END) / 7.0
                        -
                        SUM(CASE
                            WHEN q.queried_at >= datetime('now', '-30 days')
                            THEN 1 ELSE 0
                        END) / 30.0,
                        2
                    ) as query_trend,

                    -- error rate — only from queries with feedback
                    ROUND(
                        SUM(CASE WHEN q.was_correct = 0 THEN 1 ELSE 0 END)
                        * 100.0
                        / NULLIF(
                            SUM(CASE
                                WHEN q.was_correct IS NOT NULL THEN 1 ELSE 0
                            END),
                            0
                        ),
                        1
                    ) as error_rate,

                    MAX(q.queried_at) as last_queried,
                    MIN(q.queried_at) as first_queried,
                    datetime('now') as last_computed

                FROM query_model_refs r
                JOIN query_log q ON r.query_id = q.id
                GROUP BY r.model_name, r.schema_name
            """)
        logger.info("query_counts refreshed")

    def get_counts(
        self,
        model_name: str,
        schema_name: str
    ) -> Optional[sqlite3.Row]:
        # returns query_counts row for a specific model
        # returns None if no query history exists — treated as 0 by ranker
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT *
                FROM query_counts
                WHERE model_name = ?
                AND schema_name = ?
                """,
                (model_name, schema_name)
            )
            return cursor.fetchone()

    def __repr__(self) -> str:
        return f"QueryTracker(db={self.db_path})"