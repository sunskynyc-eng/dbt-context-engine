# writer.py
# Writes snapshot files from enriched TableMetadata objects and sample data.
# One file per table in a timestamped directory per collection run.
# dbt models written first ordered by importance rank.
# Raw tables written second ordered by importance rank.
# _manifest.txt summarises the full collection run.
#
# Output is deterministic — no inference, no LLM enrichment.
# All values come directly from the collector pipeline.

import logging
import os
import shutil
from datetime import datetime
from decimal import Decimal
from typing import Any, List, TYPE_CHECKING

from collector.base import TableMetadata
from collector.importance_ranker import ImportanceScore
from collector.sample_store import SampleStore

logger = logging.getLogger(__name__)

SEPARATOR = '=' * 64
COLUMN_SEPARATOR = '-' * 64


class SnapshotWriter:

    def __init__(
        self,
        output_dir: str,
        keep_last_n_runs: int
    ):
        # output_dir — root snapshots directory e.g. snapshots/
        # keep_last_n_runs — number of collection runs to retain
        self.output_dir = output_dir
        self.keep_last_n_runs = keep_last_n_runs

    def write(
        self,
        ranked_tables: List[tuple],
        sample_store: SampleStore
    ) -> str:
        # creates timestamped directory
        # writes dbt models first, raw tables second
        # writes _manifest.txt
        # cleans up old runs
        # returns path to output directory

        # timestamp generated once — consistent across all files
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        run_dir = os.path.join(self.output_dir, timestamp)
        os.makedirs(run_dir, exist_ok=True)
        logger.info(f"Writing snapshot to {run_dir}")

        # total table count for rank display e.g. '1 of 8'
        total_tables = len(ranked_tables)

        # separate dbt models and raw tables
        dbt_models = [
            (score, table) for score, table in ranked_tables
            if table.is_dbt_model
        ]
        raw_tables = [
            (score, table) for score, table in ranked_tables
            if not table.is_dbt_model
        ]

        # write dbt models first — already sorted by rank
        for score, table in dbt_models:
            self._write_table_file(
                score=score,
                table=table,
                total_tables=total_tables,
                sample_store=sample_store,
                run_dir=run_dir
            )

        # write raw tables second — already sorted by rank
        for score, table in raw_tables:
            self._write_table_file(
                score=score,
                table=table,
                total_tables=total_tables,
                sample_store=sample_store,
                run_dir=run_dir
            )

        # write manifest
        self._write_manifest(
            ranked_tables=ranked_tables,
            run_dir=run_dir,
            timestamp=timestamp
        )

        # clean up old runs
        self._cleanup_old_runs()

        logger.info(
            f"Snapshot complete: {total_tables} tables written to {run_dir}"
        )
        return run_dir

    def _write_table_file(
        self,
        score: ImportanceScore,
        table: TableMetadata,
        total_tables: int,
        sample_store: SampleStore,
        run_dir: str
    ) -> None:
        # filename: schema.table.txt
        filename = f"{table.schema}.{table.name}.txt"
        file_path = os.path.join(run_dir, filename)

        with open(file_path, 'w') as f:
            # header
            f.write(f"{SEPARATOR}\n")
            f.write(f"TABLE:      {table.name}\n")
            f.write(f"SCHEMA:     {table.schema}\n")
            source = "dbt model" if table.is_dbt_model else "raw table"
            f.write(f"SOURCE:     {source}\n")
            f.write(f"RANK:       {score.rank} of {total_tables}\n")
            f.write(f"IMPORTANCE: {score.overall:.4f}\n")
            f.write(
                f"  reliability: {score.reliability:.4f}\n"
            )
            f.write(
                f"  usage:       {score.usage:.4f}\n"
            )
            f.write(
                f"  scale:       {score.scale:.4f}\n"
            )
            f.write(
                f"  freshness:   {score.freshness_decay:.4f}\n"
            )
            f.write(f"{SEPARATOR}\n")

            if table.is_dbt_model:
                self._write_dbt_sections(f, table)
            else:
                # raw table — last modified only
                f.write("\nLAST MODIFIED\n")
                f.write(
                    f"  {table.last_modified or 'unknown'}\n"
                )

            # columns — all tables
            self._write_columns(f, table)

            # sample data — all tables
            self._write_sample_data(f, table, sample_store)

            f.write(f"\n{SEPARATOR}\n")

        logger.info(f"Wrote {file_path}")

    def _write_dbt_sections(
        self,
        f,
        table: TableMetadata
    ) -> None:
        # IDENTITY section
        f.write("\nIDENTITY\n")
        f.write(
            f"  materialization:  "
            f"{table.dbt_materialization or 'unknown'}\n"
        )
        f.write(
            f"  owner:            "
            f"{table.dbt_owner or 'unknown'}\n"
        )
        tags = ', '.join(table.dbt_tags) if table.dbt_tags else 'none'
        f.write(f"  tags:             {tags}\n")
        f.write(
            f"  last_refreshed:   "
            f"{table.dbt_last_refreshed or 'unknown'}\n"
        )
        if table.dbt_refresh_duration_seconds is not None:
            f.write(
                f"  refresh_duration: "
                f"{table.dbt_refresh_duration_seconds:.2f} seconds\n"
            )
        if table.dbt_rows_added_last_refresh is not None:
            f.write(
                f"  rows_added:       "
                f"{table.dbt_rows_added_last_refresh:,}\n"
            )

        # DESCRIPTION section
        f.write("\nDESCRIPTION\n")
        if table.dbt_description:
            f.write(f"  {table.dbt_description}\n")
        else:
            f.write("  no description available\n")

        # QUALITY section
        f.write("\nQUALITY\n")
        tests_defined = table.dbt_tests_defined or 0
        tests_passing = table.dbt_tests_passing or 0
        f.write(
            f"  tests:            "
            f"{tests_defined} defined, {tests_passing} passing\n"
        )
        f.write(
            f"  documented:       "
            f"{table.dbt_columns_documented_pct or 0:.1f}% columns\n"
        )
        has_desc = 'yes' if table.dbt_has_description else 'no'
        f.write(f"  has_description:  {has_desc}\n")

        # RELATIONSHIPS section
        f.write("\nRELATIONSHIPS\n")
        f.write(
            f"  upstream:         "
            f"{table.dbt_upstream_count or 0} models\n"
        )
        f.write(
            f"  downstream:       "
            f"{table.dbt_downstream_count or 0} models\n"
        )
        f.write(
            f"  exposures:        "
            f"{table.dbt_exposure_count or 0}\n"
        )
        if table.dbt_lineage:
            lineage = ', '.join(table.dbt_lineage)
            f.write(
                f"  lineage (immediate upstream): {lineage}\n"
            )

    def _write_columns(
        self,
        f,
        table: TableMetadata
    ) -> None:
        f.write("\nCOLUMNS\n")
        f.write(f"  {'name':<30} {'type':<20} flags\n")
        f.write(f"  {COLUMN_SEPARATOR}\n")

        for col in table.columns:
            flags = []
            if col.is_primary_key:
                flags.append('PK')
            if col.is_foreign_key and col.foreign_key_ref:
                flags.append(f"FK → {col.foreign_key_ref}")
            if not col.nullable:
                flags.append('not null')
            flags_str = '  '.join(flags)
            f.write(
                f"  {col.name:<30} {col.dtype:<20} {flags_str}\n"
            )

        # COLUMN DESCRIPTIONS section
        described = [
            col for col in table.columns
            if col.description
        ]
        if described:
            f.write("\nCOLUMN DESCRIPTIONS\n")
            for col in described:
                f.write(f"  {col.name}: {col.description}\n")

    def _write_sample_data(
        self,
        f,
        table: TableMetadata,
        sample_store: SampleStore
    ) -> None:
        samples = sample_store.read(table.schema, table.name)
        if not samples:
            f.write("\nSAMPLE DATA\n")
            f.write("  no sample data available\n")
            return

        f.write(f"\nSAMPLE DATA\n")
        f.write(
            f"  rows shown: {len(samples)} of {table.row_count:,}\n"
        )
        f.write(f"  {COLUMN_SEPARATOR}\n")

        if not samples:
            return

        # get column names from first row
        col_names = list(samples[0].keys())

        # serialise all values
        serialised = []
        for row in samples:
            serialised.append({
                col: self._truncate(self._serialise_value(row[col]))
                for col in col_names
            })

        # calculate column widths
        col_widths = {
            col: max(
                len(col),
                max(len(row[col]) for row in serialised)
            )
            for col in col_names
        }

        # write header row
        header = '  ' + ' | '.join(
            col.ljust(col_widths[col]) for col in col_names
        )
        f.write(f"{header}\n")
        f.write(f"  {COLUMN_SEPARATOR}\n")

        # write data rows
        for row in serialised:
            line = '  ' + ' | '.join(
                row[col].ljust(col_widths[col]) for col in col_names
            )
            f.write(f"{line}\n")

    def _write_manifest(
        self,
        ranked_tables: List[tuple],
        run_dir: str,
        timestamp: str
    ) -> None:
        file_path = os.path.join(run_dir, '_manifest.txt')

        dbt_models = [
            (score, table) for score, table in ranked_tables
            if table.is_dbt_model
        ]
        raw_tables = [
            (score, table) for score, table in ranked_tables
            if not table.is_dbt_model
        ]

        with open(file_path, 'w') as f:
            f.write(f"{SEPARATOR}\n")
            f.write("dbt-context-engine snapshot\n")
            f.write(f"generated:    {timestamp}\n")
            schemas = set(t.schema for _, t in ranked_tables)
            f.write(f"schemas:      {len(schemas)}\n")
            f.write(
                f"tables:       {len(ranked_tables)} "
                f"({len(dbt_models)} dbt models, "
                f"{len(raw_tables)} raw tables)\n"
            )
            f.write(f"{SEPARATOR}\n")

            f.write("\nDBT MODELS (ranked by importance)\n")
            for score, table in dbt_models:
                f.write(
                    f"  {score.rank}. "
                    f"{table.schema}.{table.name:<30} "
                    f"{score.overall:.4f}\n"
                )

            f.write("\nRAW TABLES (ranked by importance)\n")
            for score, table in raw_tables:
                f.write(
                    f"  {score.rank}. "
                    f"{table.schema}.{table.name:<30} "
                    f"{score.overall:.4f}\n"
                )

            f.write(f"\n{SEPARATOR}\n")

        logger.info(f"Wrote manifest to {file_path}")

    def _cleanup_old_runs(self) -> None:
        # lists all run directories sorted chronologically
        # deletes oldest beyond keep_last_n_runs
        if not os.path.exists(self.output_dir):
            return

        runs = sorted([
            d for d in os.listdir(self.output_dir)
            if os.path.isdir(os.path.join(self.output_dir, d))
            and d != '__pycache__'
        ])

        runs_to_delete = runs[:-self.keep_last_n_runs]
        for run in runs_to_delete:
            run_path = os.path.join(self.output_dir, run)
            shutil.rmtree(run_path)
            logger.info(f"Deleted old run: {run_path}")

    @staticmethod
    def _serialise_value(value: Any) -> str:
        # converts any Python value to display string
        if value is None:
            return ''
        if isinstance(value, bool):
            return 'true' if value else 'false'
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        return str(value)

    @staticmethod
    def _truncate(value: str, max_width: int = 50) -> str:
        # truncates string at max_width with '...' suffix
        if len(value) <= max_width:
            return value
        return value[:max_width - 3] + '...'