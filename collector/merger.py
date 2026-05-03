# merger.py
# Merges metadata from four sources into enriched TableMetadata objects.
#
# Priority rules per field:
#   row_count          → DuckDBCollector > Catalog > None
#   column type        → DuckDBCollector > Catalog > None
#   column description → Manifest > Catalog > None
#   model description  → Manifest only
#   lineage            → Manifest only
#   materialization    → Catalog > Manifest > None
#   schema             → DuckDBCollector > Catalog > None
#   last_refreshed     → RunResults > Catalog > None
#   rows_added         → RunResults only
#   tags, owner        → Manifest only
#   test results       → RunResults only
#
# Handles three cases:
#   1. Table in database and manifest — dbt model
#   2. Table in database only — raw table
#   3. Model in manifest only — not yet materialised, skip

import logging
from typing import List, Dict, Any, Optional
from .base import TableMetadata

logger = logging.getLogger(__name__)


class Merger:

    def __init__(
        self,
        database_tables: List[TableMetadata],
        manifest_data: Dict[str, Any],
        catalog_data: Dict[str, Any],
        run_results_data: Dict[str, Any]
    ):
        # database_tables — from DuckDBCollector.collect_metadata
        self.database_tables = database_tables

        # manifest_data — from ManifestParser.parse()
        # keyed by model name e.g. 'orders'
        self.manifest_data = manifest_data

        # catalog_data — from CatalogParser.parse()
        # keyed by model name e.g. 'orders'
        self.catalog_data = catalog_data

        # run_results_data — from RunResultsParser.parse()
        # keyed by model name e.g. 'orders'
        self.run_results_data = run_results_data

    def merge(self) -> List[TableMetadata]:
        enriched = []

        for table in self.database_tables:
            manifest_entry = self.manifest_data.get(table.name)
            catalog_entry = self.catalog_data.get(table.name)
            run_results_entry = self.run_results_data.get(table.name)

            if manifest_entry:
                # table exists in database and manifest — dbt model
                self._enrich_dbt_model(
                    table,
                    manifest_entry,
                    catalog_entry,
                    run_results_entry
                )
                logger.info(f"Merged dbt model: {table.name}")
            else:
                # table exists in database only — raw table
                table.is_dbt_model = False
                logger.info(
                    f"Raw table (no manifest entry): {table.name}"
                )

            enriched.append(table)

        logger.info(
            f"Merge complete: {len(enriched)} tables, "
            f"{sum(1 for t in enriched if t.is_dbt_model)} dbt models, "
            f"{sum(1 for t in enriched if not t.is_dbt_model)} raw tables"
        )
        return enriched

    def _enrich_dbt_model(
        self,
        table: TableMetadata,
        manifest_entry: Dict[str, Any],
        catalog_entry: Optional[Dict[str, Any]],
        run_results_entry: Optional[Dict[str, Any]]
    ) -> None:
        # mark as dbt model
        table.is_dbt_model = True

        # --- fields from manifest only ---
        table.dbt_description = manifest_entry.get('description')
        table.dbt_lineage = manifest_entry.get('lineage')
        table.dbt_owner = manifest_entry.get('owner')
        table.dbt_tags = manifest_entry.get('tags')
        table.dbt_has_description = manifest_entry.get('has_description')
        table.dbt_columns_documented_pct = manifest_entry.get(
            'columns_documented_pct'
        )
        table.dbt_upstream_count = manifest_entry.get('upstream_count')
        table.dbt_downstream_count = manifest_entry.get('downstream_count')

        # --- materialization: catalog > manifest ---
        if catalog_entry:
            table.dbt_materialization = catalog_entry.get(
                'materialization'
            ) or manifest_entry.get('materialization')
        else:
            table.dbt_materialization = manifest_entry.get(
                'materialization'
            )

        # --- row_count: DuckDBCollector > catalog ---
        # DuckDBCollector row_count is set during collect_metadata
        # only fall back to catalog if collector returned 0
        if table.row_count == 0 and catalog_entry:
            table.row_count = catalog_entry.get(
                'row_count'
            ) or table.row_count

        # --- last_refreshed: run_results > catalog ---
        if run_results_entry:
            table.dbt_last_refreshed = run_results_entry.get(
                'last_refreshed'
            )
            table.dbt_rows_added_last_refresh = run_results_entry.get(
                'rows_added'
            )
            table.dbt_refresh_duration_seconds = run_results_entry.get(
                'refresh_duration_seconds'
            )

        # --- column enrichment ---
        manifest_col_descriptions = manifest_entry.get(
            'column_descriptions', {}
        )
        catalog_columns = catalog_entry.get(
            'columns', {}
        ) if catalog_entry else {}

        for col in table.columns:
            # column description: manifest > catalog
            manifest_desc = manifest_col_descriptions.get(col.name, '')
            catalog_desc = catalog_columns.get(
                col.name, {}
            ).get('description', '')
            col.description = manifest_desc or catalog_desc or None

            # column type: DuckDBCollector already set dtype
            # only fill in from catalog if dtype is empty
            if not col.dtype and col.name in catalog_columns:
                col.dtype = catalog_columns[col.name].get('type', '')