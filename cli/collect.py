# collect.py
# CLI entry point for the dbt-context-engine collection pipeline.
# Orchestrates the full pipeline from database collection to snapshot output.
#
# Pipeline:
#   read config
#   → connect to database
#   → collect metadata and samples (inside SampleStore context)
#   → parse manifest, catalog, run_results
#   → merge all sources
#   → rank by importance
#   → write snapshot files
#
# Usage:
#   python -m cli.collect --config config.local.yaml

import argparse
import logging
import os
import sys
import yaml

from collector.base import TableMetadata
from collector.duckdb import DuckDBCollector
from collector.manifest_parser import ManifestParser
from collector.catalog_parser import CatalogParser
from collector.run_results_parser import RunResultsParser
from collector.merger import Merger
from collector.query_tracker import QueryTracker
from collector.importance_ranker import ImportanceRanker
from collector.sample_store import SampleStore
from snapshots.writer import SnapshotWriter

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> dict:
    # loads base config.yaml then merges local config on top
    # local config contains machine-specific paths only
    # base config contains all shared settings
    base_path = os.path.join(
        os.path.dirname(os.path.abspath(config_path)),
        'config.yaml'
    )
    if not os.path.exists(base_path):
        logger.error(f"Base config not found: {base_path}")
        sys.exit(1)
    with open(base_path, 'r') as f:
        base = yaml.safe_load(f)

    if not os.path.exists(config_path):
        logger.error(f"Local config not found: {config_path}")
        sys.exit(1)
    with open(config_path, 'r') as f:
        local = yaml.safe_load(f)

    # merge local on top of base — local wins on conflict
    base.update(local)
    return base


def run(config_path: str) -> None:
    # orchestrates the full collection pipeline
    logger.info(f"Loading config from {config_path}")
    cfg = load_config(config_path)

    # --- paths from config ---
    database_path = cfg['database']['path']
    target_dir = cfg['dbt']['target_dir']
    cache_dir = cfg['cache']['dir']
    snapshots_dir = cfg['output']['snapshots_dir']
    keep_last_n_runs = cfg['output']['keep_last_n_runs']
    query_tracker_db = cfg['cache']['query_tracker_db']

    # --- ensure directories exist ---
    os.makedirs(cache_dir, exist_ok=True)
    os.makedirs(snapshots_dir, exist_ok=True)

    # --- collector ---
    logger.info(f"Connecting to database: {database_path}")
    collector = DuckDBCollector(database_path, cfg)

    # --- parsers ---
    manifest_parser = ManifestParser(
        source_path=os.path.join(target_dir, 'manifest.json'),
        cache_dir=cache_dir
    )
    catalog_parser = CatalogParser(
        source_path=os.path.join(target_dir, 'catalog.json'),
        cache_dir=cache_dir
    )
    run_results_parser = RunResultsParser(
        source_path=os.path.join(target_dir, 'run_results.json'),
        cache_dir=cache_dir
    )

    # --- collection and snapshot inside SampleStore context ---
    with SampleStore() as sample_store:

        # collect metadata and samples
        logger.info("Starting collection")
        tables = collector.collect_all(sample_store)
        logger.info(f"Collected {len(tables)} tables")

        # parse dbt artifacts
        logger.info("Parsing dbt artifacts")
        manifest_data = manifest_parser.parse()
        catalog_data = catalog_parser.parse()
        run_results_data = run_results_parser.parse()

        # merge
        logger.info("Merging sources")
        merger = Merger(
            database_tables=tables,
            manifest_data=manifest_data,
            catalog_data=catalog_data,
            run_results_data=run_results_data
        )
        enriched = merger.merge()

        # rank
        logger.info("Ranking tables")
        query_tracker = QueryTracker(query_tracker_db)
        ranker = ImportanceRanker(cfg, query_tracker)
        ranked = ranker.rank(enriched)

        # write snapshot
        logger.info("Writing snapshot")
        writer = SnapshotWriter(
            output_dir=snapshots_dir,
            keep_last_n_runs=keep_last_n_runs
        )
        output_dir = writer.write(ranked, sample_store)

    logger.info(f"Collection complete. Snapshot written to {output_dir}")


def main():
    parser = argparse.ArgumentParser(
        description='dbt-context-engine collector'
    )
    parser.add_argument(
        '--config',
        required=True,
        help='path to config yaml file'
    )
    args = parser.parse_args()
    run(args.config)


if __name__ == '__main__':
    main()