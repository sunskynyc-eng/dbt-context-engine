# importance_ranker.py
# Scores and ranks TableMetadata objects by importance.
# Three dimensional score: reliability, usage, scale.
# Weights configurable in config.yaml.
#
# Scoring dimensions:
#   reliability — how trustworthy is this model
#   usage       — how much is this model actually used
#   scale       — how significant is this model in terms of data volume
#
# Freshness decay applied as multiplier to reliability score.
# dbt model bonus applied to final score.
# Models with no query history start at 0 for usage signals
# from QueryTracker — usage dimension still scores from
# downstream_count and exposure_count.

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from .base import TableMetadata
from .query_tracker import QueryTracker

logger = logging.getLogger(__name__)


@dataclass
class ImportanceScore:
    # multi-dimensional score for one TableMetadata object
    reliability:     float   # 0.0 to 1.0
    usage:           float   # 0.0 to 1.0
    scale:           float   # 0.0 to 1.0
    overall:         float   # weighted combination
    freshness_decay: float   # multiplier applied to reliability
    is_dbt_model:    bool
    rank:            int     # position in ranked list — set after sorting


class ImportanceRanker:

    def __init__(
        self,
        config: Dict[str, Any],
        query_tracker: QueryTracker
    ):
        # load weights from config — with sensible defaults
        ranking_cfg = config.get('importance_ranking', {})
        weights = ranking_cfg.get('weights', {})

        self.reliability_weight = weights.get('reliability', 0.4)
        self.usage_weight = weights.get('usage', 0.4)
        self.scale_weight = weights.get('scale', 0.2)
        self.dbt_model_bonus = ranking_cfg.get('dbt_model_bonus', 0.1)

        # freshness decay thresholds from config
        decay_cfg = ranking_cfg.get('freshness_decay', {})
        self.freshness_decay = {
            'under_24h':   decay_cfg.get('under_24h', 1.0),
            'under_7d':    decay_cfg.get('under_7d', 0.9),
            'under_30d':   decay_cfg.get('under_30d', 0.7),
            'under_90d':   decay_cfg.get('under_90d', 0.5),
            'under_365d':  decay_cfg.get('under_365d', 0.2),
            'over_365d':   decay_cfg.get('over_365d', 0.0)
        }

        self.query_tracker = query_tracker

    def rank(
        self,
        tables: List[TableMetadata]
    ) -> List[tuple]:
        # scores every table and returns sorted list of
        # (ImportanceScore, TableMetadata) tuples
        # highest overall score first

        # collect raw signal values for normalisation
        all_row_counts = [t.row_count for t in tables]
        all_downstream = [
            t.dbt_downstream_count or 0 for t in tables
        ]
        all_exposure = [
            t.dbt_exposure_count or 0 for t in tables
        ]
        all_durations = [
            t.dbt_refresh_duration_seconds or 0 for t in tables
        ]
        all_rows_added = [
            t.dbt_rows_added_last_refresh or 0 for t in tables
        ]
        all_upstream = [
            t.dbt_upstream_count or 0 for t in tables
        ]

        # collect query signals
        query_data = {}
        for table in tables:
            counts = self.query_tracker.get_counts(
                table.name, table.schema
            )
            query_data[table.name] = counts

        all_7d = [
            query_data[t.name]['avg_queries_7d']
            if query_data[t.name] else 0.0
            for t in tables
        ]
        all_30d = [
            query_data[t.name]['avg_queries_30d']
            if query_data[t.name] else 0.0
            for t in tables
        ]

        # score each table
        scored = []
        for table in tables:
            score = self._score_table(
                table=table,
                query_counts=query_data.get(table.name),
                all_row_counts=all_row_counts,
                all_downstream=all_downstream,
                all_exposure=all_exposure,
                all_durations=all_durations,
                all_rows_added=all_rows_added,
                all_upstream=all_upstream,
                all_7d=all_7d,
                all_30d=all_30d
            )
            scored.append((score, table))

        # sort by overall score descending
        scored.sort(key=lambda x: x[0].overall, reverse=True)

        # assign ranks
        for rank, (score, table) in enumerate(scored, start=1):
            score.rank = rank

        logger.info(f"Ranked {len(scored)} tables")
        return scored

    def _score_table(
        self,
        table: TableMetadata,
        query_counts: Optional[Any],
        all_row_counts: List[float],
        all_downstream: List[float],
        all_exposure: List[float],
        all_durations: List[float],
        all_rows_added: List[float],
        all_upstream: List[float],
        all_7d: List[float],
        all_30d: List[float]
    ) -> ImportanceScore:

        # freshness decay — applied as multiplier to reliability
        freshness_decay = self._get_freshness_decay(table)

        # --- reliability dimension ---
        test_pass_rate = 0.0
        if table.dbt_tests_defined and table.dbt_tests_defined > 0:
            test_pass_rate = (
                table.dbt_tests_passing or 0
            ) / table.dbt_tests_defined

        has_description = 1.0 if table.dbt_has_description else 0.0
        documented_pct = (
            table.dbt_columns_documented_pct or 0.0
        ) / 100.0

        # reliability raw score — average of three signals
        reliability_raw = (
            test_pass_rate * 0.4 +
            has_description * 0.3 +
            documented_pct * 0.3
        )

        # apply freshness decay
        reliability = reliability_raw * freshness_decay

        # --- usage dimension ---
        downstream_score = self._normalise(
            table.dbt_downstream_count or 0, all_downstream
        )
        exposure_score = self._normalise(
            table.dbt_exposure_count or 0, all_exposure
        )

        # query frequency signals — 0 if no history
        avg_7d = query_counts['avg_queries_7d'] if query_counts else 0.0
        avg_30d = query_counts['avg_queries_30d'] if query_counts else 0.0
        error_rate = (
            query_counts['error_rate'] or 0.0
        ) if query_counts else 0.0

        query_score = (
            0.5 * self._normalise(avg_7d, all_7d) +
            0.5 * self._normalise(avg_30d, all_30d)
        ) * (1 - error_rate / 100.0)

        # materialization bonus
        mat_score = self._materialization_score(
            table.dbt_materialization
        )

        # usage raw score
        usage = (
            downstream_score * 0.3 +
            exposure_score * 0.3 +
            query_score * 0.3 +
            mat_score * 0.1
        )

        # --- scale dimension ---
        row_count_score = self._normalise(
            table.row_count, all_row_counts
        )
        rows_added_score = self._normalise(
            table.dbt_rows_added_last_refresh or 0, all_rows_added
        )
        duration_score = self._normalise(
            table.dbt_refresh_duration_seconds or 0, all_durations
        )
        upstream_score = self._normalise(
            table.dbt_upstream_count or 0, all_upstream
        )

        scale = (
            row_count_score * 0.4 +
            rows_added_score * 0.3 +
            duration_score * 0.15 +
            upstream_score * 0.15
        )

        # --- overall score ---
        overall = (
            self.reliability_weight * reliability +
            self.usage_weight * usage +
            self.scale_weight * scale
        )

        # dbt model bonus
        if table.is_dbt_model:
            overall = min(1.0, overall + self.dbt_model_bonus)

        return ImportanceScore(
            reliability=round(reliability, 4),
            usage=round(usage, 4),
            scale=round(scale, 4),
            overall=round(overall, 4),
            freshness_decay=freshness_decay,
            is_dbt_model=table.is_dbt_model,
            rank=0  # assigned after sorting
        )

    def _get_freshness_decay(self, table: TableMetadata) -> float:
        # use dbt_last_refreshed for dbt models
        # use last_modified for raw tables
        # return 1.0 if no timestamp available — do not penalise
        timestamp = (
            table.dbt_last_refreshed
            if table.is_dbt_model
            else table.last_modified
        )

        if not timestamp:
            return 1.0

        try:
            last = datetime.fromisoformat(
                timestamp.replace('Z', '+00:00')
            )
            # make naive datetime timezone aware if needed
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            days_ago = (now - last).days

            if days_ago < 1:
                return self.freshness_decay['under_24h']
            elif days_ago < 7:
                return self.freshness_decay['under_7d']
            elif days_ago < 30:
                return self.freshness_decay['under_30d']
            elif days_ago < 90:
                return self.freshness_decay['under_90d']
            elif days_ago < 365:
                return self.freshness_decay['under_365d']
            else:
                return self.freshness_decay['over_365d']

        except Exception as e:
            logger.error(
                f"Failed to parse timestamp {timestamp}: {e}"
            )
            return 1.0

    def _materialization_score(
        self,
        materialization: Optional[str]
    ) -> float:
        # table and incremental are more important than view and ephemeral
        # raw tables have no materialization — score 0.5 as neutral
        scores = {
            'table':       1.0,
            'incremental': 0.9,
            'view':        0.5,
            'ephemeral':   0.1
        }
        if not materialization:
            return 0.5  # raw table — neutral
        return scores.get(materialization.lower(), 0.5)

    @staticmethod
    def _normalise(value: float, all_values: List[float]) -> float:
        # min-max normalisation — returns 0.0 to 1.0
        # returns 0.0 if all values are the same
        min_val = min(all_values)
        max_val = max(all_values)
        if max_val == min_val:
            return 0.0
        return (value - min_val) / (max_val - min_val)