# sample_store.py
# Manages temporary storage of sample data during collection.
# Samples are written to disk per table and read by the snapshot writer.
# All temp files are cleaned up after use regardless of success or failure.
#
# Why temp files over in-memory storage:
# Sample sizes scale with table count and sample percentage.
# At 1000 tables with 1000 rows per table samples can exceed
# available memory. Temp files keep memory usage constant
# regardless of project size.
#
# Usage:
#   with SampleStore() as store:
#       store.write('main', 'orders', samples)
#       samples = store.read('main', 'orders')

import json
import os
import tempfile
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class SampleStore:
    """
    Context manager that writes sample rows to temp files during collection.
    Reads them back during snapshot writing.
    Cleans up all temp files on close — even on failure.
    """

    def __init__(self):
        # temp directory created on enter, deleted on exit
        self._temp_dir = None
        # maps schema.table → file path
        self._file_map: Dict[str, str] = {}

    def __enter__(self) -> 'SampleStore':
        # create a dedicated temp directory for this collection run
        self._temp_dir = tempfile.mkdtemp(prefix='dbt_context_')
        logger.info(f"Sample store created at {self._temp_dir}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        # clean up all temp files regardless of success or failure
        self._cleanup()
        # return False — do not suppress exceptions
        return False

    def write(
        self,
        schema_name: str,
        table_name: str,
        samples: List[Dict[str, Any]]
    ) -> None:
        # schema_name before table_name — consistent with collect_samples
        if not self._temp_dir:
            raise RuntimeError(
                "SampleStore must be used as a context manager"
            )

        # key by schema.table to avoid name collisions across schemas
        key = f"{schema_name}.{table_name}"

        # replace dots in key for safe filename
        safe_filename = key.replace('.', '_') + '.json'
        file_path = os.path.join(self._temp_dir, safe_filename)

        try:
            with open(file_path, 'w') as f:
                json.dump(samples, f)
            self._file_map[key] = file_path
            logger.info(
                f"Wrote {len(samples)} samples for {key}"
            )
        except Exception as e:
            logger.error(
                f"Failed to write samples for {key}: {e}"
            )

    def read(
        self,
        schema_name: str,
        table_name: str
    ) -> Optional[List[Dict[str, Any]]]:
        # returns None if samples were not written for this table
        key = f"{schema_name}.{table_name}"
        file_path = self._file_map.get(key)

        if not file_path:
            logger.warning(f"No samples found for {key}")
            return None

        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(
                f"Failed to read samples for {key}: {e}"
            )
            return None

    def tables(self) -> List[str]:
        # returns list of all schema.table keys that have samples
        return list(self._file_map.keys())

    def _cleanup(self) -> None:
        # delete all temp files then the temp directory
        # runs on exit regardless of success or failure
        if not self._temp_dir:
            return

        for key, file_path in self._file_map.items():
            try:
                os.remove(file_path)
                logger.info(f"Deleted temp file for {key}")
            except Exception as e:
                logger.error(
                    f"Failed to delete temp file {file_path}: {e}"
                )

        try:
            os.rmdir(self._temp_dir)
            logger.info(
                f"Sample store cleaned up: {self._temp_dir}"
            )
        except Exception as e:
            logger.error(
                f"Failed to delete temp dir {self._temp_dir}: {e}"
            )