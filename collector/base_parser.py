# base_parser.py
# Abstract base class for all dbt artifact parsers.
# Handles hash-based change detection and JSON caching.
# Subclasses implement _cache_name, _preprocess, and _parse_file only.
#
# Pattern: Template Method
# parse() owns the algorithm — check hash, load cache or parse, save
# Subclasses implement the file-specific parsing logic only
#
# Caller is responsible for passing paths — BaseParser does not read config
#
# Memory note: _preprocess reduces raw JSON before parsing
# ijson streaming is not used in this iteration — see DESIGN.md

import json
import os
from abc import ABC, abstractmethod

from .utils import has_file_changed, save_file_hash


class BaseParser(ABC):

    def __init__(self, source_path: str, cache_dir: str):
        # path to the dbt artifact file to parse
        # e.g. /path/to/dbt/project/target/manifest.json
        self.source_path = source_path

        # directory where hash and cache files are stored
        # created automatically if it does not exist
        self.cache_dir = cache_dir

        # path to the hash file — stores MD5 of last parsed source
        # used to detect whether the source file has changed
        self.hash_path = os.path.join(
            cache_dir, f"{self._cache_name()}.hash"
        )

        # path to the cache file — stores the parsed output as JSON
        # loaded directly when source file has not changed
        self.cache_path = os.path.join(
            cache_dir, f"{self._cache_name()}.cache.json"
        )

    @abstractmethod
    def _cache_name(self) -> str:
        # returns identifier used for cache files e.g. 'manifest'
        pass

    @abstractmethod
    def _preprocess(self, data: dict) -> dict:
        # receives full raw JSON loaded from source file
        # returns reduced dictionary containing only fields needed
        # keeps memory usage low for large artifact files
        # e.g. ManifestParser keeps only 'nodes' and 'child_map'
        pass

    @abstractmethod
    def _parse_file(self, data: dict) -> dict:
        # receives preprocessed reduced dictionary from _preprocess
        # returns extracted structured data ready for caching
        pass

    def parse(self) -> dict:
        # Final method — do not override in subclasses
        # Override _preprocess() and _parse_file() instead
        # check both hash and cache file exist before using cache
        # cache file may have been deleted manually while hash remains
        if (not has_file_changed(self.source_path, self.hash_path)
                and os.path.exists(self.cache_path)):
            return self.__load_cache()
        raw = self.__load_source()
        preprocessed = self._preprocess(raw)    # reduce before parsing
        result = self._parse_file(preprocessed)
        save_file_hash(self.source_path, self.hash_path)
        self.__save_cache(result)
        return result

    def __load_source(self) -> dict:
        # reads raw JSON from source file
        with open(self.source_path, 'r') as f:
            return json.load(f)

    def __load_cache(self) -> dict:
        # reads previously cached parse result
        with open(self.cache_path, 'r') as f:
            return json.load(f)

    def __save_cache(self, data: dict) -> None:
        # writes parse result to cache for future runs
        os.makedirs(self.cache_dir, exist_ok=True)
        with open(self.cache_path, 'w') as f:
            json.dump(data, f, indent=2)

    def __repr__(self) -> str:
        # e.g. ManifestParser(source=manifest.json)
        return (
            f"{self.__class__.__name__}"
            f"(source={os.path.basename(self.source_path)})"
        )