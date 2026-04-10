"""Registry for built-in and external ingestion plugins."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import pluginlib

from common.data_source.plugin_base import (
    DataSourceSyncPlugin,
    PLUGIN_TYPE_DATA_SOURCE_SYNC,
)


logger = logging.getLogger(__name__)


class DataSourcePluginRegistry:
    def __init__(self) -> None:
        self._plugins: dict[str, type[DataSourceSyncPlugin]] = {}
        self._loaded = False

    def load_plugins(self) -> None:
        if self._loaded:
            return

        env_paths = [
            path.strip()
            for path in os.environ.get("RAGFLOW_CONNECTOR_PLUGIN_PATHS", "").split(os.pathsep)
            if path.strip()
        ]
        builtin_path = str(Path(__file__).with_name("plugins"))
        search_paths = env_paths + [builtin_path]
        loader = pluginlib.PluginLoader(paths=search_paths)

        for plugin_name, plugin_class in loader.plugins.get(
            PLUGIN_TYPE_DATA_SOURCE_SYNC, {}
        ).items():
            try:
                source_name = plugin_class.get_source_name()
            except Exception as exc:
                logger.exception(
                    "Failed to load data source plugin %s: %s",
                    plugin_name,
                    exc,
                )
                continue

            if source_name in self._plugins:
                logger.warning(
                    "Data source plugin source '%s' is already registered. Keeping %s and skipping %s.",
                    source_name,
                    self._plugins[source_name].__name__,
                    plugin_class.__name__,
                )
                continue

            self._plugins[source_name] = plugin_class
            logger.info(
                "Loaded data source plugin %s for source %s",
                plugin_class.__name__,
                source_name,
            )

        self._loaded = True

    def get_plugin(self, source_name: str) -> type[DataSourceSyncPlugin] | None:
        self.load_plugins()
        return self._plugins.get(source_name)

    def has_plugin(self, source_name: str) -> bool:
        self.load_plugins()
        return source_name in self._plugins


DATA_SOURCE_PLUGIN_REGISTRY = DataSourcePluginRegistry()
