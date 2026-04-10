"""Plugin interfaces for ingestion data sources."""

from __future__ import annotations

from typing import Any

import pluginlib


PLUGIN_TYPE_DATA_SOURCE_SYNC = "data_source_sync"


@pluginlib.Parent(PLUGIN_TYPE_DATA_SOURCE_SYNC)
class DataSourceSyncPlugin:
    def __init__(self, conf: dict[str, Any]) -> None:
        self.conf = conf

    @classmethod
    @pluginlib.abstractmethod
    def get_source_name(cls) -> str:
        raise NotImplementedError

    @pluginlib.abstractmethod
    async def generate(self, task: dict[str, Any]) -> Any:
        raise NotImplementedError

    def get_source_prefix(self) -> str:
        return ""

    def describe_connection(
        self,
        task: dict[str, Any],
    ) -> tuple[str, str, str] | None:
        return None
