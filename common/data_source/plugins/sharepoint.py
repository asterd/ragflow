"""Built-in SharePoint ingestion plugin."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from common.data_source.config import INDEX_BATCH_SIZE
from common.data_source.plugin_base import DataSourceSyncPlugin
from common.data_source.sharepoint_connector import SharePointConnector


class SharePointSyncPlugin(DataSourceSyncPlugin):
    _version_ = "1.0.0"

    @classmethod
    def get_source_name(cls) -> str:
        return "sharepoint"

    async def generate(self, task: dict[str, Any]) -> Any:
        connector = SharePointConnector(
            site_url=self.conf["site_url"],
            folder_paths=self._normalize_folder_paths(self.conf.get("folder_paths")),
            batch_size=self.conf.get("batch_size", INDEX_BATCH_SIZE),
        )
        connector.set_allow_images(self.conf.get("allow_images", False))
        connector.load_credentials(self.conf["credentials"])
        connector.validate_connector_settings()
        self.connector = connector

        if task["reindex"] == "1" or not task["poll_range_start"]:
            return connector.load_from_state()

        return connector.poll_source(
            task["poll_range_start"].timestamp(),
            datetime.now(timezone.utc).timestamp(),
        )

    def get_source_prefix(self) -> str:
        return "SharePoint"

    def describe_connection(
        self,
        task: dict[str, Any],
    ) -> tuple[str, str, str] | None:
        monitored_paths = self._normalize_folder_paths(self.conf.get("folder_paths"))
        monitored = ", ".join(monitored_paths) if monitored_paths else "/"
        extra = (
            f"folders={monitored}, batch_size={self.conf.get('batch_size', INDEX_BATCH_SIZE)}"
        )
        return ("SharePoint", self.conf["site_url"], extra)

    @staticmethod
    def _normalize_folder_paths(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if isinstance(value, str):
            return [
                item.strip()
                for item in value.replace("\r", "\n").replace(",", "\n").split("\n")
                if item.strip()
            ]
        return [str(value).strip()]
