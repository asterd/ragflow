import asyncio
from datetime import datetime, timezone

from common.data_source.plugin_registry import DataSourcePluginRegistry
from common.data_source.plugins import sharepoint as sharepoint_plugin
from common.data_source.plugins.sharepoint import SharePointSyncPlugin
from common.data_source.sharepoint_connector import SharePointConnector


def test_normalize_sharepoint_site_relative_folder_path():
    connector = SharePointConnector(
        site_url="https://contoso.sharepoint.com/sites/Knowledge",
        folder_paths=[],
    )
    connector._site_server_relative_url = "/sites/Knowledge"

    assert (
        connector._normalize_folder_path("Shared Documents/Policies")
        == "/sites/Knowledge/Shared Documents/Policies"
    )


def test_normalize_sharepoint_full_url_folder_path():
    connector = SharePointConnector(
        site_url="https://contoso.sharepoint.com/sites/Knowledge",
        folder_paths=[],
    )
    connector._site_server_relative_url = "/sites/Knowledge"

    assert (
        connector._normalize_folder_path(
            "https://contoso.sharepoint.com/sites/Knowledge/Shared%20Documents/Policies"
        )
        == "/sites/Knowledge/Shared Documents/Policies"
    )


def test_builtin_sharepoint_plugin_is_registered(monkeypatch):
    monkeypatch.delenv("RAGFLOW_CONNECTOR_PLUGIN_PATHS", raising=False)

    registry = DataSourcePluginRegistry()
    plugin = registry.get_plugin("sharepoint")

    assert plugin is not None
    assert plugin.get_source_name() == "sharepoint"


def test_empty_folder_paths_defaults_to_site_root():
    connector = SharePointConnector(
        site_url="https://contoso.sharepoint.com/sites/Knowledge",
        folder_paths=[],
    )
    connector._site_server_relative_url = "/sites/Knowledge"

    assert connector._get_normalized_folder_paths() == ["/sites/Knowledge"]


def test_sharepoint_plugin_can_sync_deleted_files(monkeypatch):
    class DummyConnector:
        def __init__(self, site_url, folder_paths=None, batch_size=2):
            self.site_url = site_url
            self.folder_paths = folder_paths or []
            self.batch_size = batch_size
            self.poll_args = None

        def set_allow_images(self, allow_images):
            self.allow_images = allow_images

        def load_credentials(self, credentials):
            self.credentials = credentials

        def validate_connector_settings(self):
            return None

        def list_current_files(self):
            return ["existing-file"]

        def load_from_state(self):
            return iter([["full-sync"]])

        def poll_source(self, start, end):
            self.poll_args = (start, end)
            return iter([["incremental-sync"]])

    monkeypatch.setattr(sharepoint_plugin, "SharePointConnector", DummyConnector)

    plugin = SharePointSyncPlugin(
        {
            "site_url": "https://contoso.sharepoint.com/sites/Knowledge",
            "folder_paths": [],
            "batch_size": 2,
            "sync_deleted_files": True,
            "credentials": {
                "tenant_id": "tenant",
                "client_id": "client",
                "client_secret": "secret",
            },
        }
    )

    result = asyncio.run(
        plugin.generate(
            {
                "reindex": "0",
                "poll_range_start": datetime(2025, 1, 1, tzinfo=timezone.utc),
            }
        )
    )

    document_batches, current_files = result

    assert list(document_batches) == [["incremental-sync"]]
    assert current_files == ["existing-file"]
