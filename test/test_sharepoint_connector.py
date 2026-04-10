from common.data_source.plugin_registry import DataSourcePluginRegistry
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
