"""SharePoint connector."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote, urlparse

import msal
from office365.runtime.client_request_exception import ClientRequestException
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File as SharePointFile

from common.data_source.config import (
    BLOB_STORAGE_SIZE_THRESHOLD,
    DocumentSource,
    INDEX_BATCH_SIZE,
)
from common.data_source.exceptions import (
    ConnectorMissingCredentialError,
    ConnectorValidationError,
)
from common.data_source.interfaces import LoadConnector, OnyxExtensionType, PollConnector
from common.data_source.models import Document, GenerateDocumentsOutput, SecondsSinceUnixEpoch
from common.data_source.utils import get_file_ext, is_accepted_file_ext


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SharePointFileEntry:
    unique_id: str
    name: str
    server_relative_url: str
    modified_at: datetime
    size_bytes: int


class SharePointConnector(LoadConnector, PollConnector):
    """Connector that syncs files from one SharePoint site."""

    def __init__(
        self,
        site_url: str,
        folder_paths: list[str] | None = None,
        batch_size: int = INDEX_BATCH_SIZE,
    ) -> None:
        self.site_url = site_url.rstrip("/")
        self.folder_paths = folder_paths or []
        self.batch_size = batch_size
        self.size_threshold: int | None = BLOB_STORAGE_SIZE_THRESHOLD
        self.sharepoint_client: ClientContext | None = None
        self._allow_images = False
        self._site_server_relative_url: str | None = None

    def set_allow_images(self, allow_images: bool) -> None:
        self._allow_images = allow_images

    def _build_extension_type(self) -> OnyxExtensionType:
        extension_type = OnyxExtensionType.Plain | OnyxExtensionType.Document
        if self._allow_images:
            extension_type |= OnyxExtensionType.Multimedia
        return extension_type

    def _is_supported_file(self, file_name: str) -> bool:
        return is_accepted_file_ext(
            get_file_ext(file_name),
            self._build_extension_type(),
        )

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        tenant_id = credentials.get("tenant_id")
        client_id = credentials.get("client_id")
        client_secret = credentials.get("client_secret")

        if not all([tenant_id, client_id, client_secret, self.site_url]):
            raise ConnectorMissingCredentialError(
                "SharePoint requires tenant_id, client_id, client_secret, and site_url."
            )

        app = msal.ConfidentialClientApplication(
            client_id=client_id,
            client_credential=client_secret,
            authority=f"https://login.microsoftonline.com/{tenant_id}",
        )
        token_result = app.acquire_token_for_client(
            scopes=["https://graph.microsoft.com/.default"]
        )
        access_token = token_result.get("access_token")
        if not access_token:
            raise ConnectorMissingCredentialError(
                f"SharePoint access token acquisition failed: {token_result.get('error_description') or token_result}"
            )

        try:
            self.sharepoint_client = ClientContext(self.site_url).with_access_token(
                access_token
            )
        except Exception as exc:
            raise ConnectorMissingCredentialError(
                f"Failed to initialize SharePoint client: {exc}"
            ) from exc
        return None

    def validate_connector_settings(self) -> None:
        if self.sharepoint_client is None:
            raise ConnectorMissingCredentialError("SharePoint")

        try:
            web = self.sharepoint_client.web.get().execute_query()
            self._site_server_relative_url = (
                getattr(web, "serverRelativeUrl", None)
                or web.properties.get("ServerRelativeUrl")
                or ""
            )
            normalized_paths = self._get_normalized_folder_paths()
            for folder_path in normalized_paths:
                self._get_folder(folder_path)
        except ClientRequestException as exc:
            if getattr(exc, "response_status_code", None) in {401, 403}:
                raise ConnectorValidationError(
                    "Invalid SharePoint credentials or insufficient permissions."
                ) from exc
            raise ConnectorValidationError(
                f"SharePoint validation error: {exc}"
            ) from exc
        except Exception as exc:
            raise ConnectorValidationError(
                f"SharePoint validation error: {exc}"
            ) from exc

    def load_from_state(self) -> GenerateDocumentsOutput:
        return self._yield_documents(None, None)

    def poll_source(
        self,
        start: SecondsSinceUnixEpoch,
        end: SecondsSinceUnixEpoch,
    ) -> GenerateDocumentsOutput:
        return self._yield_documents(
            datetime.fromtimestamp(start, tz=timezone.utc),
            datetime.fromtimestamp(end, tz=timezone.utc),
        )

    def list_current_files(self) -> list[SharePointFileEntry]:
        return self._collect_files(None, None)

    def _yield_documents(
        self,
        start: datetime | None,
        end: datetime | None,
    ) -> GenerateDocumentsOutput:
        all_files = self._collect_files(start, end)
        filename_counts: dict[str, int] = {}
        for entry in all_files:
            filename_counts[entry.name] = filename_counts.get(entry.name, 0) + 1

        batch: list[Document] = []
        for entry in all_files:
            if (
                self.size_threshold is not None
                and entry.size_bytes > self.size_threshold
            ):
                logger.warning(
                    "SharePoint file %s exceeds size threshold %s. Skipping.",
                    entry.server_relative_url,
                    self.size_threshold,
                )
                continue

            blob = self._download_file(entry.server_relative_url)
            if not blob:
                logger.warning(
                    "SharePoint file %s produced an empty payload. Skipping.",
                    entry.server_relative_url,
                )
                continue

            semantic_identifier = (
                entry.server_relative_url.lstrip("/").replace("/", " / ")
                if filename_counts.get(entry.name, 0) > 1
                else entry.name
            )
            batch.append(
                Document(
                    id=f"sharepoint:{entry.unique_id}",
                    source=DocumentSource.SHAREPOINT,
                    semantic_identifier=semantic_identifier,
                    extension=get_file_ext(entry.name),
                    blob=blob,
                    doc_updated_at=entry.modified_at,
                    size_bytes=entry.size_bytes or len(blob),
                    metadata={"server_relative_url": entry.server_relative_url},
                )
            )
            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

    def _collect_files(
        self,
        start: datetime | None,
        end: datetime | None,
    ) -> list[SharePointFileEntry]:
        normalized_paths = self._get_normalized_folder_paths()
        files: list[SharePointFileEntry] = []
        seen_file_ids: set[str] = set()
        for folder_path in normalized_paths:
            self._collect_files_recursive(
                folder_path=folder_path,
                start=start,
                end=end,
                files=files,
                seen_file_ids=seen_file_ids,
            )
        return files

    def _collect_files_recursive(
        self,
        folder_path: str,
        start: datetime | None,
        end: datetime | None,
        files: list[SharePointFileEntry],
        seen_file_ids: set[str],
    ) -> None:
        folder = self._get_folder(folder_path)
        self.sharepoint_client.load(folder.files)
        self.sharepoint_client.load(folder.folders)
        self.sharepoint_client.execute_query()

        for sp_file in folder.files:  # type: ignore[attr-defined]
            entry = self._map_file_entry(sp_file)
            if entry is None:
                continue
            if entry.unique_id in seen_file_ids:
                continue
            if start is not None and entry.modified_at <= start:
                continue
            if end is not None and entry.modified_at > end:
                continue
            seen_file_ids.add(entry.unique_id)
            files.append(entry)

        for subfolder in folder.folders:  # type: ignore[attr-defined]
            subfolder_name = (
                getattr(subfolder, "name", None)
                or subfolder.properties.get("Name")
                or ""
            )
            if subfolder_name.lower() == "forms":
                continue
            subfolder_path = (
                getattr(subfolder, "serverRelativeUrl", None)
                or subfolder.properties.get("ServerRelativeUrl")
                or ""
            )
            if not subfolder_path:
                continue
            self._collect_files_recursive(
                folder_path=subfolder_path,
                start=start,
                end=end,
                files=files,
                seen_file_ids=seen_file_ids,
            )

    def _get_folder(self, folder_path: str):
        if self.sharepoint_client is None:
            raise ConnectorMissingCredentialError("SharePoint")
        folder = self.sharepoint_client.web.get_folder_by_server_relative_url(
            folder_path
        )
        folder.get().execute_query()
        return folder

    def _download_file(self, server_relative_url: str) -> bytes:
        if self.sharepoint_client is None:
            raise ConnectorMissingCredentialError("SharePoint")
        response = SharePointFile.open_binary(
            self.sharepoint_client, server_relative_url
        )
        return response.content

    def _map_file_entry(self, sp_file: Any) -> SharePointFileEntry | None:
        file_name = getattr(sp_file, "name", None) or sp_file.properties.get("Name")
        if not file_name or not self._is_supported_file(file_name):
            return None

        server_relative_url = (
            getattr(sp_file, "serverRelativeUrl", None)
            or sp_file.properties.get("ServerRelativeUrl")
        )
        if not server_relative_url:
            return None

        unique_id = (
            str(getattr(sp_file, "unique_id", None) or "")
            or str(sp_file.properties.get("UniqueId") or "")
            or server_relative_url
        )
        modified_at = self._parse_modified_time(
            getattr(sp_file, "time_last_modified", None)
            or sp_file.properties.get("TimeLastModified")
        )
        size_bytes = self._parse_size(
            getattr(sp_file, "length", None) or sp_file.properties.get("Length")
        )
        return SharePointFileEntry(
            unique_id=unique_id,
            name=file_name,
            server_relative_url=server_relative_url,
            modified_at=modified_at,
            size_bytes=size_bytes,
        )

    def _get_normalized_folder_paths(self) -> list[str]:
        cleaned_paths = [self._normalize_folder_path(path) for path in self.folder_paths if path]
        if cleaned_paths:
            return cleaned_paths

        root_path = self._site_server_relative_url
        if root_path is None:
            root_path = urlparse(self.site_url).path or "/"
        root_path = root_path.rstrip("/") or "/"
        return [root_path]

    def _normalize_folder_path(self, folder_path: str) -> str:
        folder_path = unquote((folder_path or "").strip())
        if not folder_path:
            raise ConnectorValidationError("SharePoint folder path cannot be empty.")

        parsed_path = urlparse(folder_path)
        if parsed_path.scheme and parsed_path.netloc:
            folder_path = parsed_path.path or "/"

        folder_path = folder_path.replace("//", "/")
        if self._site_server_relative_url is None:
            site_path = urlparse(self.site_url).path or ""
        else:
            site_path = self._site_server_relative_url
        site_path = site_path.rstrip("/")

        if not folder_path.startswith("/"):
            folder_path = f"{site_path}/{folder_path}" if site_path else f"/{folder_path}"

        if site_path and not folder_path.startswith(site_path):
            folder_path = f"{site_path}{folder_path}"

        while "//" in folder_path:
            folder_path = folder_path.replace("//", "/")

        if folder_path.endswith("/") and folder_path != "/":
            folder_path = folder_path.rstrip("/")

        return folder_path

    @staticmethod
    def _parse_modified_time(raw_value: Any) -> datetime:
        if isinstance(raw_value, datetime):
            return (
                raw_value.replace(tzinfo=timezone.utc)
                if raw_value.tzinfo is None
                else raw_value.astimezone(timezone.utc)
            )

        if isinstance(raw_value, str):
            value = raw_value.replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(value)
                return (
                    parsed.replace(tzinfo=timezone.utc)
                    if parsed.tzinfo is None
                    else parsed.astimezone(timezone.utc)
                )
            except ValueError:
                logger.warning(
                    "Unable to parse SharePoint modified timestamp %s. Using current time.",
                    raw_value,
                )

        return datetime.now(timezone.utc)

    @staticmethod
    def _parse_size(raw_value: Any) -> int:
        if isinstance(raw_value, int):
            return raw_value
        if isinstance(raw_value, str) and raw_value.isdigit():
            return int(raw_value)
        return 0
