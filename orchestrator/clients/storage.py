from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

try:  # Optional dependency used when GCS uploads are enabled
    from google.cloud import storage  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - optional dependency
    storage = None  # type: ignore[assignment]


class BaseUploader:

    def __init__(self) -> None:
        self._logger = logging.getLogger(__name__)

    def upload_bytes(
        self,
        *,
        job_id: str,
        content: bytes,
        filename: Optional[str] = None,
        content_type: Optional[str] = None,
        job_type: Optional[str] = None,
    ) -> Optional[str]:
        """Return None to signal that no upload occurred."""

        self._logger.info(
            "callback.upload.noop job_id=%s filename=%s bytes=%s job_type=%s",
            job_id,
            filename,
            len(content),
            job_type,
        )
        return None


@dataclass
class GCSUploader(BaseUploader):

    bucket: str
    prefix: str = "callbacks"
    credentials_path: Optional[Path] = None

    _client: object = field(init=False, repr=False)
    _bucket: object = field(init=False, repr=False)

    def __post_init__(self) -> None:
        super().__init__()
        if isinstance(self.credentials_path, str):
            self.credentials_path = Path(self.credentials_path)
        self.prefix = self.prefix.strip("/")
        self._client = self._create_client()
        self._bucket = self._client.bucket(self.bucket)
        credentials_exists = (
            self.credentials_path.exists() if isinstance(self.credentials_path, Path) else None
        )
        self._logger.info(
            "callback.gcs_uploader_initialized bucket=%s prefix=%s credentials=%s credentials_exists=%s",
            self.bucket,
            self.prefix,
            self.credentials_path,
            credentials_exists,
        )

    def _create_client(self) -> object:
        if storage is None:
            raise RuntimeError(
                "google-cloud-storage is required to upload callback images"
            )
        if self.credentials_path:
            return storage.Client.from_service_account_json(str(self.credentials_path))
        return storage.Client()

    def upload_bytes(
        self,
        *,
        job_id: str,
        content: bytes,
        filename: Optional[str] = None,
        content_type: Optional[str] = None,
        job_type: Optional[str] = None,
    ) -> Optional[str]:
        suffix = Path(filename).suffix if filename else ""
        if not suffix:
            suffix = ".bin"

        unique_name = f"{job_id}{suffix}"

        normalized_job_type = self._normalize_job_type(job_type)
        path_parts = [part for part in (self.prefix, normalized_job_type) if part]
        prefix_path = "/".join(path_parts)
        blob_name = f"{prefix_path}/{unique_name}" if prefix_path else unique_name

        self._logger.info(
            "callback.gcs_upload_start bucket=%s blob=%s bytes=%s job_id=%s job_type=%s",
            self.bucket,
            blob_name,
            len(content),
            job_id,
            job_type,
        )

        blob = self._bucket.blob(blob_name)
        try:
            blob.upload_from_string(content, content_type=content_type)
        except Exception:
            self._logger.exception(
                "callback.gcs_upload_error bucket=%s blob=%s job_id=%s content_type=%s job_type=%s",
                self.bucket,
                blob_name,
                job_id,
                content_type,
                job_type,
            )
            raise

        self._logger.info(
            "callback.gcs_upload_complete bucket=%s blob=%s bytes=%s job_id=%s job_type=%s",
            self.bucket,
            blob_name,
            len(content),
            job_id,
            job_type,
        )

        return f"gs://{self.bucket}/{blob_name}"

    @staticmethod
    def _normalize_job_type(job_type: Optional[str]) -> Optional[str]:
        if not job_type:
            return None
        normalized = str(job_type).strip().lower()
        if not normalized:
            return None
        sanitized_chars = []
        for char in normalized:
            if char.isalnum() or char in {"-", "_"}:
                sanitized_chars.append(char)
            else:
                sanitized_chars.append("-")
        sanitized = "".join(sanitized_chars).strip("-_")
        return sanitized or None


__all__ = ["BaseUploader", "GCSUploader"]
