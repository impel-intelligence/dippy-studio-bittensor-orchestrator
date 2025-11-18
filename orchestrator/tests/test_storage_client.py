from __future__ import annotations

from typing import Any, Dict, List

import pytest

from orchestrator.clients.storage import GCSUploader


class _StubBlob:
    def __init__(self, name: str, recorder: Dict[str, Any]) -> None:
        self.name = name
        self._recorder = recorder

    def upload_from_string(self, content: bytes, content_type: str | None = None) -> None:
        uploads: List[Dict[str, Any]] = self._recorder.setdefault("uploads", [])
        uploads.append({"name": self.name, "content": content, "content_type": content_type})


class _StubBucket:
    def __init__(self, recorder: Dict[str, Any]) -> None:
        self._recorder = recorder

    def blob(self, name: str) -> _StubBlob:
        names: List[str] = self._recorder.setdefault("blob_names", [])
        names.append(name)
        return _StubBlob(name, self._recorder)


class _StubClient:
    def __init__(self, recorder: Dict[str, Any]) -> None:
        self._recorder = recorder

    def bucket(self, name: str) -> _StubBucket:
        self._recorder["bucket_name"] = name
        return _StubBucket(self._recorder)


def _uploader(monkeypatch: pytest.MonkeyPatch) -> tuple[GCSUploader, dict]:
    recorder: Dict[str, Any] = {}

    def _fake_client(self: GCSUploader) -> _StubClient:
        return _StubClient(recorder)

    monkeypatch.setattr(GCSUploader, "_create_client", _fake_client)
    return GCSUploader(bucket="test-bucket", prefix="callbacks"), recorder


def test_gcs_uploader_appends_job_type(monkeypatch: pytest.MonkeyPatch) -> None:
    uploader, recorder = _uploader(monkeypatch)

    uri = uploader.upload_bytes(
        job_id="abc123",
        content=b"payload",
        filename="image.png",
        content_type="image/png",
        job_type="Generate ",
    )

    assert uri == "gs://test-bucket/callbacks/generate/abc123.png"
    assert recorder.get("blob_names") == ["callbacks/generate/abc123.png"]


def test_gcs_uploader_handles_missing_job_type(monkeypatch: pytest.MonkeyPatch) -> None:
    uploader, recorder = _uploader(monkeypatch)

    uri = uploader.upload_bytes(
        job_id="xyz789",
        content=b"payload",
        filename="photo.jpeg",
        content_type="image/jpeg",
        job_type=None,
    )

    assert uri == "gs://test-bucket/callbacks/xyz789.jpeg"
    assert recorder.get("blob_names") == ["callbacks/xyz789.jpeg"]
