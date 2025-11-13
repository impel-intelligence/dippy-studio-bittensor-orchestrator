from __future__ import annotations

from orchestrator.common.job_store import Job, JobRequest, JobResponse, JobType
from orchestrator.schemas.job import JobRecord


def test_job_record_includes_image_metadata() -> None:
    job = Job(
        job_request=JobRequest(
            job_type=JobType.GENERATE,
            payload={"prompt": "hello"},
        ),
        hotkey="hk",
    )
    job.job_response = JobResponse(
        payload={
            "image_uri": "gs://bucket/path.png",
            "image_sha256": "abc123",
        }
    )

    record = JobRecord.from_store(job)

    assert record.result_image_url == "gs://bucket/path.png"
    assert record.result_image_sha256 == "abc123"
