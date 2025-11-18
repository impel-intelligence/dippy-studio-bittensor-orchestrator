from __future__ import annotations

from typing import NoReturn

from fastapi import HTTPException, status

from orchestrator.services.exceptions import (
    CallbackImageError,
    CallbackImageUploadError,
    CallbackSecretMismatch,
    CallbackSecretMissing,
    CallbackValidationError,
    CallbackServiceError,
    JobNotFound,
    JobQueryError,
    JobRelayError,
    JobServiceError,
    JobValidationError,
    ListenServiceError,
    MinerSelectionError,
)


def raise_job_service_error(exc: JobServiceError) -> NoReturn:
    if isinstance(exc, (JobValidationError, JobQueryError)):
        status_code = status.HTTP_400_BAD_REQUEST
    elif isinstance(exc, JobNotFound):
        status_code = status.HTTP_404_NOT_FOUND
    elif isinstance(exc, JobRelayError):
        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    else:
        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    raise HTTPException(status_code=status_code, detail=str(exc)) from exc


def raise_listen_service_error(exc: ListenServiceError) -> NoReturn:
    if isinstance(exc, MinerSelectionError):
        status_code = status.HTTP_404_NOT_FOUND
    else:
        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    raise HTTPException(status_code=status_code, detail=str(exc)) from exc


def raise_callback_service_error(exc: CallbackServiceError) -> NoReturn:
    if isinstance(exc, (CallbackValidationError, CallbackImageError, CallbackSecretMissing)):
        status_code = status.HTTP_400_BAD_REQUEST
    elif isinstance(exc, CallbackSecretMismatch):
        status_code = status.HTTP_403_FORBIDDEN
    elif isinstance(exc, CallbackImageUploadError):
        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    else:
        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    raise HTTPException(status_code=status_code, detail=str(exc)) from exc


__all__ = [
    "raise_job_service_error",
    "raise_listen_service_error",
    "raise_callback_service_error",
]
