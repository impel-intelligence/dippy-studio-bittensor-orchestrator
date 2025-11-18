"""Domain-specific exceptions for orchestrator services."""

from __future__ import annotations


class ServiceError(Exception):
    """Base exception for service-layer failures."""


# Job service errors -------------------------------------------------------


class JobServiceError(ServiceError):
    """Base class for job service failures."""


class JobRelayError(JobServiceError):
    """Raised when communication with the job relay fails."""


class JobNotFound(JobServiceError):
    """Raised when a requested job cannot be located."""


class JobValidationError(JobServiceError):
    """Raised when an invalid job payload or parameter is provided."""


class JobQueryError(JobServiceError):
    """Raised when list/dump query arguments are invalid."""


# Listen service errors ----------------------------------------------------


class ListenServiceError(ServiceError):
    """Base class for listen service failures."""


class MinerSelectionError(ListenServiceError):
    """Raised when a miner candidate cannot be selected."""


# Callback service errors --------------------------------------------------


class CallbackServiceError(ServiceError):
    """Base class for callback processing failures."""


class CallbackValidationError(CallbackServiceError):
    """Raised when incoming callback data is invalid."""


class CallbackSecretMissing(CallbackServiceError):
    """Raised when a job callback secret is absent."""


class CallbackSecretMismatch(CallbackServiceError):
    """Raised when the provided callback secret does not match expectations."""


class CallbackImageError(CallbackServiceError):
    """Raised when an image payload cannot be processed."""


class CallbackImageUploadError(CallbackServiceError):
    """Raised when the image uploader fails."""


__all__ = [
    "ServiceError",
    "JobServiceError",
    "JobRelayError",
    "JobNotFound",
    "JobValidationError",
    "JobQueryError",
    "ListenServiceError",
    "MinerSelectionError",
    "CallbackServiceError",
    "CallbackValidationError",
    "CallbackSecretMissing",
    "CallbackSecretMismatch",
    "CallbackImageError",
    "CallbackImageUploadError",
]
