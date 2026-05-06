"""Sentinel exception classes form the documented hierarchy."""

from dflockd_client.errors import (
    AlreadyQueuedError,
    AuthError,
    DflockdError,
    DflockdTimeoutError,
    DrainingError,
    LeaseExpiredError,
    LimitMismatchError,
    MaxLocksError,
    MaxWaitersError,
    NotQueuedError,
)


def test_dflockd_error_is_runtime_error():
    assert issubclass(DflockdError, RuntimeError)


def test_dflockd_timeout_is_timeout_error():
    """``isinstance(err, TimeoutError)`` is the documented predicate."""
    assert issubclass(DflockdTimeoutError, TimeoutError)


def test_protocol_errors_subclass_dflockd_error():
    for cls in (
        AuthError,
        MaxLocksError,
        MaxWaitersError,
        LimitMismatchError,
        NotQueuedError,
        AlreadyQueuedError,
        LeaseExpiredError,
        DrainingError,
    ):
        assert issubclass(cls, DflockdError)
