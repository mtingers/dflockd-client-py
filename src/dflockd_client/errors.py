"""Sentinel exception types raised by dflockd protocol operations.

All error responses from the server map to one of these classes so callers
can branch on `isinstance` instead of parsing the wire string. Wrapped
errors keep the original wire status in `args[0]`.
"""


class DflockdError(RuntimeError):
    """Base class for protocol-status errors returned by dflockd."""


class DflockdTimeoutError(TimeoutError):
    """Server returned a 'timeout' response to a blocking acquire/wait."""


class AuthError(DflockdError):
    """Server rejected authentication."""


class MaxLocksError(DflockdError):
    """Cluster-wide max-locks cap was reached."""


class MaxWaitersError(DflockdError):
    """Per-key waiter cap was reached."""


class LimitMismatchError(DflockdError):
    """Existing semaphore key was created with a different limit."""


class NotQueuedError(DflockdError):
    """Wait was called without a matching prior enqueue."""


class AlreadyQueuedError(DflockdError):
    """Connection already has an enqueued state for this key."""


class LeaseExpiredError(DflockdError):
    """Promoted holder's lease expired before observation."""


class DrainingError(DflockdError):
    """Server is shutting down and rejected the operation."""
