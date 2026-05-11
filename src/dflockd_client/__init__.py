"""dflockd-client: Python client for the dflockd distributed FIFO lock server.

Two parallel APIs are exposed:

  - **Sync** — :class:`SyncDistributedLock` and :class:`SyncDistributedSemaphore`,
    plus the underlying :class:`SyncConn` and low-level functions in
    :mod:`dflockd_client._sync`.
  - **Async** — :class:`AsyncDistributedLock` and :class:`AsyncDistributedSemaphore`,
    plus :class:`AsyncConn` in :mod:`dflockd_client._async`.

Both speak the same wire protocol against the same servers; choose by
the runtime style of the caller.
"""

from importlib.metadata import PackageNotFoundError, version

from . import _async, _sync
from ._async import AsyncConn
from ._async import DistributedLock as AsyncDistributedLock
from ._async import DistributedSemaphore as AsyncDistributedSemaphore
from ._protocol import StatsResult, fence_from_token
from ._sync import DistributedLock as SyncDistributedLock
from ._sync import DistributedSemaphore as SyncDistributedSemaphore
from ._sync import SyncConn
from .errors import (
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
from .sharding import DEFAULT_SERVERS, ShardingStrategy, stable_hash_shard

try:
    __version__ = version("dflockd-client")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    "__version__",
    "StatsResult",
    "DflockdError",
    "DflockdTimeoutError",
    "AuthError",
    "MaxLocksError",
    "MaxWaitersError",
    "LimitMismatchError",
    "NotQueuedError",
    "AlreadyQueuedError",
    "LeaseExpiredError",
    "DrainingError",
    "DEFAULT_SERVERS",
    "ShardingStrategy",
    "stable_hash_shard",
    "fence_from_token",
    "AsyncConn",
    "AsyncDistributedLock",
    "AsyncDistributedSemaphore",
    "SyncConn",
    "SyncDistributedLock",
    "SyncDistributedSemaphore",
    "_async",
    "_sync",
]
