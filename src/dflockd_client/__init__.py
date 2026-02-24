from importlib.metadata import PackageNotFoundError, version

from ._common import StatsResult
from .client import DistributedLock as AsyncDistributedLock
from .client import DistributedSemaphore as AsyncDistributedSemaphore
from .sharding import DEFAULT_SERVERS, ShardingStrategy, stable_hash_shard
from .sync_client import DistributedLock as SyncDistributedLock
from .sync_client import DistributedSemaphore as SyncDistributedSemaphore

try:
    __version__ = version("dflockd-client")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    "__version__",
    "StatsResult",
    "DEFAULT_SERVERS",
    "ShardingStrategy",
    "stable_hash_shard",
    "AsyncDistributedLock",
    "AsyncDistributedSemaphore",
    "SyncDistributedLock",
    "SyncDistributedSemaphore",
]
