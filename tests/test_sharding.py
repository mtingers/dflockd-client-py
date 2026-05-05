"""Pure-function tests for the CRC-32 shard helper."""

import pytest

from dflockd_client.sharding import DEFAULT_SERVERS, stable_hash_shard


class TestStableHashShard:
    def test_deterministic(self):
        assert stable_hash_shard("k", 7) == stable_hash_shard("k", 7)

    @pytest.mark.parametrize("n", [1, 2, 3, 7, 16, 100])
    @pytest.mark.parametrize("key", ["a", "b", "foo", "bar/baz", "x" * 1000])
    def test_in_range(self, key, n):
        assert 0 <= stable_hash_shard(key, n) < n

    def test_single_server(self):
        assert all(stable_hash_shard(k, 1) == 0 for k in ("a", "b", "c"))

    def test_zero_servers_raises(self):
        with pytest.raises(ValueError, match="num_servers must be > 0"):
            stable_hash_shard("k", 0)

    def test_distribution(self):
        """Keys should spread across all 4 buckets."""
        counts = [0, 0, 0, 0]
        for i in range(1000):
            counts[stable_hash_shard(f"key-{i}", 4)] += 1
        assert all(c > 100 for c in counts), counts


def test_default_servers():
    assert DEFAULT_SERVERS == (("127.0.0.1", 6388),)
