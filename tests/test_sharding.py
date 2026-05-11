"""Pure-function tests for the CRC-32 shard helper."""

import pytest

from dflockd_client.sharding import (
    DEFAULT_SERVERS,
    _validate_server_endpoint,
    _validate_servers,
    stable_hash_shard,
)


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


class TestValidateServerEndpoint:
    def test_valid(self):
        assert _validate_server_endpoint("127.0.0.1", 6388) == ("127.0.0.1", 6388)

    @pytest.mark.parametrize("host", ["", "bad host", "bad\nhost"])
    def test_rejects_bad_host(self, host):
        with pytest.raises(ValueError):
            _validate_server_endpoint(host, 6388)

    def test_rejects_non_string_host(self):
        with pytest.raises(TypeError):
            _validate_server_endpoint(127001, 6388)

    @pytest.mark.parametrize("port", [0, -1, 65536])
    def test_rejects_out_of_range_port(self, port):
        with pytest.raises(ValueError):
            _validate_server_endpoint("127.0.0.1", port)

    @pytest.mark.parametrize("port", [True, "6388"])
    def test_rejects_non_int_port(self, port):
        with pytest.raises(TypeError):
            _validate_server_endpoint("127.0.0.1", port)


class TestValidateServers:
    def test_valid_list(self):
        _validate_servers([("127.0.0.1", 6388)])

    def test_valid_tuple(self):
        _validate_servers((("127.0.0.1", 6388),))

    def test_rejects_empty(self):
        with pytest.raises(ValueError, match="non-empty"):
            _validate_servers([])

    def test_rejects_single_endpoint_tuple(self):
        with pytest.raises(TypeError, match="\\(host, port\\)"):
            _validate_servers(("127.0.0.1", 6388))
