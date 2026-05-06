"""Pure-function tests for the wire protocol layer.

Every function in :mod:`dflockd_client._protocol` is fully exercised here
without touching a socket. Validation rules, encoder formats, and the
status → exception mapping are all locked down so the I/O layers can stay
trivially small wrappers.
"""

import pytest

from dflockd_client import _protocol as proto
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


# ---------------------------------------------------------------------------
# validate_*
# ---------------------------------------------------------------------------


class TestValidateProtocolLine:
    def test_accepts_simple_line(self):
        proto.validate_protocol_line("name", "hello")

    def test_rejects_newline(self):
        with pytest.raises(ValueError, match="must not contain newlines"):
            proto.validate_protocol_line("name", "x\ny")

    def test_rejects_carriage_return(self):
        with pytest.raises(ValueError, match="must not contain newlines"):
            proto.validate_protocol_line("name", "x\ry")

    def test_rejects_too_long(self):
        with pytest.raises(ValueError, match="too long"):
            proto.validate_protocol_line("name", "x" * (proto.MAX_LINE_BYTES + 1))

    def test_allow_empty_default_true(self):
        proto.validate_protocol_line("name", "")  # ok by default

    def test_allow_empty_false_rejects_empty(self):
        with pytest.raises(ValueError, match="must not be empty"):
            proto.validate_protocol_line("name", "", allow_empty=False)


class TestValidateKey:
    def test_accepts_normal(self):
        proto.validate_key("key", "user-42")

    def test_rejects_empty(self):
        with pytest.raises(ValueError, match="must not be empty"):
            proto.validate_key("key", "")

    @pytest.mark.parametrize("bad", ["a b", "a\tb", "a\nb", "a\rb"])
    def test_rejects_whitespace(self, bad):
        with pytest.raises(ValueError):
            proto.validate_key("key", bad)


class TestValidateToken:
    def test_accepts_normal(self):
        proto.validate_token("abc123")

    def test_rejects_empty(self):
        with pytest.raises(ValueError):
            proto.validate_token("")

    def test_rejects_whitespace(self):
        with pytest.raises(ValueError, match="must not contain whitespace"):
            proto.validate_token("a b")


class TestValidateTimeoutS:
    def test_accepts_zero(self):
        proto.validate_timeout_s("t", 0)

    def test_accepts_positive(self):
        proto.validate_timeout_s("t", 60)

    def test_rejects_negative(self):
        with pytest.raises(ValueError, match=">= 0"):
            proto.validate_timeout_s("t", -1)

    def test_rejects_too_large(self):
        with pytest.raises(ValueError, match="too large"):
            proto.validate_timeout_s("t", proto.MAX_PROTOCOL_SECONDS + 1)

    def test_rejects_bool(self):
        # ``True`` is an int subclass — must be rejected explicitly.
        with pytest.raises(TypeError, match="must be an integer"):
            proto.validate_timeout_s("t", True)  # type: ignore[arg-type]

    def test_rejects_float(self):
        with pytest.raises(TypeError, match="must be an integer"):
            proto.validate_timeout_s("t", 1.5)  # type: ignore[arg-type]


class TestValidateLeaseTtlS:
    def test_none_is_allowed(self):
        proto.validate_lease_ttl_s(None)

    def test_accepts_positive(self):
        proto.validate_lease_ttl_s(33)

    def test_rejects_zero(self):
        with pytest.raises(ValueError, match=">= 1"):
            proto.validate_lease_ttl_s(0)

    def test_rejects_negative(self):
        with pytest.raises(ValueError):
            proto.validate_lease_ttl_s(-1)


class TestValidateSemaphoreLimit:
    def test_accepts_positive(self):
        proto.validate_semaphore_limit(5)

    def test_rejects_zero(self):
        with pytest.raises(ValueError, match="> 0"):
            proto.validate_semaphore_limit(0)

    def test_rejects_negative(self):
        with pytest.raises(ValueError):
            proto.validate_semaphore_limit(-1)

    def test_rejects_bool(self):
        with pytest.raises(TypeError):
            proto.validate_semaphore_limit(True)  # type: ignore[arg-type]


class TestValidatePrefix:
    def test_accepts_lock(self):
        proto.validate_prefix("")

    def test_accepts_semaphore(self):
        proto.validate_prefix("s")

    def test_rejects_other(self):
        with pytest.raises(ValueError, match="cmd_prefix must be"):
            proto.validate_prefix("sem")


class TestValidatePrefixLimit:
    """The (prefix, limit) pair must agree: 's' requires limit, '' rejects it.

    Without this check, a lock acquire with ``limit=N`` was previously sent
    as ``l <key> <timeout> <N>`` — which the server parsed as
    ``<lease_ttl>=N``, silently producing wrong behaviour with no error.
    """

    def test_lock_without_limit(self):
        proto.validate_prefix_limit("", None)

    def test_lock_with_limit_rejected(self):
        with pytest.raises(ValueError, match="limit must not be set"):
            proto.validate_prefix_limit("", 5)

    def test_semaphore_with_limit(self):
        proto.validate_prefix_limit("s", 3)

    def test_semaphore_without_limit_rejected(self):
        with pytest.raises(ValueError, match="limit is required"):
            proto.validate_prefix_limit("s", None)

    def test_semaphore_zero_limit_rejected(self):
        with pytest.raises(ValueError, match="limit must be > 0"):
            proto.validate_prefix_limit("s", 0)


class TestValidateAuthToken:
    def test_accepts_normal(self):
        proto.validate_auth_token("shared-secret")

    def test_rejects_newline(self):
        with pytest.raises(ValueError, match="must not contain newlines"):
            proto.validate_auth_token("a\nb")

    def test_rejects_too_long(self):
        with pytest.raises(ValueError, match="too long"):
            proto.validate_auth_token("x" * (proto.MAX_AUTH_TOKEN_BYTES + 1))


# ---------------------------------------------------------------------------
# encode_lines
# ---------------------------------------------------------------------------


class TestEncodeLines:
    def test_three_lines(self):
        assert proto.encode_lines("l", "k", "10") == b"l\nk\n10\n"

    def test_empty_arg(self):
        assert proto.encode_lines("ping", "_", "") == b"ping\n_\n\n"

    def test_unicode(self):
        assert proto.encode_lines("l", "résumé", "0") == "l\nrésumé\n0\n".encode(
            "utf-8"
        )

    def test_rejects_embedded_newline(self):
        with pytest.raises(ValueError, match="must not contain newlines"):
            proto.encode_lines("l", "k\nbad", "0")


# ---------------------------------------------------------------------------
# build_*_arg
# ---------------------------------------------------------------------------


class TestBuildAcquireArg:
    def test_lock_no_lease(self):
        assert proto.build_acquire_arg(5, limit=None, lease_ttl_s=None) == "5"

    def test_lock_with_lease(self):
        assert proto.build_acquire_arg(5, limit=None, lease_ttl_s=10) == "5 10"

    def test_semaphore_no_lease(self):
        assert proto.build_acquire_arg(5, limit=3, lease_ttl_s=None) == "5 3"

    def test_semaphore_with_lease(self):
        assert proto.build_acquire_arg(5, limit=3, lease_ttl_s=10) == "5 3 10"


class TestBuildRenewArg:
    def test_no_lease(self):
        assert proto.build_renew_arg("tok", None) == "tok"

    def test_with_lease(self):
        assert proto.build_renew_arg("tok", 30) == "tok 30"


class TestBuildEnqueueArg:
    def test_lock_no_lease(self):
        assert proto.build_enqueue_arg(limit=None, lease_ttl_s=None) == ""

    def test_lock_with_lease(self):
        assert proto.build_enqueue_arg(limit=None, lease_ttl_s=10) == "10"

    def test_semaphore_no_lease(self):
        assert proto.build_enqueue_arg(limit=3, lease_ttl_s=None) == "3"

    def test_semaphore_with_lease(self):
        assert proto.build_enqueue_arg(limit=3, lease_ttl_s=10) == "3 10"


def test_build_wait_arg():
    assert proto.build_wait_arg(15) == "15"


# ---------------------------------------------------------------------------
# make_*_arg (validate + build + validate-line)
# ---------------------------------------------------------------------------


class TestMakeAcquireArg:
    def test_returns_validated_string(self):
        assert proto.make_acquire_arg(5, limit=2, lease_ttl_s=10) == "5 2 10"

    def test_rejects_bad_timeout(self):
        with pytest.raises(ValueError):
            proto.make_acquire_arg(-1, limit=None, lease_ttl_s=None)


class TestMakeRenewArg:
    def test_no_lease(self):
        assert proto.make_renew_arg("tok", None) == "tok"

    def test_rejects_empty_token(self):
        with pytest.raises(ValueError):
            proto.make_renew_arg("", 5)


class TestMakeEnqueueArg:
    def test_lock_with_lease(self):
        assert proto.make_enqueue_arg(limit=None, lease_ttl_s=10) == "10"

    def test_rejects_bad_lease(self):
        with pytest.raises(ValueError):
            proto.make_enqueue_arg(limit=None, lease_ttl_s=0)


class TestMakeWaitArg:
    def test_normal(self):
        assert proto.make_wait_arg(7) == "7"

    def test_rejects_negative(self):
        with pytest.raises(ValueError):
            proto.make_wait_arg(-1)


# ---------------------------------------------------------------------------
# cmd_name / op_label
# ---------------------------------------------------------------------------


class TestCmdName:
    @pytest.mark.parametrize(
        "prefix,op,expected",
        [("", "l", "l"), ("", "r", "r"), ("s", "l", "sl"), ("s", "e", "se")],
    )
    def test_concatenates(self, prefix, op, expected):
        assert proto.cmd_name(prefix, op) == expected

    def test_rejects_bad_prefix(self):
        with pytest.raises(ValueError):
            proto.cmd_name("x", "l")


class TestOpLabel:
    def test_lock(self):
        assert proto.op_label("", "acquire") == "acquire"

    def test_semaphore(self):
        assert proto.op_label("s", "acquire") == "sem_acquire"


# ---------------------------------------------------------------------------
# Status → exception mapping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "wire,exc",
    [
        ("error_auth", AuthError),
        ("error_max_locks", MaxLocksError),
        ("error_max_waiters", MaxWaitersError),
        ("error_limit_mismatch", LimitMismatchError),
        ("error_not_enqueued", NotQueuedError),
        ("error_already_enqueued", AlreadyQueuedError),
        ("error_lease_expired", LeaseExpiredError),
        ("error_draining", DrainingError),
    ],
)
def test_status_error_maps_known_codes(wire, exc):
    err = proto.status_error("op", wire)
    assert isinstance(err, exc)


def test_status_error_unknown_falls_back_to_dflockd_error():
    err = proto.status_error("op", "error_something_new")
    assert type(err) is DflockdError


def test_raise_status_error_raises():
    with pytest.raises(MaxLocksError):
        proto.raise_status_error("op", "error_max_locks")


def test_raise_auth_error_draining_raises_draining():
    with pytest.raises(DrainingError):
        proto.raise_auth_error("error_draining")


def test_raise_auth_error_other_raises_permission_error():
    """The auth handshake surfaces ``PermissionError`` so callers can
    branch without importing dflockd-specific exceptions."""
    with pytest.raises(PermissionError, match="authentication failed"):
        proto.raise_auth_error("error_auth")


# ---------------------------------------------------------------------------
# Decoders
# ---------------------------------------------------------------------------


class TestParseGrantResponse:
    def test_grant(self):
        token, lease = proto.parse_grant_response("ok abc 30", op="acquire")
        assert (token, lease) == ("abc", 30)

    def test_timeout(self):
        with pytest.raises(DflockdTimeoutError, match="timeout in acquire"):
            proto.parse_grant_response("timeout", op="acquire")

    def test_error(self):
        with pytest.raises(MaxLocksError):
            proto.parse_grant_response("error_max_locks", op="acquire")

    def test_malformed_ok(self):
        with pytest.raises(RuntimeError, match="bad acquire response"):
            proto.parse_grant_response("ok onlytwo", op="acquire")

    def test_non_int_lease(self):
        with pytest.raises(RuntimeError, match="bad acquire response"):
            proto.parse_grant_response("ok tok notanint", op="acquire")

    def test_negative_lease(self):
        with pytest.raises(RuntimeError, match="bad acquire response"):
            proto.parse_grant_response("ok tok -1", op="acquire")

    def test_zero_lease(self):
        with pytest.raises(RuntimeError, match="bad acquire response"):
            proto.parse_grant_response("ok tok 0", op="acquire")

    def test_lease_above_protocol_max(self):
        # A misbehaving server returning a huge lease would otherwise drive
        # the renew loop's sleep to centuries, masking lease expiry.
        too_big = proto.MAX_PROTOCOL_SECONDS + 1
        with pytest.raises(RuntimeError, match="bad acquire response"):
            proto.parse_grant_response(f"ok tok {too_big}", op="acquire")

    def test_max_protocol_seconds_lease_is_accepted(self):
        _, lease = proto.parse_grant_response(
            f"ok tok {proto.MAX_PROTOCOL_SECONDS}", op="acquire"
        )
        assert lease == proto.MAX_PROTOCOL_SECONDS


class TestParseRenewResponse:
    def test_ok(self):
        assert proto.parse_renew_response("ok 18", op="renew") == 18

    def test_zero(self):
        assert proto.parse_renew_response("ok 0", op="renew") == 0

    def test_error(self):
        with pytest.raises(LeaseExpiredError):
            proto.parse_renew_response("error_lease_expired", op="renew")

    def test_malformed(self):
        with pytest.raises(RuntimeError, match="bad renew response"):
            proto.parse_renew_response("ok junk", op="renew")

    def test_negative_remaining(self):
        with pytest.raises(RuntimeError, match="bad renew response"):
            proto.parse_renew_response("ok -1", op="renew")

    def test_remaining_above_protocol_max(self):
        too_big = proto.MAX_PROTOCOL_SECONDS + 1
        with pytest.raises(RuntimeError, match="bad renew response"):
            proto.parse_renew_response(f"ok {too_big}", op="renew")


class TestParseEnqueueResponse:
    def test_queued(self):
        assert proto.parse_enqueue_response("queued", op="enqueue") == (
            "queued",
            None,
            None,
        )

    def test_acquired_grant(self):
        status, tok, lease = proto.parse_enqueue_response(
            "acquired tok 25", op="enqueue"
        )
        assert (status, tok, lease) == ("acquired", "tok", 25)

    def test_already_enqueued(self):
        with pytest.raises(AlreadyQueuedError):
            proto.parse_enqueue_response("error_already_enqueued", op="enqueue")

    def test_malformed_acquired(self):
        with pytest.raises(RuntimeError, match="bad enqueue response"):
            proto.parse_enqueue_response("acquired tok", op="enqueue")

    def test_acquired_zero_lease(self):
        # _decode_token_lease's min_value=1 floor applies on this path too.
        with pytest.raises(RuntimeError, match="bad enqueue response"):
            proto.parse_enqueue_response("acquired tok 0", op="enqueue")

    def test_acquired_negative_lease(self):
        with pytest.raises(RuntimeError, match="bad enqueue response"):
            proto.parse_enqueue_response("acquired tok -1", op="enqueue")

    def test_acquired_lease_above_protocol_max(self):
        too_big = proto.MAX_PROTOCOL_SECONDS + 1
        with pytest.raises(RuntimeError, match="bad enqueue response"):
            proto.parse_enqueue_response(f"acquired tok {too_big}", op="enqueue")


class TestParseReleaseResponse:
    def test_ok(self):
        proto.parse_release_response("ok", op="release")

    def test_error(self):
        with pytest.raises(DflockdError):
            proto.parse_release_response("error", op="release")


class TestParseAuthResponse:
    def test_ok(self):
        proto.parse_auth_response("ok")

    def test_bad_credentials(self):
        with pytest.raises(PermissionError):
            proto.parse_auth_response("error_auth")

    def test_draining(self):
        with pytest.raises(DrainingError):
            proto.parse_auth_response("error_draining")


class TestParseStatsResponse:
    def test_ok(self):
        payload = (
            '{"connections":3,"locks":[],"semaphores":[],'
            '"idle_locks":[],"idle_semaphores":[]}'
        )
        result = proto.parse_stats_response("ok " + payload)
        assert result["connections"] == 3
        assert result["locks"] == []

    def test_non_ok(self):
        with pytest.raises(DflockdError, match="stats failed"):
            proto.parse_stats_response("error")

    def test_status_error_maps_to_sentinel(self):
        with pytest.raises(AuthError):
            proto.parse_stats_response("error_auth")

    def test_malformed_json(self):
        with pytest.raises(RuntimeError, match="bad stats response"):
            proto.parse_stats_response("ok {not json")

    @pytest.mark.parametrize(
        "payload",
        [
            "[]",
            '{"locks":[],"semaphores":[],"idle_locks":[],"idle_semaphores":[]}',
            (
                '{"connections":true,"locks":[],"semaphores":[],'
                '"idle_locks":[],"idle_semaphores":[]}'
            ),
            (
                '{"connections":-1,"locks":[],"semaphores":[],'
                '"idle_locks":[],"idle_semaphores":[]}'
            ),
            (
                '{"connections":null,"locks":[],"semaphores":[],'
                '"idle_locks":[],"idle_semaphores":[]}'
            ),
            (
                '{"connections":3.5,"locks":[],"semaphores":[],'
                '"idle_locks":[],"idle_semaphores":[]}'
            ),
            (
                '{"connections":3,"locks":{},"semaphores":[],'
                '"idle_locks":[],"idle_semaphores":[]}'
            ),
            (
                '{"connections":3,"locks":null,"semaphores":[],'
                '"idle_locks":[],"idle_semaphores":[]}'
            ),
            (
                '{"connections":3,"locks":[1],"semaphores":[],'
                '"idle_locks":[],"idle_semaphores":[]}'
            ),
        ],
    )
    def test_wrong_shape(self, payload):
        with pytest.raises(RuntimeError, match="bad stats response"):
            proto.parse_stats_response("ok " + payload)

    def test_extra_fields_are_allowed(self):
        payload = (
            '{"connections":3,"locks":[],"semaphores":[],'
            '"idle_locks":[],"idle_semaphores":[],"extra":"ok"}'
        )
        result = proto.parse_stats_response("ok " + payload)
        assert result["connections"] == 3
