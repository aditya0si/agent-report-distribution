"""Error taxonomy and the retry driver: what gets retried, what does not, and the backoff shape."""

from __future__ import annotations

import random

import botocore.exceptions
import pytest

from agent_reports.common import errors
from agent_reports.common.retry import RetryPolicy, RetryStats, call_with_retry, compute_delay


def client_error(code: str, status: int = 400, operation: str = "SendMessage") -> botocore.exceptions.ClientError:
    return botocore.exceptions.ClientError(
        {"Error": {"Code": code, "Message": f"{code} happened"}, "ResponseMetadata": {"HTTPStatusCode": status}},
        operation,
    )


class TestClassify:
    def test_retryable_aws_codes(self) -> None:
        for code in ("Throttling", "TooManyRequestsException", "ServiceUnavailable", "RequestTimeout"):
            classified = errors.classify(client_error(code, status=500))
            assert classified.retryable is True, code
            assert isinstance(classified, errors.TransientError)

    def test_permanent_aws_codes(self) -> None:
        for code in ("AccessDenied", "MessageRejected", "ValidationException", "NoSuchKey"):
            classified = errors.classify(client_error(code, status=403))
            assert classified.retryable is False, code
            assert isinstance(classified, errors.PermanentError)

    def test_five_hundreds_retryable_even_with_unknown_code(self) -> None:
        assert errors.classify(client_error("WeirdFailure", status=503)).retryable is True

    def test_four_twenty_nine_is_throttling(self) -> None:
        classified = errors.classify(client_error("Unknown", status=429))
        assert isinstance(classified, errors.ThrottlingError)
        assert classified.retryable is True

    def test_four_hundreds_are_permanent(self) -> None:
        assert errors.classify(client_error("Unknown", status=400)).retryable is False

    def test_connection_errors_are_retryable(self) -> None:
        exc = botocore.exceptions.EndpointConnectionError(endpoint_url="https://s3.example")
        assert errors.classify(exc).retryable is True

    def test_read_timeout_is_retryable(self) -> None:
        assert errors.classify(botocore.exceptions.ReadTimeoutError(endpoint_url="x")).retryable

    def test_unknown_exception_is_permanent(self) -> None:
        classified = errors.classify(RuntimeError("boom"))
        assert classified.retryable is False
        assert classified.code == "PermanentError"

    def test_already_classified_passes_through(self) -> None:
        original = errors.InvalidMessageError("bad payload", context={"message_id": "m1"})
        assert errors.classify(original) is original

    def test_error_context_and_log_fields(self) -> None:
        exc = errors.ReportNotFoundError("missing", context={"agent_id": "AGT-000001"})
        fields = exc.as_log_fields()
        assert fields["error_code"] == "ReportNotFoundError"
        assert fields["retryable"] is True
        assert fields["agent_id"] == "AGT-000001"
        assert "AGT-000001" in str(exc)

    def test_error_taxonomy_retryable_flags(self) -> None:
        assert errors.MissingReportError("x").retryable is True
        assert errors.AuthzDeniedError("x").retryable is False
        assert errors.ConfigError("x").retryable is False
        assert errors.ThrottlingError("x").retryable is True


class TestBackoff:
    def test_ceiling_doubles_and_caps(self) -> None:
        policy = RetryPolicy(base_delay=0.1, max_delay=1.0)
        assert policy.ceiling(0) == pytest.approx(0.1)
        assert policy.ceiling(1) == pytest.approx(0.2)
        assert policy.ceiling(3) == pytest.approx(0.8)
        assert policy.ceiling(4) == pytest.approx(1.0)
        assert policy.ceiling(10) == pytest.approx(1.0)

    def test_full_jitter_stays_within_the_ceiling(self) -> None:
        policy = RetryPolicy(base_delay=0.1, max_delay=1.0, jitter="full")
        for attempt in range(6):
            for value in (0.0, 0.25, 0.5, 0.999, 1.0):
                delay = compute_delay(attempt, policy, rand=lambda value=value: value)
                assert 0.0 <= delay <= policy.ceiling(attempt) + 1e-9

    def test_full_jitter_actually_varies(self) -> None:
        policy = RetryPolicy(base_delay=1.0, max_delay=1.0, jitter="full")
        values = {compute_delay(3, policy, rand=random.Random(seed).random) for seed in range(8)}
        assert len(values) > 1

    def test_equal_jitter_is_in_the_upper_half(self) -> None:
        policy = RetryPolicy(base_delay=1.0, max_delay=1.0, jitter="equal")
        assert compute_delay(2, policy, rand=lambda: 0.0) == pytest.approx(0.5)
        assert compute_delay(2, policy, rand=lambda: 1.0) == pytest.approx(1.0)

    def test_no_jitter_is_deterministic(self) -> None:
        policy = RetryPolicy(base_delay=0.1, max_delay=10.0, jitter="none")
        assert compute_delay(2, policy) == pytest.approx(0.4)

    def test_zero_base_delay_returns_zero(self) -> None:
        assert compute_delay(5, RetryPolicy(base_delay=0.0, max_delay=0.0)) == 0.0

    @pytest.mark.parametrize(
        "kwargs, message",
        [
            ({"max_attempts": 0}, "max_attempts"),
            ({"base_delay": -1.0}, "delays"),
            ({"base_delay": 1.0, "max_delay": 0.5}, "max_delay"),
            ({"jitter": "sprinkle"}, "jitter"),
        ],
    )
    def test_policy_validation(self, kwargs: dict[str, object], message: str) -> None:
        with pytest.raises(ValueError, match=message):
            RetryPolicy(**kwargs)  # type: ignore[arg-type]


class TestCallWithRetry:
    def test_returns_first_success_without_sleeping(self) -> None:
        slept: list[float] = []
        stats = RetryStats()
        result = call_with_retry(
            lambda: "ok", sleep=slept.append, stats=stats, policy=RetryPolicy()
        )
        assert result == "ok"
        assert slept == []
        assert stats.attempts == 1
        assert stats.retries == 0

    def test_retries_transient_failures_then_succeeds(self) -> None:
        attempts = {"count": 0}
        slept: list[float] = []

        def flaky() -> str:
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise client_error("Throttling", status=429)
            return "done"

        stats = RetryStats()
        result = call_with_retry(
            flaky,
            policy=RetryPolicy(max_attempts=5, base_delay=0.5, max_delay=4.0),
            sleep=slept.append,
            rand=lambda: 1.0,
            stats=stats,
        )
        assert result == "done"
        assert attempts["count"] == 3
        assert slept == [pytest.approx(0.5), pytest.approx(1.0)]
        assert stats.retries == 2
        assert stats.retried_codes == ["DependencyError", "DependencyError"]

    def test_permanent_failure_is_not_retried(self) -> None:
        calls = {"count": 0}

        def always_denied() -> None:
            calls["count"] += 1
            raise client_error("AccessDenied", status=403)

        with pytest.raises(errors.PermanentError):
            call_with_retry(always_denied, policy=RetryPolicy(max_attempts=5), sleep=lambda _: None)
        assert calls["count"] == 1

    def test_exhausting_attempts_raises_dependency_error(self) -> None:
        slept: list[float] = []

        def always_throttled() -> None:
            raise client_error("Throttling", status=429)

        with pytest.raises(errors.DependencyError) as excinfo:
            call_with_retry(
                always_throttled,
                policy=RetryPolicy(max_attempts=3, base_delay=0.1, max_delay=1.0),
                sleep=slept.append,
                rand=lambda: 0.5,
                operation="ses.send_email",
            )
        assert len(slept) == 2
        assert excinfo.value.context["attempts"] == 3
        assert excinfo.value.context["last_error_code"] == "DependencyError"
        assert "ses.send_email" in str(excinfo.value)

    def test_on_retry_callback_reports_each_retry(self) -> None:
        seen: list[tuple[int, str, float]] = []

        def flaky() -> str:
            if len(seen) < 2:
                raise client_error("ServiceUnavailable", status=503)
            return "ok"

        call_with_retry(
            flaky,
            policy=RetryPolicy(max_attempts=4, base_delay=0.1, max_delay=0.4),
            sleep=lambda _: None,
            rand=lambda: 1.0,
            on_retry=lambda attempt, exc, delay: seen.append((attempt, exc.code, delay)),
        )
        assert [attempt for attempt, _, _ in seen] == [1, 2]
        assert {code for _, code, _ in seen} == {"DependencyError"}

    def test_retry_on_override_forces_retry_of_a_permanent_error(self) -> None:
        calls = {"count": 0}

        def boom() -> str:
            calls["count"] += 1
            if calls["count"] < 2:
                raise ValueError("application-level flake")
            return "recovered"

        result = call_with_retry(
            boom,
            policy=RetryPolicy(max_attempts=3, base_delay=0.0, max_delay=0.0, retry_on=(ValueError,)),
            sleep=lambda _: None,
        )
        assert result == "recovered"
        assert calls["count"] == 2

    def test_retry_on_override_does_not_retry_other_errors(self) -> None:
        calls = {"count": 0}

        def boom() -> str:
            calls["count"] += 1
            raise KeyError("not covered by retry_on")

        with pytest.raises(errors.PermanentError):
            call_with_retry(
                boom,
                policy=RetryPolicy(max_attempts=3, base_delay=0.0, max_delay=0.0, retry_on=(ValueError,)),
                sleep=lambda _: None,
            )
        assert calls["count"] == 1
