"""Settings: env parsing, validation, and the local-mode switch."""

from __future__ import annotations

import pytest

from agent_reports.common.errors import ConfigError
from agent_reports.common.settings import (
    ENV_PREFIX,
    S3_MAX_PRESIGN_SECONDS,
    Settings,
    load_settings,
)


class TestDefaultsAndEnv:
    def test_defaults_are_valid(self) -> None:
        assert Settings().validate() is not None

    def test_from_env_reads_prefixed_variables(self) -> None:
        settings = Settings.from_env(
            {
                f"{ENV_PREFIX}REGION": "eu-west-1",
                f"{ENV_PREFIX}PRESIGN_TTL_SECONDS": "120",
                f"{ENV_PREFIX}SQS_BATCH_SIZE": "5",
                f"{ENV_PREFIX}RAW_BUCKET": "custom-raw",
            }
        )
        assert settings.region == "eu-west-1"
        assert settings.presign_ttl_seconds == 120
        assert settings.sqs_batch_size == 5
        assert settings.raw_bucket == "custom-raw"

    def test_unprefixed_variables_are_ignored(self) -> None:
        assert Settings.from_env({"REGION": "eu-west-1"}).region == "us-east-1"

    def test_empty_string_keeps_default(self) -> None:
        assert Settings.from_env({f"{ENV_PREFIX}RAW_BUCKET": ""}).raw_bucket == "agent-reports-raw"

    def test_empty_optional_becomes_none(self) -> None:
        assert Settings.from_env({f"{ENV_PREFIX}ENDPOINT_URL": ""}).endpoint_url is None

    def test_non_integer_raises_config_error(self) -> None:
        with pytest.raises(ConfigError, match="must be an integer"):
            Settings.from_env({f"{ENV_PREFIX}PRESIGN_TTL_SECONDS": "soon"})

    def test_endpoint_url_from_env(self) -> None:
        settings = Settings.from_env({f"{ENV_PREFIX}ENDPOINT_URL": "http://localhost:4566"})
        assert settings.endpoint_url == "http://localhost:4566"


class TestValidation:
    def test_short_bucket_name_rejected(self) -> None:
        with pytest.raises(ConfigError, match="3-63 character"):
            Settings(raw_bucket="ab").validate()

    def test_ttl_below_minimum_rejected(self) -> None:
        with pytest.raises(ConfigError, match="at least 60"):
            Settings(presign_ttl_seconds=30).validate()

    def test_ttl_above_s3_limit_rejected(self) -> None:
        with pytest.raises(ConfigError, match=str(S3_MAX_PRESIGN_SECONDS)):
            Settings(presign_ttl_seconds=S3_MAX_PRESIGN_SECONDS + 1).validate()

    def test_batch_size_over_ten_rejected(self) -> None:
        with pytest.raises(ConfigError, match="between 1 and 10"):
            Settings(sqs_batch_size=11).validate()

    def test_sender_must_look_like_an_email(self) -> None:
        with pytest.raises(ConfigError, match="email address"):
            Settings(ses_sender="not-an-address").validate()

    def test_endpoint_url_scheme_checked(self) -> None:
        with pytest.raises(ConfigError, match="http"):
            Settings(endpoint_url="localhost:4566").validate()

    def test_errors_are_aggregated(self) -> None:
        with pytest.raises(ConfigError) as excinfo:
            Settings(raw_bucket="x", sqs_batch_size=0, ses_sender="nope").validate()
        message = str(excinfo.value)
        assert "raw_bucket" in message
        assert "sqs_batch_size" in message
        assert "ses_sender" in message


class TestZones:
    def test_zone_uris_use_s3_by_default(self) -> None:
        uris = Settings().zone_uris()
        assert uris == {
            "raw": "s3://agent-reports-raw",
            "processed": "s3://agent-reports-processed",
            "reports": "s3://agent-reports-out",
        }

    def test_local_root_switches_every_zone_to_the_filesystem(self) -> None:
        uris = Settings(local_root="C:/tmp/demo/").zone_uris()
        assert uris["raw"] == "C:/tmp/demo/raw"
        assert uris["reports"] == "C:/tmp/demo/reports"

    def test_require_queue_rejects_a_non_sqs_url(self) -> None:
        with pytest.raises(ConfigError, match="not an SQS URL"):
            Settings(agent_queue_url="http://example.com/q").require_queue()

    def test_require_queue_returns_the_url(self) -> None:
        url = "https://sqs.us-east-1.amazonaws.com/000000000000/agent-reports-fanout"
        assert Settings(agent_queue_url=url).require_queue() == url

    def test_load_settings_validates_by_default(self) -> None:
        with pytest.raises(ConfigError):
            load_settings({f"{ENV_PREFIX}SES_SENDER": "broken"})
        assert load_settings({f"{ENV_PREFIX}SES_SENDER": "broken"}, validate=False) is not None

    def test_redacted_exposes_every_field(self) -> None:
        assert set(Settings().redacted()) == set(Settings().__dataclass_fields__)
