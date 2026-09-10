from __future__ import annotations

import pytest

from railway_network_analytics.config import Config, ConfigError


def env(targets_path, **overrides) -> dict[str, str]:
    return {"POLL_TARGETS_PATH": str(targets_path),
            "TIMETABLES_CLIENT_ID": "id", "TIMETABLES_API_KEY": "key", **overrides}


def test_defaults_are_applied(poll_targets):
    path = poll_targets()
    config = Config.from_env(env(path))
    assert config.poll_targets_path.name == "poll_targets.json"
    assert config.poll_interval_seconds == 3600
    assert config.max_polls == 0
    assert config.log_level == "INFO"


def test_overrides_are_parsed_and_typed(poll_targets):
    config = Config.from_env(
        env(poll_targets(), POLL_INTERVAL_SECONDS="5", LOG_LEVEL="debug", LOG_FORMAT="JSON")
    )
    assert config.poll_interval_seconds == 5
    assert isinstance(config.poll_interval_seconds, int)
    assert config.log_level == "DEBUG"  # normalised
    assert config.log_format == "json"


def test_empty_string_is_treated_as_unset(poll_targets):
    """Docker and compose pass empty variables readily; they must not blank a default."""
    config = Config.from_env(env(poll_targets(), POLL_INTERVAL_SECONDS="", LOG_LEVEL=""))
    assert config.poll_interval_seconds == 3600
    assert config.log_level == "INFO"


def test_missing_poll_targets_file_is_rejected(tmp_path):
    with pytest.raises(ConfigError, match="does not exist"):
        Config.from_env({"POLL_TARGETS_PATH": str(tmp_path / "nope.json"),
                         "TIMETABLES_CLIENT_ID": "i", "TIMETABLES_API_KEY": "k"})


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"POLL_INTERVAL_SECONDS": "thirty"}, "not an integer"),
        ({"POLL_INTERVAL_SECONDS": "0"}, "must be > 0"),
        ({"HTTP_TIMEOUT_SECONDS": "-1"}, "must be > 0"),
        ({"MAX_POLLS": "-1"}, "must be >= 0"),
        ({"LOG_LEVEL": "LOUD"}, "LOG_LEVEL"),
        ({"LOG_FORMAT": "yaml"}, "LOG_FORMAT"),
            ],
)
def test_invalid_values_are_rejected(poll_targets, overrides, expected):
    with pytest.raises(ConfigError, match=expected.replace("(", r"\(").replace(")", r"\)")):
        Config.from_env(env(poll_targets(), **overrides))


def test_max_polls_zero_is_valid(poll_targets):
    """0 means 'run forever' — it is the one sentinel that must survive validation."""
    assert Config.from_env(env(poll_targets(), MAX_POLLS="0")).max_polls == 0


def test_all_errors_are_reported_at_once(poll_targets):
    """One restart per mistake is a bad operator experience."""
    with pytest.raises(ConfigError) as exc:
        Config.from_env(env(poll_targets(), POLL_INTERVAL_SECONDS="x", LOG_LEVEL="LOUD"))
    assert "POLL_INTERVAL_SECONDS" in str(exc.value)
    assert "LOG_LEVEL" in str(exc.value)


def test_config_is_immutable(poll_targets):
    config = Config.from_env(env(poll_targets()))
    with pytest.raises(AttributeError):
        config.poll_interval_seconds = 1  # type: ignore[misc]


def test_sink_defaults_to_jsonl_and_needs_no_kafka_credentials(poll_targets):
    """Offline runs and the whole test suite must work without Kafka secrets."""
    config = Config.from_env(env(poll_targets()))
    assert config.sink == "jsonl"
    assert config.kafka_bootstrap_servers == ""


def test_kafka_sink_requires_credentials(poll_targets):
    with pytest.raises(ConfigError) as exc:
        Config.from_env(env(poll_targets(), SINK="kafka"))
    message = str(exc.value)
    assert "KAFKA_BOOTSTRAP_SERVERS is required" in message
    assert "KAFKA_USERNAME is required" in message
    assert "KAFKA_PASSWORD is required" in message


def test_kafka_sink_accepts_full_credentials(poll_targets, tmp_path):
    ca = tmp_path / "ca.pem"
    ca.write_text("-----BEGIN CERTIFICATE-----")
    config = Config.from_env(env(
        poll_targets(), SINK="kafka", KAFKA_BOOTSTRAP_SERVERS="host:9092",
        KAFKA_USERNAME="avnadmin", KAFKA_PASSWORD="hunter2", KAFKA_CA_CERT=str(ca),
    ))
    assert config.sink == "kafka"
    assert config.kafka_topic == "railway.db.stop_observations"


def test_kafka_sink_rejects_missing_ca_cert(poll_targets, tmp_path):
    with pytest.raises(ConfigError, match="KAFKA_CA_CERT"):
        Config.from_env(env(
            poll_targets(), SINK="kafka", KAFKA_BOOTSTRAP_SERVERS="host:9092",
            KAFKA_USERNAME="u", KAFKA_PASSWORD="p", KAFKA_CA_CERT=str(tmp_path / "nope.pem"),
        ))


def test_unknown_sink_is_rejected(poll_targets):
    with pytest.raises(ConfigError, match="must be 'jsonl' or 'kafka'"):
        Config.from_env(env(poll_targets(), SINK="postgres"))
