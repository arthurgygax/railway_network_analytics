"""Exit codes are the operator contract: they are what a supervisor or CI acts on.

Every case here fails before any network call, so the suite stays offline.
"""

from __future__ import annotations

import logging

import pytest

from railway_network_analytics.__main__ import (
    EXIT_CONFIG,
    EXIT_SINK,
    EXIT_TARGETS,
    _redact,
    main,
)
from railway_network_analytics.config import Config

ENV_KEYS = (
    "POLL_TARGETS_PATH", "STATE_DIR", "TIMETABLES_CLIENT_ID", "TIMETABLES_API_KEY",
    "OUTPUT_DIR", "POLL_INTERVAL_SECONDS", "HTTP_TIMEOUT_SECONDS", "MAX_POLLS",
    "LOG_LEVEL", "LOG_FORMAT", "SINK", "KAFKA_TOPIC", "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_USERNAME", "KAFKA_PASSWORD", "KAFKA_CA_CERT",
)


@pytest.fixture(autouse=True)
def _restore_root_logger():
    """main() reconfigures the root logger; put it back so tests stay independent."""
    root = logging.getLogger()
    saved = (root.handlers[:], root.level)
    yield
    root.handlers[:], root.level = saved


@pytest.fixture
def clean_env(monkeypatch):
    for key in ENV_KEYS:
        monkeypatch.delenv(key, raising=False)
    return monkeypatch


def test_invalid_config_exits_2_and_explains_on_stderr(clean_env, poll_targets, capsys):
    clean_env.setenv("POLL_TARGETS_PATH", str(poll_targets()))
    clean_env.setenv("TIMETABLES_CLIENT_ID", "id")
    clean_env.setenv("TIMETABLES_API_KEY", "key")
    clean_env.setenv("POLL_INTERVAL_SECONDS", "nope")
    assert main() == EXIT_CONFIG
    assert "POLL_INTERVAL_SECONDS" in capsys.readouterr().err


def test_missing_credentials_exit_2(clean_env, poll_targets, capsys):
    clean_env.setenv("POLL_TARGETS_PATH", str(poll_targets()))
    assert main() == EXIT_CONFIG
    err = capsys.readouterr().err
    assert "TIMETABLES_CLIENT_ID is required" in err
    assert "TIMETABLES_API_KEY is required" in err


def test_empty_poll_targets_exits_3(clean_env, poll_targets, tmp_path):
    clean_env.setenv("POLL_TARGETS_PATH", str(poll_targets([])))
    clean_env.setenv("TIMETABLES_CLIENT_ID", "id")
    clean_env.setenv("TIMETABLES_API_KEY", "key")
    clean_env.setenv("OUTPUT_DIR", str(tmp_path / "out"))
    clean_env.setenv("STATE_DIR", str(tmp_path / "state"))
    assert main() == EXIT_TARGETS


def test_unreadable_poll_targets_exits_3(clean_env, tmp_path):
    broken = tmp_path / "poll_targets.json"
    broken.write_text("{not json", encoding="utf-8")
    clean_env.setenv("POLL_TARGETS_PATH", str(broken))
    clean_env.setenv("TIMETABLES_CLIENT_ID", "id")
    clean_env.setenv("TIMETABLES_API_KEY", "key")
    clean_env.setenv("OUTPUT_DIR", str(tmp_path / "out"))
    assert main() == EXIT_TARGETS


def test_unwritable_output_dir_exits_4_before_any_network_call(
    clean_env, poll_targets, tmp_path
):
    readonly = tmp_path / "ro"
    readonly.mkdir()
    readonly.chmod(0o500)
    clean_env.setenv("POLL_TARGETS_PATH", str(poll_targets()))
    clean_env.setenv("TIMETABLES_CLIENT_ID", "id")
    clean_env.setenv("TIMETABLES_API_KEY", "key")
    clean_env.setenv("OUTPUT_DIR", str(readonly / "out"))
    clean_env.setenv("STATE_DIR", str(tmp_path / "state"))
    try:
        assert main() == EXIT_SINK
    finally:
        readonly.chmod(0o700)


def test_secrets_are_redacted_from_the_config_log(poll_targets):
    """We now hold two secrets. Logging config wholesale is an easy way to leak them."""
    config = Config.from_env({
        "POLL_TARGETS_PATH": str(poll_targets()),
        "TIMETABLES_CLIENT_ID": "public-id",
        "TIMETABLES_API_KEY": "SECRET-API-KEY",
        "KAFKA_PASSWORD": "SECRET-PASSWORD",
    })
    redacted = _redact(config)
    assert redacted["timetables_api_key"] == "***"
    assert redacted["kafka_password"] == "***"
    # Non-secrets must stay visible or the log is useless for debugging.
    assert redacted["timetables_client_id"] == "public-id"
    assert redacted["kafka_ca_cert"] == "certs/ca.pem"
    assert "SECRET" not in str(redacted)
