from __future__ import annotations

import string
import time
import urllib
from datetime import datetime, timedelta

import pytest
from hypothesis import given, strategies

from pgqueuer.db import dsn
from pgqueuer.helpers import (
    normalize_cron_expression,
    retry_timer_buffer_timeout,
    timeout_with_jitter,
    timer,
    utc_now,
)


async def test_perf_counter_dt() -> None:
    assert isinstance(utc_now(), datetime)
    assert utc_now().tzinfo is not None


def test_heartbeat_buffer_timeout_empty_list() -> None:
    dts = list[timedelta]()
    expected = timedelta(hours=24)
    assert retry_timer_buffer_timeout(dts) == expected


def test_heartbeat_buffer_timeout_all_dts_less_than_or_equal_to_t0() -> None:
    dts = [timedelta(seconds=-1), timedelta(seconds=0)]
    expected = timedelta(hours=24)
    assert retry_timer_buffer_timeout(dts) == expected


def test_heartbeat_buffer_timeout_positive_dts() -> None:
    dts = [timedelta(seconds=10), timedelta(seconds=5)]
    expected = timedelta(seconds=5)
    assert retry_timer_buffer_timeout(dts) == expected


def test_heartbeat_buffer_timeout_mixed_dts() -> None:
    dts = [timedelta(seconds=-5), timedelta(seconds=10)]
    expected = timedelta(seconds=10)
    assert retry_timer_buffer_timeout(dts) == expected


def test_heartbeat_buffer_timeout_custom_t0() -> None:
    dts = [timedelta(seconds=4), timedelta(seconds=6)]
    expected = timedelta(seconds=6)
    assert retry_timer_buffer_timeout(dts, _t0=timedelta(seconds=5)) == expected


def test_heartbeat_buffer_timeout_custom_default() -> None:
    dts = list[timedelta]()
    expected = timedelta(hours=48)
    assert retry_timer_buffer_timeout(dts, _default=timedelta(hours=48)) == expected


def test_delay_within_jitter_range() -> None:
    base_timeout = timedelta(seconds=10)
    delay_multiplier = 2.0
    jitter_span = (0.8, 1.2)

    # Call the function multiple times to check the jitter range
    for _ in range(100):
        delay = timeout_with_jitter(base_timeout, delay_multiplier, jitter_span)
        base_delay = base_timeout.total_seconds() * delay_multiplier
        assert base_delay * jitter_span[0] <= delay.total_seconds() <= base_delay * jitter_span[1]


def test_delay_is_timedelta() -> None:
    base_timeout = timedelta(seconds=5)
    delay_multiplier = 1.5
    delay = timeout_with_jitter(base_timeout, delay_multiplier)
    assert isinstance(delay, timedelta)


def test_custom_jitter_range() -> None:
    base_timeout = timedelta(seconds=8)
    delay_multiplier = 1.0
    jitter_span = (0.5, 1.5)

    # Call the function multiple times to check the custom jitter range
    for _ in range(100):
        delay = timeout_with_jitter(base_timeout, delay_multiplier, jitter_span)
        base_delay = base_timeout.total_seconds() * delay_multiplier
        assert base_delay * jitter_span[0] <= delay.total_seconds() <= base_delay * jitter_span[1]


@pytest.mark.parametrize(
    "expression, expected",
    (
        ("@hourly", "0 * * * *"),
        ("@midnight", "0 0 * * *"),
    ),
)
def test_normalize_cron_expression(expression: str, expected: str) -> None:
    assert normalize_cron_expression(expression) == expected


def test_timer() -> None:
    with timer() as elapsed:
        t1 = elapsed()
        time.sleep(0.01)
        t2 = elapsed()
        assert t2 > t1

    assert elapsed() == elapsed()

    with pytest.raises(ValueError):
        with timer() as elapsed:
            raise ValueError

        assert elapsed() == elapsed()


def test_dsn_all_parameters() -> None:
    """Test that the DSN is correctly constructed when all parameters are provided."""
    result = dsn(
        host="localhost",
        user="myuser",
        password="mypassword",
        database="mydb",
        port="5432",
    )
    assert result == "postgresql://myuser:mypassword@localhost:5432/mydb"


def test_dsn_missing_parameters(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a ValueError is raised when parameters are missing."""
    # Clear relevant environment variables
    monkeypatch.delenv("PGHOST", raising=False)
    monkeypatch.delenv("PGUSER", raising=False)
    monkeypatch.delenv("PGPASSWORD", raising=False)
    monkeypatch.delenv("PGDATABASE", raising=False)
    monkeypatch.delenv("PGPORT", raising=False)

    with pytest.raises(ValueError) as exc_info:
        dsn()
    assert "Missing required parameters" in str(exc_info.value)
    assert "host" in str(exc_info.value)
    assert "user" in str(exc_info.value)
    assert "password" in str(exc_info.value)
    assert "database" in str(exc_info.value)
    assert "port" in str(exc_info.value)


def test_dsn_special_characters() -> None:
    """Test that special characters in user and password are encoded correctly."""
    result = dsn(
        host="localhost",
        user="user@name",
        password="p@ss:word",
        database="mydb",
        port="5432",
    )
    expected = "postgresql://user%40name:p%40ss%3Aword@localhost:5432/mydb"
    assert result == expected


def test_dsn_environment_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that environment variables are used when parameters are not provided."""
    monkeypatch.setenv("PGHOST", "envhost")
    monkeypatch.setenv("PGUSER", "envuser")
    monkeypatch.setenv("PGPASSWORD", "envpassword")
    monkeypatch.setenv("PGDATABASE", "envdb")
    monkeypatch.setenv("PGPORT", "5432")

    result = dsn()
    assert result == "postgresql://envuser:envpassword@envhost:5432/envdb"


def test_dsn_missing_env_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a ValueError is raised when environment variables are missing."""
    monkeypatch.delenv("PGHOST", raising=False)
    monkeypatch.delenv("PGUSER", raising=False)
    monkeypatch.delenv("PGPASSWORD", raising=False)
    monkeypatch.delenv("PGDATABASE", raising=False)
    monkeypatch.delenv("PGPORT", raising=False)

    with pytest.raises(ValueError) as exc_info:
        dsn()
    assert "Missing required parameters" in str(exc_info.value)


def test_dsn_partial_parameters_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that parameters override environment variables when both are provided."""
    monkeypatch.setenv("PGHOST", "envhost")
    monkeypatch.setenv("PGUSER", "envuser")
    monkeypatch.setenv("PGPASSWORD", "envpassword")
    monkeypatch.setenv("PGDATABASE", "envdb")
    monkeypatch.setenv("PGPORT", "5432")

    result = dsn(
        user="paramuser",
        password="parampassword",
    )
    assert result == "postgresql://paramuser:parampassword@envhost:5432/envdb"


def test_dsn_empty_parameters(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a ValueError is raised when parameters are empty strings."""
    # Clear relevant environment variables
    monkeypatch.delenv("PGHOST", raising=False)
    monkeypatch.delenv("PGUSER", raising=False)
    monkeypatch.delenv("PGPASSWORD", raising=False)
    monkeypatch.delenv("PGDATABASE", raising=False)
    monkeypatch.delenv("PGPORT", raising=False)

    with pytest.raises(ValueError) as exc_info:
        dsn(
            host="",
            user="",
            password="",
            database="",
            port="",
        )
    assert "Missing required parameters" in str(exc_info.value)


def test_dsn_none_parameters(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a ValueError is raised when parameters are None."""
    # Clear relevant environment variables
    monkeypatch.delenv("PGHOST", raising=False)
    monkeypatch.delenv("PGUSER", raising=False)
    monkeypatch.delenv("PGPASSWORD", raising=False)
    monkeypatch.delenv("PGDATABASE", raising=False)
    monkeypatch.delenv("PGPORT", raising=False)

    with pytest.raises(ValueError) as exc_info:
        dsn(
            host="",
            user="",
            password="",
            database="",
            port="",
        )
    assert "Missing required parameters" in str(exc_info.value)


def test_dsn_numeric_port() -> None:
    """Test that the function handles numeric port values."""
    result = dsn(
        host="localhost",
        user="myuser",
        password="mypassword",
        database="mydb",
        port=5432,  # Port as an integer
    )
    assert result == "postgresql://myuser:mypassword@localhost:5432/mydb"


def test_dsn_missing_single_parameter(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a ValueError is raised when a single parameter is missing."""
    # Clear relevant environment variables
    monkeypatch.delenv("PGDATABASE", raising=False)

    with pytest.raises(ValueError) as exc_info:
        dsn(
            host="localhost",
            user="myuser",
            password="mypassword",
            database="",  # Missing database
            port="5432",
        )
    assert "Missing required parameters: database" in str(exc_info.value)


def test_dsn_special_characters_in_host() -> None:
    """Test that special characters in the host are handled correctly."""
    result = dsn(
        host="local_host",
        user="user",
        password="pass",
        database="db",
        port="5432",
    )
    assert result == "postgresql://user:pass@local_host:5432/db"


def test_dsn_ipv6_host() -> None:
    """Test that IPv6 addresses are handled correctly."""
    result = dsn(
        host="::1",
        user="user",
        password="pass",
        database="db",
        port="5432",
    )
    assert result == "postgresql://user:pass@::1:5432/db"


def test_dsn_long_password() -> None:
    """Test that long passwords are handled correctly."""
    long_password = "p" * 1000  # Password of 1000 characters
    expected_password = urllib.parse.quote(long_password)
    result = dsn(
        host="localhost",
        user="user",
        password=long_password,
        database="db",
        port="5432",
    )
    expected = f"postgresql://user:{expected_password}@localhost:5432/db"
    assert result == expected


@given(
    host=strategies.text(alphabet=string.ascii_letters + string.digits),
    user=strategies.text(alphabet=string.ascii_letters + string.digits),
    password=strategies.text(alphabet=string.ascii_letters + string.digits),
    database=strategies.text(alphabet=string.ascii_letters + string.digits),
    port=strategies.integers(min_value=1, max_value=2**16),
)
def test_old_vs_new_dsn(
    host: str,
    user: str,
    password: str,
    database: str,
    port: str | int,
) -> None:
    def old_dsn(host: str, user: str, password: str, database: str, port: str) -> str:
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"

    def new_dsn(host: str, user: str, password: str, database: str, port: str | int) -> str:
        # Safely encode user and password
        user = urllib.parse.quote(user)
        password = urllib.parse.quote(password)

        # Construct netloc
        netloc = f"{user}:{password}@{host}:{port}"

        # Construct path
        path = f"/{database}"

        # Build the DSN using urlunparse
        return urllib.parse.urlunparse(("postgresql", netloc, path, "", "", ""))

    assert old_dsn(
        host,
        user,
        password,
        database,
        str(port),
    ) == new_dsn(
        host,
        user,
        password,
        database,
        port,
    )
