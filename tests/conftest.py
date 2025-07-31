"""Configuration and fixture definitions for testing."""

import pytest


def record_found_in_log(
    caplog: pytest.LogCaptureFixture,
    find: tuple[int, str],
) -> bool:
    """Look for a specific logging record in the captured log.

    Arguments:
        caplog: An instance of the caplog fixture
        find: A tuple giving the logging level and message to look for

    """

    try:
        # Iterate over the record tuples, ignoring the leading element
        # giving the logger name
        _ = next(msg for msg in caplog.record_tuples if msg[1:] == find)
        return True
    except StopIteration:
        return False


@pytest.fixture
def fixture_config():
    """Create a dummy configuration object."""
    from ve_data_science_tool.config import Config

    return Config(
        repository_path="dummy_value",
        app_client_uuid="dummy_value",
        app_client_name="dummy_value",
        remote_collection_uuid="dummy_value",
        local_collection_uuid="dummy_value",
    )


@pytest.fixture(scope="session", autouse=True)
def fixture_mocked_config(session_mocker):
    """Mock the load config call for all tests."""
    from ve_data_science_tool.config import Config

    mocked_value = Config(
        repository_path="dummy_value",
        app_client_uuid="dummy_value",
        app_client_name="dummy_value",
        remote_collection_uuid="dummy_value",
        local_collection_uuid="dummy_value",
    )

    session_mocker.patch(
        "ve_data_science_tool.config.load_config", return_value=mocked_value
    )
