"""Tests standard tap features using the built-in SDK tests library."""

import os

import pytest
from singer_sdk.testing import get_standard_tap_tests, tap_sync_test

from tap_csv.tap import TapCSV


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    test_data_dir = os.path.dirname(os.path.abspath(__file__))
    SAMPLE_CONFIG = {
        "files": [
            {
                "entity": "test",
                "path": f"{test_data_dir}/data/alphabet.csv",
                "keys": [],
            }
        ]
    }
    tests = get_standard_tap_tests(TapCSV, config=SAMPLE_CONFIG)
    for test in tests:
        test()


def test_incremental():
    """Tests incremental state handling"""
    test_data_dir = os.path.dirname(os.path.abspath(__file__))
    SAMPLE_CONFIG = {
        "files": [
            {
                "entity": "test",
                "path": f"{test_data_dir}/data/alphabet.csv",
                "keys": [],
            }
        ]
    }

    # Verify state messages are written as expected
    (o, e) = tap_sync_test(TapCSV(config=SAMPLE_CONFIG))
    output = o.getvalue()
    print(output)

    assert (
        """{"type": "STATE", "value": {"bookmarks": {"test": {"starting_replication_value": null, "replication_key": "replication_key", "replication_key_value": "alphabet.csv:000000001"}}}}"""  # NOQA
        in output
    )
    assert output.endswith(
        """{"type": "STATE", "value": {"bookmarks": {"test": {"replication_key": "replication_key", "replication_key_value": "alphabet.csv:000000012"}}}}\n"""  # NOQA
    )


def test_incremental_state_in_config():
    """Tests incremental state handling"""
    test_data_dir = os.path.dirname(os.path.abspath(__file__))
    SAMPLE_CONFIG = {
        "files": [
            {
                "entity": "test",
                "path": f"{test_data_dir}/data/alphabet.csv",
                "start_from": "AlpHAbeT.csv:000000002",
                "keys": [],
            }
        ]
    }

    # Verify state messages are written as expected
    (o, e) = tap_sync_test(TapCSV(config=SAMPLE_CONFIG))
    output = o.getvalue()
    print(output)

    assert "alphabet.csv:000000001" not in output
    assert "alphabet.csv:000000002" in output
    assert output.endswith(
        """{"type": "STATE", "value": {"bookmarks": {"test": {"replication_key": "replication_key", "replication_key_value": "alphabet.csv:000000012"}}}}\n"""  # NOQA
    )


def test_global_path_in_config_overriden_by_specific_path():
    """Tests config of global default path"""
    test_data_dir = os.path.dirname(os.path.abspath(__file__))
    SAMPLE_CONFIG = {
        "path": "/path/to/global",
        "files": [
            {
                "entity": "test",
                "path": f"{test_data_dir}/data/alphabet.csv",
                "keys": [],
            }
        ],
    }

    # Verify state messages are written as expected
    (o, e) = tap_sync_test(TapCSV(config=SAMPLE_CONFIG))
    output = o.getvalue()
    print(output)

    assert "alphabet.csv:000000012" in output


def test_global_path():
    """Tests config of global default path"""
    test_data_dir = os.path.dirname(os.path.abspath(__file__))
    SAMPLE_CONFIG = {
        "path": f"{test_data_dir}/data",
        "files": [
            {
                "entity": "test",
                "prefix": "alphabet",
                "keys": [],
            }
        ],
    }

    # Verify state messages are written as expected
    (o, e) = tap_sync_test(TapCSV(config=SAMPLE_CONFIG))
    output = o.getvalue()
    print(output)

    assert '"col1": "a"' in output
    assert '"col2": "h"' in output
    assert '"col3": "i"' in output
    assert "alphabet.csv:000000012" in output


def test_delimiter():
    """Tests config of delimiter"""
    test_data_dir = os.path.dirname(os.path.abspath(__file__))
    SAMPLE_CONFIG = {
        "path": f"{test_data_dir}/data",
        "files": [
            {
                "entity": "tilde",
                "prefix": "tilde",
                "delimiter": "~",
                "keys": ["col1"],
            }
        ],
    }

    # Verify state messages are written as expected
    (o, e) = tap_sync_test(TapCSV(config=SAMPLE_CONFIG))
    output = o.getvalue()
    print(output)

    assert '"col1": "a"' in output
    assert '"col2": "h"' in output
    assert '"col3": "i"' in output
    assert "tilde.txt:000000012" in output


def test_dialect():
    """Tests config of dialect"""
    test_data_dir = os.path.dirname(os.path.abspath(__file__))
    SAMPLE_CONFIG = {
        "path": f"{test_data_dir}/data",
        "files": [
            {
                "entity": "tab",
                "prefix": "tab",
                "dialect": "excel-tab",
                "keys": ["col1"],
            }
        ],
    }

    # Verify state messages are written as expected
    (o, e) = tap_sync_test(TapCSV(config=SAMPLE_CONFIG))
    output = o.getvalue()
    print(output)

    assert '"col1": "a"' in output
    assert '"col2": "h"' in output
    assert '"col3": "i"' in output
    assert "tab.txt:000000012" in output


def test_header():
    """Tests config of header"""
    test_data_dir = os.path.dirname(os.path.abspath(__file__))
    SAMPLE_CONFIG = {
        "path": f"{test_data_dir}/data",
        "files": [
            {
                "entity": "header",
                "prefix": "header_",
                "keys": ["col1"],
                "header": ["col1", "col2", "col3"],
            }
        ],
    }

    # Verify state messages are written as expected
    (o, e) = tap_sync_test(TapCSV(config=SAMPLE_CONFIG))
    output = o.getvalue()
    print(output)

    assert '"col1": "col1"' not in output
    assert '"j":' not in output
    assert '"col1": "a"' in output
    assert '"col1": "g"' in output
    assert '"col1": "j"' in output
    assert '"col1": "m"' in output
    assert '"col1": "p"' in output
    assert '"col2": "h"' in output
    assert '"col3": "r"' in output
    assert "header_a_yes.csv:000000003" in output
    assert "header_z_no.csv:000000002" in output


def test_unknown_config():
    """Tests unknown config value should throw"""
    SAMPLE_CONFIG = {
        "files": [
            {
                "entity": "tilde",
                "prefix": "tilde",
                "delimiter": "~",
                "keys": ["col1"],
                "typo": "foo",
            }
        ],
    }

    with pytest.raises(ValueError):
        tap_sync_test(TapCSV(config=SAMPLE_CONFIG))

    with pytest.raises(ValueError):
        SAMPLE_CONFIG = {}
        SAMPLE_CONFIG["foo"] = "bar"
        SAMPLE_CONFIG["files"] = [{"entity": "foo", "keys": []}]
        tap_sync_test(TapCSV(config=SAMPLE_CONFIG))


def test_files_can_be_empty():
    """Tests files cant be empty"""
    SAMPLE_CONFIG = {
        "files": [],
    }

    # Should not raise
    tap_sync_test(TapCSV(config=SAMPLE_CONFIG))
