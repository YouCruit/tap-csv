"""Tests standard tap features using the built-in SDK tests library."""

import os

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

    assert "tilde.txt:000000012" in output
