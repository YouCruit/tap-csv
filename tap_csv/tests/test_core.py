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

    # Verify state messages are written as expected
    (o, e) = tap_sync_test(TapCSV(config=SAMPLE_CONFIG))
    output = o.getvalue()

    assert("""{"type": "STATE", "value": {"bookmarks": {"test": {"starting_replication_value": null, "replication_key": "replication_key", "replication_key_value": "alphabet.csv:1"}}}}""" in output)
    assert(output.endswith("""{"type": "STATE", "value": {"bookmarks": {"test": {"replication_key": "replication_key", "replication_key_value": "alphabet.csv:3"}}}}\n"""))
