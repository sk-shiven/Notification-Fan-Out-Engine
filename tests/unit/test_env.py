"""Environment smoke test."""

import fanout


def test_fanout_import():
    assert fanout.__version__ == "0.1.0"
