import pytest
import pandas as pd


def test_contains_notna(data):
    """
    Test whether data contains notna values before integrate its to Clickhouse DB
    """
    assert data.notna.mean().mean() == 1, "Data contains Not NA value. NA not accepted in Clickhouse DB"
