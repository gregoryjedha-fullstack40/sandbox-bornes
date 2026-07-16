import pytest
import pandas as pd
from .etl import collect_data

# # Sample DataFrame as a fixture
# @pytest.fixture
# def sample_data():
#     return pd.DataFrame({"feature": [1, 2, None, 4], "target": [2, 4, 6, 8]})


def test_collect_data_belib():
    df = collect_data("belib_stat", fromdate=None, todate=None, force=True)
    assert df.isnull().sum().sum() == 0  # Ensure no NaN values

def test_collect_data_irve():
    df2 = collect_data("irve_conso", fromdate=None, todate=None, force=True)
    assert df2.isnull().sum().sum() == 0  # Ensure no NaN values
