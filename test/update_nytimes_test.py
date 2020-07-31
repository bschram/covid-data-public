import structlog
import io

import pytest
import pandas as pd

from more_itertools import one
from covidactnow.datapublic import common_df
from scripts import update_nytimes_data
from scripts.update_nytimes_data import NYTimesUpdater, DATA_ROOT
from covidactnow.datapublic import common_test_helpers
import requests_mock


def test_update_nytimes_virgin_islands():

    updater = NYTimesUpdater.make_with_data_root(DATA_ROOT)
    data = common_df.read_csv(
        io.StringIO(
            "county,state_full_name,aggregate_level,fips,date,cases,deaths\n"
            ",Virgin Islands,state,78,2020-07-31,10,1\n"
        )
    ).reset_index()
    results = updater.transform(data)

    expected = common_df.read_csv(
        io.StringIO(
            "country,county,state_full_name,state,aggregate_level,fips,date,cases,deaths\n"
            "USA,,U.S. Virgin Islands,VI,state,78,2020-07-31,10,1\n"
        )
    ).reset_index()
    results_dict = common_test_helpers.to_dict(["state", "state_full_name"], data)
    expected_dict = common_test_helpers.to_dict(["state", "state_full_name"], expected)
    assert results_dict == expected_dict


@pytest.mark.parametrize("is_ct_county", [True, False])
def test_remove_ct_cases(is_ct_county):
    backfill_records = [("09", "2020-07-24", 188)]
    if is_ct_county:
        fips = "09001"
    else:
        fips = "36061"

    data_buf = io.StringIO(
        "fips,state,date,aggregate_level,cases\n"
        f"{fips},CT,2020-07-23,county,1000\n"
        f"{fips},CT,2020-07-24,county,1288\n"
        f"{fips},CT,2020-07-25,county,1388\n"
    )

    data = common_df.read_csv(data_buf)
    data = data.reset_index()

    results = update_nytimes_data.remove_backfilled_cases(data, backfill_records)

    if is_ct_county:
        expected_cases = pd.Series([1000, 1100, 1200], name="cases")
    else:
        expected_cases = pd.Series([1000, 1288, 1388], name="cases")

    pd.testing.assert_series_equal(expected_cases, results.cases)
