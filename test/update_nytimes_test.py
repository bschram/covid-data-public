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

    results = update_nytimes_data.remove_state_backfilled_cases(data, backfill_records)

    if is_ct_county:
        expected_cases = pd.Series([1000, 1100, 1200], name="cases")
    else:
        expected_cases = pd.Series([1000, 1288, 1388], name="cases")

    pd.testing.assert_series_equal(expected_cases, results.cases)


def test_remove_county_backfill():

    backfill = [("48113", "2020-08-17", 500)]
    data_buf = io.StringIO(
        "fips,state,date,aggregate_level,cases\n"
        f"48112,TX,2020-08-17,county,1700\n"
        f"48112,TX,2020-08-18,county,1700\n"
        f"48113,TX,2020-08-16,county,1000\n"
        f"48113,TX,2020-08-17,county,1600\n"
        f"48113,TX,2020-08-18,county,1700\n"
        f"48,TX,2020-08-16,state,2600\n"
        f"48,TX,2020-08-17,state,3600\n"
        f"48,TX,2020-08-18,state,4700\n"
    )
    data = common_df.read_csv(data_buf, set_index=False)
    results = update_nytimes_data.remove_county_backfilled_cases(data, backfill)

    # Days before 8/17 should be the same
    # days on/after 8/17 should have 500 less cases
    data_buf = io.StringIO(
        "fips,state,date,aggregate_level,cases\n"
        f"48112,TX,2020-08-17,county,1700\n"
        f"48112,TX,2020-08-18,county,1700\n"
        f"48113,TX,2020-08-16,county,1000\n"
        f"48113,TX,2020-08-17,county,1100\n"
        f"48113,TX,2020-08-18,county,1200\n"
        f"48,TX,2020-08-16,state,2600\n"
        f"48,TX,2020-08-17,state,3100\n"
        f"48,TX,2020-08-18,state,4200\n"
    )
    expected = common_df.read_csv(data_buf, set_index=False)

    pd.testing.assert_frame_equal(results, expected)


def test_remove_ma_county_cases():
    data_buf = io.StringIO(
        "fips,state,date,aggregate_level,cases\n"
        "25025,MA,2020-08-10,county,1000\n"
        "25025,MA,2020-08-11,county,1000\n"
        "25025,MA,2020-08-12,county,1000\n"
        "25025,MA,2020-08-13,county,1000\n"
        "25025,MA,2020-08-14,county,1025\n"
        "25025,MA,2020-08-19,county,1030\n"
        "25025,MA,2020-08-20,county,1030\n"
        "25,MA,2020-08-11,state,1000\n"
        "25,MA,2020-08-12,state,1000\n"
        "25,MA,2020-08-13,state,1000\n"
    )
    data = common_df.read_csv(data_buf, set_index=False)

    results = update_nytimes_data._remove_ma_county_zeroes_data(data)
    results = results.sort_values(["fips", "date"]).reset_index(drop=True)
    # State data should be untouched
    # MA County data on/before 8/11 should be untouched
    # MA County data that changes after 8/11 should be picked up.
    data_buf = io.StringIO(
        "fips,state,date,aggregate_level,cases\n"
        "25,MA,2020-08-11,state,1000\n"
        "25,MA,2020-08-12,state,1000\n"
        "25,MA,2020-08-13,state,1000\n"
        "25025,MA,2020-08-10,county,1000\n"
        "25025,MA,2020-08-11,county,1000\n"
        "25025,MA,2020-08-14,county,1025\n"
        "25025,MA,2020-08-19,county,1030\n"
        "25025,MA,2020-08-20,county,1030\n"
    )
    expected = common_df.read_csv(data_buf, set_index=False)
    pd.testing.assert_frame_equal(results, expected)
