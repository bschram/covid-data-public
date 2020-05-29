from io import StringIO
import numpy as np
import numbers
from typing import List

import pandas as pd
import pytest

from scripts import update_covid_data_scraper
from scripts.update_covid_data_scraper import strip_whitespace


# turns all warnings into errors for this module
pytestmark = pytest.mark.filterwarnings("error")


def is_empty(v):
    if v is None:
        return True
    if v == "":
        return True
    if not isinstance(v, numbers.Number):
        return False
    return np.isnan(v)


def to_dict(keys: List[str], df: pd.DataFrame):
    """Transforms df into a dict mapping columns `keys` to a dict of the record/row in df.

    Use this to extract the values from a DataFrame for easier comparisons in assert statements.
    """
    try:
        if any(df.index.names):
            df = df.reset_index()
        df = df.set_index(keys)
        records_without_nas = {}
        for key, values in df.to_dict(orient="index").items():
            records_without_nas[key] = {k: v for k, v in values.items() if not is_empty(v)}
        return records_without_nas
    except Exception:
        # Print df to provide more context when the above code raises.
        print(f"Problem with {df}")
        raise


def test_remove_duplicate_city_data():
    input_df = pd.read_csv(
        StringIO(
            "city,county,state,fips,date,metric_a\n"
            "Smithville,,ZZ,97123,2020-03-23,march23-removed\n"
            "Smithville,,ZZ,97123,2020-03-22,march22-kept\n"
            "New York City,,ZZ,97324,2020-03-22,march22-ny-patched\n"
            ",North County,ZZ,97001,2020-03-22,county-not-touched\n"
            ",North County,ZZ,97001,2020-03-23,county-not-touched\n"
        )
    )

    output_df = update_covid_data_scraper.remove_duplicate_city_data(input_df)
    expected_df = pd.read_csv(
        StringIO(
            "city,county,state,fips,date,metric_a\n"
            "Smithville,Smithville,ZZ,97123,2020-03-22,march22-kept\n"
            "New York City,New York,ZZ,97324,2020-03-22,march22-ny-patched\n"
            ",North County,ZZ,97001,2020-03-22,county-not-touched\n"
            ",North County,ZZ,97001,2020-03-23,county-not-touched\n"
        )
    )

    assert to_dict(["fips", "date"], output_df) == to_dict(["fips", "date"], expected_df)


def test_strip_whitespace():
    input_df = pd.read_csv(
        StringIO("col_a,col_b,col_c,col_num\n" " ,b1,c1,1\n" "a2, b2,c2,2\n" ',b3," c3 ",3\n')
    )
    output_df = strip_whitespace(input_df)
    expected_df = pd.read_csv(
        StringIO("col_a,col_b,col_c,col_num\n" ",b1,c1,1\n" "a2,b2,c2,2\n" ",b3,c3,3\n")
    )
    assert to_dict(["col_c"], output_df) == to_dict(["col_c"], expected_df)


def test_transform():
    transformer = update_covid_data_scraper.TransformCovidDataScraper.make_with_data_root(
        update_covid_data_scraper.DATA_ROOT
    )
    df = transformer.transform()
    assert df
