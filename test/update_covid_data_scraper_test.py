from io import StringIO

import pandas as pd
import pytest
import structlog

from covidactnow.datapublic.common_fields import CommonFields
from covidactnow.datapublic.common_test_helpers import to_dict

from scripts import update_covid_data_scraper

# turns all warnings into errors for this module
from scripts.update_helpers import UNEXPECTED_COLUMNS_MESSAGE

pytestmark = pytest.mark.filterwarnings("error")


def test_transform():
    with structlog.testing.capture_logs() as logs:
        transformer = update_covid_data_scraper.CovidDataScraperTransformer.make_with_data_root(
            update_covid_data_scraper.DATA_ROOT, structlog.get_logger(),
        )
        transformer.timeseries_csv_local_path = StringIO(
            "locationID,county,country,state,level,cases,deaths,tested,date\n"
            "iso1:us#iso2:us-ak#fips:02013,Aleutians East Borough,United States,Alaska,county,10,1,100,2020-06-01\n"
            "iso1:us#iso2:us-ak#fips:02013,Aleutians East Borough,United States,Alaska,county,11,1,110,2020-06-02\n"
            "iso1:us#iso2:us-ak,,United States,Alaska,state,20,2,200,2020-06-01\n"
            "iso1:us#iso2:us-ak#(unassigned),,United States,Alaska,state,2000,200,20000,2020-06-01\n"
            "iso1:us#iso2:us-ak#(unassigned),,United States,Alaska,county,2000,200,20000,2020-06-01\n"
        )
        df = transformer.transform()

    expected_df = pd.read_csv(
        StringIO(
            "country,county,state,fips,aggregate_level,date,cases,deaths,negative_tests\n"
            "USA,Aleutians East Borough,AK,02013,county,2020-06-01,10,1,90\n"
            "USA,Aleutians East Borough,AK,02013,county,2020-06-02,11,1,99\n"
            "USA,,AK,02,state,2020-06-01,20,2,180"
        ),
        dtype={CommonFields.FIPS: str},
        low_memory=False,
        parse_dates=[CommonFields.DATE],
    )

    assert to_dict(["fips", "date"], df) == to_dict(["fips", "date"], expected_df)

    assert [l["event"] for l in logs] == [
        "Dropping county rows with unexpected locationID",
        "Dropping state rows with unexpected locationID",
        UNEXPECTED_COLUMNS_MESSAGE,
    ]
