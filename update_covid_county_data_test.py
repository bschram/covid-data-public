import pytest
import structlog
from more_itertools import one

from scripts.update_covid_county_data import CovidCountyDataTransformer, DATA_ROOT
import requests_mock

from scripts.helpers import UNEXPECTED_COLUMNS_MESSAGE

# turns all warnings into errors for this module
pytestmark = pytest.mark.filterwarnings("error")


# Fetched with:
# curl https://api.covidcountydata.org/swagger.json > test/data/api.covidcountydata.org_swagger.json
SWAGGER_JSON_URL = "https://api.covidcountydata.org/swagger.json"
SWAGGER_JSON_PATH = "test/data/api.covidcountydata.org_swagger.json"

# Fetched with:
# curl -H "Accept: text/csv" https://api.covidcountydata.org/covid_us > test/data/api.covidcountydata.org_covid_us_all_csv
# Dates and locations picked to get coverage of all the variables:
# cat test/data/api.covidcountydata.org_covid_us_all_csv | csvgrep -c location -r '\A(48347|34002|34001|6025|26121|21|42009|19017)\Z' | csvgrep -c dt -r '(2020-06-13|2020-08-07|2020-08-10)' > test/data/api.covidcountydata.org_covid_us_csv
COVID_US_URL = "https://api.covidcountydata.org/covid_us"
COVID_US_PATH = "test/data/api.covidcountydata.org_covid_us_csv"


# Remote requests are mocked out so in theory this key is never sent over the network.
TEST_APIKEY = "covidactnow-local-test-key"


def test_update_covid_county_data_basic():
    with structlog.testing.capture_logs() as logs, requests_mock.Mocker() as m:
        m.get(SWAGGER_JSON_URL, text=open(SWAGGER_JSON_PATH).read())
        m.get(COVID_US_URL, text=open(COVID_US_PATH).read())
        transformer = CovidCountyDataTransformer.make_with_data_root(
            DATA_ROOT, TEST_APIKEY, structlog.get_logger()
        )
        df = transformer.transform()
    assert not df.empty
    assert logs == []


def test_update_covid_county_data_renamed_field():
    with structlog.testing.capture_logs() as logs, requests_mock.Mocker() as m:
        m.get(SWAGGER_JSON_URL, text=open(SWAGGER_JSON_PATH).read())
        covid_csv = open(COVID_US_PATH).read().replace("hospital_beds_in_use_any", "foobar")
        m.get(COVID_US_URL, text=covid_csv)
        transformer = CovidCountyDataTransformer.make_with_data_root(
            DATA_ROOT, TEST_APIKEY, structlog.get_logger()
        )
        df = transformer.transform()
    assert not df.empty
    log_entry = one(logs)
    assert log_entry["event"] == UNEXPECTED_COLUMNS_MESSAGE
    assert log_entry["missing_fields"] == {"hospital_beds_in_use_any"}
    assert log_entry["extra_fields"] == {"foobar"}


def test_update_covid_county_data_bad_fips():
    with structlog.testing.capture_logs() as logs, requests_mock.Mocker() as m:
        m.get(SWAGGER_JSON_URL, text=open(SWAGGER_JSON_PATH).read())
        covid_csv = open(COVID_US_PATH).read().replace("6025", "31337")
        m.get(COVID_US_URL, text=covid_csv)
        transformer = CovidCountyDataTransformer.make_with_data_root(
            DATA_ROOT, TEST_APIKEY, structlog.get_logger()
        )
        df = transformer.transform()
    assert not df.empty
    log_entry = one(logs)
    assert log_entry["event"] == "Some counties did not match by fips"
    assert log_entry["bad_fips"] == ["31337"]


def test_update_covid_county_data_drop_empty_state():
    with structlog.testing.capture_logs() as logs, requests_mock.Mocker() as m:
        m.get(SWAGGER_JSON_URL, text=open(SWAGGER_JSON_PATH).read())
        covid_csv = open(COVID_US_PATH).read().replace("6025", "0")
        m.get(COVID_US_URL, text=covid_csv)
        transformer = CovidCountyDataTransformer.make_with_data_root(
            DATA_ROOT, TEST_APIKEY, structlog.get_logger()
        )
        df = transformer.transform()
    assert not df.empty
    log_entry = one(logs)
    assert log_entry["event"] == "Dropping rows with null in important columns"
    assert "2 rows" in log_entry["bad_rows"]
