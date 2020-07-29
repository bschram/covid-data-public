import structlog
from more_itertools import one

from scripts.update_cmdc import CmdcTransformer, DATA_ROOT
import requests_mock

from scripts.helpers import UNEXPECTED_COLUMNS_MESSAGE


def test_update_cmdc():
    # This test and others depend on data files that can be updated by:
    # curl https://api.covid.valorum.ai/swagger.json > test/data/api.covid.valorum.ai_swagger.json
    # curl https://api.covid.valorum.ai/covid |grep -P '"fips":(6|6075),' |  \
    #   grep -P '"dt":"2020-06-1[0123]"' > test/data/api.covid.valorum.ai_covid_us
    # followed by manual fixing of JSON in api.covid.valorum.ai_covid_us to wrap the list in [] and remove
    # the last ','.
    with structlog.testing.capture_logs() as logs, requests_mock.Mocker() as m:
        m.get(
            "https://api.covid.valorum.ai/swagger.json",
            text=open("test/data/api.covid.valorum.ai_swagger.json").read(),
        )
        m.get(
            "https://api.covid.valorum.ai/covid_us",
            text=open("test/data/api.covid.valorum.ai_covid_us").read(),
        )
        # TODO(tom): Pass in apikey when https://github.com/valorumdata/cmdc.py/issues/9 is fixed.
        # Same in other tests.
        transformer = CmdcTransformer.make_with_data_root(DATA_ROOT, None, structlog.get_logger())
        df = transformer.transform()
    assert not df.empty
    assert logs == []


def test_update_cmdc_renamed_field():
    with structlog.testing.capture_logs() as logs, requests_mock.Mocker() as m:
        m.get(
            "https://api.covid.valorum.ai/swagger.json",
            text=open("test/data/api.covid.valorum.ai_swagger.json").read(),
        )
        covid_json = (
            open("test/data/api.covid.valorum.ai_covid_us")
            .read()
            .replace("hospital_beds_in_use_covid_confirmed", "foobar")
        )
        m.get("https://api.covid.valorum.ai/covid_us", text=covid_json)
        transformer = CmdcTransformer.make_with_data_root(DATA_ROOT, None, structlog.get_logger())
        df = transformer.transform()
    assert not df.empty
    log_entry = one(logs)
    assert log_entry["event"] == UNEXPECTED_COLUMNS_MESSAGE
    assert log_entry["missing_fields"] == {"hospital_beds_in_use_covid_confirmed"}
    assert log_entry["extra_fields"] == {"foobar"}


def test_update_cmdc_bad_fips():
    with structlog.testing.capture_logs() as logs, requests_mock.Mocker() as m:
        m.get(
            "https://api.covid.valorum.ai/swagger.json",
            text=open("test/data/api.covid.valorum.ai_swagger.json").read(),
        )
        covid_json = open("test/data/api.covid.valorum.ai_covid_us").read().replace("6075", "31337")
        m.get("https://api.covid.valorum.ai/covid_us", text=covid_json)
        transformer = CmdcTransformer.make_with_data_root(DATA_ROOT, None, structlog.get_logger())
        df = transformer.transform()
    assert not df.empty
    log_entry = one(logs)
    assert log_entry["event"] == "Some counties did not match by fips"
    assert log_entry["bad_fips"] == ["31337"]


def test_update_cmdc_drop_empty_state():
    with structlog.testing.capture_logs() as logs, requests_mock.Mocker() as m:
        m.get(
            "https://api.covid.valorum.ai/swagger.json",
            text=open("test/data/api.covid.valorum.ai_swagger.json").read(),
        )
        covid_json = open("test/data/api.covid.valorum.ai_covid_us").read().replace("6075", "0")
        m.get("https://api.covid.valorum.ai/covid_us", text=covid_json)
        transformer = CmdcTransformer.make_with_data_root(DATA_ROOT, None, structlog.get_logger())
        df = transformer.transform()
    assert not df.empty
    log_entry = one(logs)
    assert log_entry["event"] == "Dropping rows with null in important columns"
    assert "4 rows" in log_entry["bad_rows"]
