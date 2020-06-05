import structlog
from more_itertools import one

from scripts.update_cmdc import CmdcTransformer, DATA_ROOT
import requests_mock


def test_update_cmdc():
    with structlog.testing.capture_logs() as logs, requests_mock.Mocker() as m:
        m.get(
            "https://api.covid.valorum.ai/swagger.json",
            text=open("test/data/api.covid.valorum.ai_swagger.json").read(),
        )
        m.get(
            "https://api.covid.valorum.ai/covid",
            text=open("test/data/api.covid.valorum.ai_covid").read(),
        )
        # TODO(tom): Pass in apikey when https://github.com/valorumdata/cmdc.py/issues/9 is fixed.
        # Same in other tests.
        transformer = CmdcTransformer.make_with_data_root(DATA_ROOT, None)
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
            open("test/data/api.covid.valorum.ai_covid")
            .read()
            .replace("hospital_beds_in_use_covid_confirmed", "foobar")
        )
        m.get("https://api.covid.valorum.ai/covid", text=covid_json)
        transformer = CmdcTransformer.make_with_data_root(DATA_ROOT, None)
        df = transformer.transform()
    assert not df.empty
    log_entry = one(logs)
    assert log_entry["event"] == "columns from cmdc do not match Fields"
    assert log_entry["missing_fields"] == {"hospital_beds_in_use_covid_confirmed"}
    assert log_entry["extra_fields"] == {"foobar"}


def test_update_cmdc_bad_fips():
    with structlog.testing.capture_logs() as logs, requests_mock.Mocker() as m:
        m.get(
            "https://api.covid.valorum.ai/swagger.json",
            text=open("test/data/api.covid.valorum.ai_swagger.json").read(),
        )
        covid_json = open("test/data/api.covid.valorum.ai_covid").read().replace("6075", "31337")
        m.get("https://api.covid.valorum.ai/covid", text=covid_json)
        transformer = CmdcTransformer.make_with_data_root(DATA_ROOT, None)
        df = transformer.transform()
    assert not df.empty
    log_entry = one(logs)
    assert log_entry["event"] == "Some counties did not match by fips"
    assert log_entry["bad_fips"] == ["31337"]
