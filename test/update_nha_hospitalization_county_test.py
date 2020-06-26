import structlog

import requests_mock

from covidactnow.datapublic.common_fields import CommonFields
from covidactnow.datapublic.common_test_helpers import to_dict
from scripts.update_nha_hospitalization_county import SOURCE_URL, CsvCopy, DATA_ROOT


def test_nha_basic():
    with structlog.testing.capture_logs() as logs, requests_mock.Mocker() as m:
        m.get(
            SOURCE_URL,
            text="""foo,bar
date,county_name,vents
04/01,Carson City,300
04/02,Clark,200
""",
        )
        transformer = CsvCopy.make_with_data_root(DATA_ROOT)
        df = transformer.transform()
    assert to_dict([CommonFields.FIPS, CommonFields.DATE], df) == {
        ("32510", "2020-04-01"): {"county": "Carson City", "vents": "300"},
        ("32003", "2020-04-02"): {"county": "Clark County", "vents": "200"},
    }
    assert [l["event"] for l in logs] == ["Fetching URL"]


def test_bad_county():
    with structlog.testing.capture_logs() as logs, requests_mock.Mocker() as m:
        m.get(
            SOURCE_URL,
            text="""foo,bar
date,county_name,vents
04/01,Not A County,100
""",
        )
        transformer = CsvCopy.make_with_data_root(DATA_ROOT)
        df = transformer.transform()
    assert to_dict([CommonFields.FIPS, CommonFields.DATE], df) == {}
    assert [l["event"] for l in logs] == [
        "Fetching URL",
        "Imported county name not found in FIPS data",
    ]


def test_bad_float():
    with structlog.testing.capture_logs() as logs, requests_mock.Mocker() as m:
        m.get(
            SOURCE_URL,
            text="""foo,bar
date,county_name,vents
04/01,Carson City,#REF!
""",
        )
        transformer = CsvCopy.make_with_data_root(DATA_ROOT)
        df = transformer.transform()
    assert to_dict([CommonFields.FIPS, CommonFields.DATE], df) == {
        ("32510", "2020-04-01"): {"county": "Carson City"},
    }
    assert [l["event"] for l in logs] == ["Fetching URL", "Dropping value not a float"]
