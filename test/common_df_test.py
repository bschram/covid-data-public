from io import StringIO
import pandas as pd
import pytest
import temppathlib
from more_itertools import one

from covidactnow.datapublic import common_df
from covidactnow.datapublic.common_df import (
    strip_whitespace,
    read_csv_to_indexed_df,
    only_common_columns,
)
from covidactnow.datapublic.common_test_helpers import to_dict
from covidactnow.datapublic.common_fields import CommonFields, COMMON_FIELDS_TIMESERIES_KEYS
import structlog.testing

# turns all warnings into errors for this module
pytestmark = pytest.mark.filterwarnings("error")


def test_strip_whitespace():
    input_df = pd.read_csv(
        StringIO(
            """col_a,col_b,col_c,col_num
,b1,c1,1
a2, b2,c2,2
,b3," c3 ",3
"""
        )
    )
    output_df = strip_whitespace(input_df)
    expected_df = pd.read_csv(
        StringIO(
            """col_a,col_b,col_c,col_num
,b1,c1,1
a2,b2,c2,2
,b3,c3,3
"""
        )
    )
    assert to_dict(["col_c"], output_df) == to_dict(["col_c"], expected_df)


def test_write_csv_empty():
    df = pd.DataFrame([], columns=[CommonFields.DATE, CommonFields.FIPS, CommonFields.CASES])
    with temppathlib.NamedTemporaryFile("w+") as tmp, structlog.testing.capture_logs() as logs:
        common_df.write_csv(df, tmp.path, structlog.get_logger())
        assert "fips,date,cases\n" == tmp.file.read()
    assert [l["event"] for l in logs] == ["Fixing DataFrame index", "Writing DataFrame"]


def test_write_csv_extra_columns_dropped():
    df = pd.DataFrame(
        [], columns=[CommonFields.DATE, CommonFields.FIPS, "extra1", CommonFields.CASES, "extra2"]
    )
    df = df.set_index(COMMON_FIELDS_TIMESERIES_KEYS)
    with temppathlib.NamedTemporaryFile("w+") as tmp, structlog.testing.capture_logs() as logs:
        log = structlog.get_logger()
        common_df.write_csv(only_common_columns(df, log), tmp.path, log)
        assert "fips,date,cases\n" == tmp.file.read()
    assert [l["event"] for l in logs] == [
        "Dropping columns not in CommonFields",
        "Writing DataFrame",
    ]


def test_write_csv_columns_are_sorted_in_output_with_extras():
    df = pd.DataFrame(
        [], columns=[CommonFields.DATE, CommonFields.FIPS, "extra2", CommonFields.CASES, "extra1"]
    )
    df = df.set_index(COMMON_FIELDS_TIMESERIES_KEYS)
    with temppathlib.NamedTemporaryFile("w+") as tmp, structlog.testing.capture_logs() as logs:
        log = structlog.get_logger()
        common_df.write_csv(df, tmp.path, log)
        assert "fips,date,cases,extra1,extra2\n" == tmp.file.read()
    assert [l["event"] for l in logs] == [
        "Writing DataFrame",
    ]


def test_write_csv():
    df = pd.DataFrame(
        {
            CommonFields.DATE: ["2020-04-01", "2020-04-02"],
            CommonFields.FIPS: ["06045", "45123"],
            CommonFields.CASES: [234, 456],
        }
    )
    df_original = df.copy()
    expected_csv = """fips,date,cases
06045,2020-04-01,234
45123,2020-04-02,456
"""
    # Call common_df.write_csv with index set to ["fips", "date"], the expected normal index.
    with temppathlib.NamedTemporaryFile("w+") as tmp, structlog.testing.capture_logs() as logs:
        common_df.write_csv(df.set_index(["fips", "date"]), tmp.path, structlog.get_logger())
        assert expected_csv == tmp.file.read()
    assert [l["event"] for l in logs] == ["Writing DataFrame"]

    # Pass df with other index that will be changed. Check that the same output is written to the
    # file.
    with temppathlib.NamedTemporaryFile("w+") as tmp, structlog.testing.capture_logs() as logs:
        common_df.write_csv(df, tmp.path, structlog.get_logger())
        assert expected_csv == tmp.file.read()
    assert [l["event"] for l in logs] == ["Fixing DataFrame index", "Writing DataFrame"]

    with temppathlib.NamedTemporaryFile("w+") as tmp, structlog.testing.capture_logs() as logs:
        common_df.write_csv(df.set_index(["date", "cases"]), tmp.path, structlog.get_logger())
        assert expected_csv == tmp.file.read()
    assert [l["event"] for l in logs] == ["Fixing DataFrame index", "Writing DataFrame"]

    with temppathlib.NamedTemporaryFile("w+") as tmp, structlog.testing.capture_logs() as logs:
        common_df.write_csv(df.set_index(["date", "fips"]), tmp.path, structlog.get_logger())
        assert expected_csv == tmp.file.read()
    assert [l["event"] for l in logs] == ["Fixing DataFrame index", "Writing DataFrame"]

    assert repr(df) == repr(df_original)


def test_write_csv_without_date():
    df = pd.DataFrame(
        {
            CommonFields.FIPS: ["06045", "45123"],
            "extra_index": ["idx_1", "idx_2"],
            CommonFields.CASES: [234, 456],
            "extra_column": ["extra_data", None],
        }
    )
    expected_csv = """fips,extra_index,cases,extra_column
06045,idx_1,234,extra_data
45123,idx_2,456,
"""
    with temppathlib.NamedTemporaryFile("w+") as tmp, structlog.testing.capture_logs() as logs:
        common_df.write_csv(
            df, tmp.path, structlog.get_logger(), index_names=[CommonFields.FIPS, "extra_index"]
        )
        assert expected_csv == tmp.file.read()
    assert [l["event"] for l in logs] == ["Fixing DataFrame index", "Writing DataFrame"]


def test_read_csv():
    input_csv = """fips,date,cases
06045,2020-04-01,234
45123,2020-04-02,456
    """

    with temppathlib.NamedTemporaryFile("w+") as tmp:
        tmp.path.write_text(input_csv)
        df = read_csv_to_indexed_df(tmp.path)
    assert one(df.loc[("06045", "2020-04-01"), "cases"]) == 234


def test_read_csv_no_index():
    input_csv = """fips,date,cases
06045,2020-04-01,234
45123,2020-04-02,456
    """

    with temppathlib.NamedTemporaryFile("w+") as tmp:
        tmp.path.write_text(input_csv)
        df = common_df.read_csv(tmp.path, set_index=False)

    expected_first_row = ["06045", pd.Timestamp("2020-04-01 00:00:00"), 234]
    assert list(df.iloc[0]) == expected_first_row


def test_float_formatting():
    input_csv = """fips,date,col_1,col_2,col_3,col_4,col_5,col_6
99123,2020-04-01,1,2.0000000,3,0.0004,0.00005,6000000000
99123,2020-04-02,,,,,,
99123,2020-04-03,1,2,3.1234567,4,5,6.0
"""
    input_df = read_csv_to_indexed_df(StringIO(input_csv))

    expected_csv = """fips,date,col_1,col_2,col_3,col_4,col_5,col_6
99123,2020-04-01,1,2,3,0.0004,5e-05,6000000000
99123,2020-04-02,,,,,,
99123,2020-04-03,1,2,3.1234567,4,5,6
"""

    with temppathlib.NamedTemporaryFile("w+") as tmp, structlog.testing.capture_logs() as logs:
        common_df.write_csv(input_df, tmp.path, structlog.get_logger())
        assert expected_csv == tmp.file.read()

    assert [l["event"] for l in logs] == ["Writing DataFrame"]


def test_float_na_formatting():
    df = pd.DataFrame(
        [("99", "2020-04-01", 1.0, 2, 3), ("99", "2020-04-02", pd.NA, pd.NA, None)],
        columns="fips date metric_a metric_b metric_c".split(),
    ).set_index(COMMON_FIELDS_TIMESERIES_KEYS)

    expected_csv = """fips,date,metric_a,metric_b,metric_c
99,2020-04-01,1,2,3
99,2020-04-02,,,
"""

    with temppathlib.NamedTemporaryFile("w+") as tmp, structlog.testing.capture_logs() as logs:
        common_df.write_csv(df, tmp.path, structlog.get_logger())
        assert expected_csv == tmp.file.read()

    assert [l["event"] for l in logs] == ["Writing DataFrame"]


def test_remove_index_column():
    df = pd.DataFrame(
        [("99", "2020-04-01", "a", 123)], columns=["fips", "date", "index", "cases"]
    ).set_index(COMMON_FIELDS_TIMESERIES_KEYS)

    with temppathlib.NamedTemporaryFile("w+") as tmp, structlog.testing.capture_logs() as logs:
        common_df.write_csv(df, tmp.path, structlog.get_logger())
        assert "fips,date,cases\n99,2020-04-01,123\n" == tmp.file.read()

    assert [l["event"] for l in logs] == ["Dropping column named 'index'", "Writing DataFrame"]
