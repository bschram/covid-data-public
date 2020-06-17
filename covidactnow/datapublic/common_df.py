"""
Shared code that handles `pandas.DataFrames` objects.
"""

import pathlib
from typing import TextIO, Union

import pandas as pd
import numpy as np
from structlog import stdlib

from covidactnow.datapublic.common_fields import (
    CommonFields,
    COMMON_FIELDS_ORDER_MAP,
    COMMON_FIELDS_TIMESERIES_KEYS,
)


def fix_df_index(df: pd.DataFrame, log: stdlib.BoundLogger) -> pd.DataFrame:
    """Return a `DataFrame` with the CAN CommonFields index or the unmodified input if already set."""
    if df.index.names != COMMON_FIELDS_TIMESERIES_KEYS:
        log.warning("Fixing DataFrame index", current_index=df.index.names)
        if df.index.names != [None]:
            df = df.reset_index(inplace=False)
        df = df.set_index(COMMON_FIELDS_TIMESERIES_KEYS, inplace=False)
    df = df.sort_index()

    if "index" in df.columns:
        # This is not expected in our normal code path but seems to sneak in occasionally
        # when calling reset_index on a DataFrame that doesn't have a named index.
        log.warning("Dropping column named 'index'")
        df = df.drop(columns="index")

    df = sort_common_field_columns(df)

    return df


def only_common_columns(df: pd.DataFrame, log: stdlib.BoundLogger) -> pd.DataFrame:
    """Return a DataFrame with columns not in CommonFields dropped."""
    extra_columns = {col for col in df.columns if CommonFields.get(col) is None}
    if extra_columns:
        log.warning("Dropping columns not in CommonFields", extra_columns=extra_columns)
        df = df.drop(columns=extra_columns)
    return df


def write_csv(df: pd.DataFrame, path: pathlib.Path, log: stdlib.BoundLogger) -> None:
    """Write `df` to `path` as a CSV with index set by `fix_df_index`."""
    df = fix_df_index(df, log)
    log.info("Writing DataFrame", current_index=df.index.names)
    # A column with floats and pd.NA (which is different from np.nan) is given type 'object' and does
    # not get formatted by to_csv float_format. Changing the pd.NA to np.nan seems to let convert_dtypes
    # to change 'object' columns to 'float64' and 'Int64'.
    df = df.replace({pd.NA: np.nan}).convert_dtypes()
    # Format that outputs floats without a fraction as an integer without decimal point. Very large and small
    # floats (uncommon in our data) are output in exponent format. We currently output a large number of fractional
    # sig digits; 7 is likely enough but I don't see a way to limit them when formatting output.
    df.to_csv(path, date_format="%Y-%m-%d", index=True, float_format="%.12g")


# Alias to support old name. Please `import common_df` and call `common_df.write_csv(...)`.
write_df_as_csv = write_csv


def read_csv(path_or_buf: Union[pathlib.Path, TextIO]) -> pd.DataFrame:
    """Read `path_or_buf` containing CommonFields and return a DataFrame with timeseries index set."""
    return pd.read_csv(
        path_or_buf,
        parse_dates=[CommonFields.DATE],
        dtype={CommonFields.FIPS: str},
        low_memory=False,
    ).set_index(COMMON_FIELDS_TIMESERIES_KEYS)


# Alias to support old name. Please `import common_df` and call `common_df.read_csv(...)`.
read_csv_to_indexed_df = read_csv


def strip_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """Return `df` with `str.strip` applied to columns with `object` dtype."""

    def strip_series(col):
        if col.dtypes == object:
            return col.str.strip()
        else:
            return col

    return df.apply(strip_series, axis=0)


def sort_common_field_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Sort columns to match the order of CommonFields, followed by remaining columns in alphabetical order."""
    this_columns_order = {
        col: COMMON_FIELDS_ORDER_MAP.get(col, i + len(COMMON_FIELDS_ORDER_MAP))
        for i, col in enumerate(sorted(df.columns))
    }
    return df.loc[:, sorted(df.columns, key=lambda c: this_columns_order[c])]
