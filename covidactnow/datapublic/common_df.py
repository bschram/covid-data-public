"""
Shared code that handles `pandas.DataFrames` objects.
"""

import pathlib

import pandas as pd
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
    extra_columns = {col for col in df.columns if CommonFields.get(col) is None}
    if extra_columns:
        log.warning("Dropping columns not in CommonFields", extra_columns=extra_columns)
        df = df.drop(columns=extra_columns)
    df = sort_common_field_columns(df)

    return df


def write_df_as_csv(df: pd.DataFrame, path: pathlib.Path, log: stdlib.BoundLogger) -> None:
    """Write `df` to `path` as a CSV with index set by `fix_df_index`."""
    df = fix_df_index(df, log)
    log.info("Writing DataFrame", current_index=df.index.names)
    df.to_csv(path, date_format="%Y-%m-%d", index=True)


def read_csv_to_indexed_df(path: pathlib.Path) -> pd.DataFrame:
    """Read `path` containing CommonFields and return a DataFrame with timeseries index set."""
    return pd.read_csv(
        path, parse_dates=[CommonFields.DATE], dtype={CommonFields.FIPS: str}, low_memory=False,
    ).set_index(COMMON_FIELDS_TIMESERIES_KEYS)


def strip_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """Return `df` with `str.strip` applied to columns with `object` dtype."""

    def strip_series(col):
        if col.dtypes == object:
            return col.str.strip()
        else:
            return col

    return df.apply(strip_series, axis=0)


def sort_common_field_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Sort columns to match the order of CommonFields."""
    return df.loc[:, sorted(df.columns, key=lambda c: COMMON_FIELDS_ORDER_MAP[c])]
