"""
Shared code that handles `pandas.DataFrames` objects.
"""

import pathlib

import pandas as pd
from structlog import stdlib

from covidactnow.datapublic.common_fields import CommonFields


def fix_df_index(df: pd.DataFrame, log: stdlib.BoundLogger) -> pd.DataFrame:
    """Return a `DataFrame` with the CAN CommonFields index or the unmodified input if already set."""
    if df.index.names != [CommonFields.FIPS, CommonFields.DATE]:
        log.warning("Fixing DataFrame index", current_index=df.index.names)
        if df.index.names != [None]:
            df = df.reset_index(inplace=False)
        df = df.set_index([CommonFields.FIPS, CommonFields.DATE], inplace=False)

    return df


def write_df_as_csv(df: pd.DataFrame, path: pathlib.Path, log: stdlib.BoundLogger) -> None:
    """Write `df` to `path` as a CSV with index set by `fix_df_index`."""
    df = fix_df_index(df, log)
    log.info("Writing DataFrame", current_index=df.index.names)
    df.to_csv(path, date_format="%Y-%m-%d", index=True)


def strip_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    """Return `df` with `str.strip` applied to columns with `object` dtype."""

    def strip_series(col):
        if col.dtypes == object:
            return col.str.strip()
        else:
            return col

    return df.apply(strip_series, axis=0)
