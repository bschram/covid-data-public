"""
Shared code that handles `pandas.DataFrames` objects.
"""

import pathlib

import pandas as pd
from structlog import stdlib

from covidactnow.datapublic.common_fields import CommonFields


def fix_df_index(df: pd.DataFrame, log: stdlib.BoundLogger, inplace=False):
    """Modify `df` to revert any existing named index and add standard the FIPS and DATE index."""
    if df.index.names != [CommonFields.FIPS, CommonFields.DATE]:
        log.warning("Fixing DataFrame index", current_index=df.index.names)
        if df.index.names != [None]:
            if inplace:
                df.reset_index(inplace=True)
            else:
                df = df.reset_index(inplace=False)

        if inplace:
            df.set_index([CommonFields.FIPS, CommonFields.DATE], inplace=True)
        else:
            df = df.set_index([CommonFields.FIPS, CommonFields.DATE], inplace=False)

    return None if inplace else df


def write_df_as_csv(df: pd.DataFrame, path: pathlib.Path, log: stdlib.BoundLogger):
    df = fix_df_index(df, log, inplace=False)
    log.info("Writing DataFrame", current_index=df.index.names)
    df.to_csv(path, date_format="%Y-%m-%d", index=True)


def strip_whitespace(df: pd.DataFrame) -> pd.DataFrame:
    def strip_series(col):
        if col.dtypes == object:
            return col.str.strip()
        else:
            return col

    return df.apply(strip_series, axis=0)
