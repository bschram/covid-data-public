import pathlib
from typing import Optional, MutableMapping

import pandas as pd

from covidactnow.datapublic.common_fields import CommonFields

UNEXPECTED_COLUMNS_MESSAGE = "DataFrame columns do not match expected fields"


class FieldNameAndCommonField(str):
    """Represents the original field/column name and CommonField it maps to or None if dropped."""

    def __new__(cls, field_name: str, common_field: Optional[CommonFields]):
        o = super().__new__(cls, field_name)
        o.common_field = common_field
        return o


def load_county_fips_data(fips_csv: pathlib.Path) -> pd.DataFrame:
    df = pd.read_csv(fips_csv, dtype={"fips": str})
    df["fips"] = df.fips.str.zfill(5)
    return df


def rename_fields(df, fields, already_transformed_fields, log) -> pd.DataFrame:
    """Return df with columns renamed according to fields, logging and dropping unexpected columns."""
    extra_fields = set(df.columns) - set(fields) - already_transformed_fields
    missing_fields = set(fields) - set(df.columns)
    if extra_fields or missing_fields:
        # If this warning happens in a test you may need to edit the sample data in test/data
        # to make sure all the expected fields appear in the sample.
        log.warning(
            UNEXPECTED_COLUMNS_MESSAGE, extra_fields=extra_fields, missing_fields=missing_fields,
        )
    rename: MutableMapping[str, str] = {f: f for f in already_transformed_fields}
    for col in df.columns:
        field = fields.get(col)
        if field and field.common_field:
            if field.value in rename:
                raise AssertionError(f"Field {repr(field)} misconfigured")
            rename[field.value] = field.common_field.value
    # Copy only columns in `rename.keys()` to a new DataFrame and rename.
    df = df.loc[:, list(rename.keys())].rename(columns=rename)
    return df
