import os
import pathlib
import sys
from enum import Enum
from typing import Union, MutableMapping, Optional
import pandas as pd

import structlog
from pydantic import BaseModel
from structlog._config import BoundLoggerLazyProxy

from covidactnow.datapublic import common_init
from covidactnow.datapublic.common_df import write_df_as_csv
from covidactnow.datapublic.common_fields import GetByValueMixin, CommonFields
from scripts.update_covid_data_scraper import FieldNameAndCommonField, load_county_fips_data
import cmdc

from scripts.update_test_and_trace import load_census_state

DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"


# Keep in sync with COMMON_FIELD_MAP in cmdc.py in the covid-data-model repo.
class Fields(GetByValueMixin, FieldNameAndCommonField, Enum):
    VINTAGE = "vintage", None
    FIPS = "fips", CommonFields.FIPS
    DT = "dt", CommonFields.DATE
    ACTIVE_TOTAL = "active_total", None
    CASES_TOTAL = "cases_total", None
    DEATHS_TOTAL = "deaths_total", CommonFields.DEATHS
    HOSPITAL_BEDS_IN_USE_COVID_CONFIRMED = "hospital_beds_in_use_covid_confirmed", None
    HOSPITAL_BEDS_IN_USE_COVID_SUSPECTED = "hospital_beds_in_use_covid_suspected", None
    HOSPITAL_BEDS_IN_USE_COVID_TOTAL = "hospital_beds_in_use_covid_total", None
    ICU_BEDS_IN_USE_COVID_CONFIRMED = "icu_beds_in_use_covid_confirmed", None
    ICU_BEDS_IN_USE_COVID_SUSPECTED = "icu_beds_in_use_covid_suspected", None
    ICU_BEDS_IN_USE_COVID_TOTAL = (
        "icu_beds_in_use_covid_total",
        CommonFields.CURRENT_ICU,
    )
    NEGATIVE_TESTS_TOTAL = "negative_tests_total", CommonFields.NEGATIVE_TESTS
    POSITIVE_TESTS_TOTAL = "positive_tests_total", CommonFields.POSITIVE_TESTS
    RECOVERED_TOTAL = "recovered_total", None
    VENTILATORS_IN_USE_COVID_TOTAL = (
        "ventilators_in_use_covid_total",
        CommonFields.CURRENT_VENTILATED,
    )


class CmdcTransformer(BaseModel):
    """Get the newest data from Valorum / Covid Modeling Data Collaborative and return a DataFrame
    of timeseries."""

    # API key, see https://github.com/valorumdata/cmdc.py#api-keys
    cmdc_key: Optional[str]

    # Path of a text file of state names, copied from census.gov
    census_state_path: pathlib.Path

    # FIPS for each county, by name
    county_fips_csv: pathlib.Path

    log: Union[structlog.BoundLoggerBase, BoundLoggerLazyProxy]

    class Config:
        arbitrary_types_allowed = True

    @staticmethod
    def make_with_data_root(
        data_root: pathlib.Path, cmdc_key: Optional[str] = None
    ) -> "CmdcTransformer":
        return CmdcTransformer(
            cmdc_key=cmdc_key,
            census_state_path=data_root / "misc" / "state.txt",
            county_fips_csv=data_root / "misc" / "fips_population.csv",
            log=structlog.get_logger(),
        )

    def transform(self) -> pd.DataFrame:
        cmdc_client = cmdc.Client(apikey=self.cmdc_key)

        cmdc_client.covid()
        df = cmdc_client.fetch()
        # Keep only the most recent VINTAGE of each FIPS, DT. See
        # https://github.com/valorumdata/cmdc.py/issues/2
        df = df.sort_values(Fields.VINTAGE).groupby([Fields.FIPS, Fields.DT]).last().reset_index()
        # Transform FIPS from an int64 to a string of 2 or 5 chars. See
        # https://github.com/valorumdata/cmdc.py/issues/3
        df[Fields.FIPS] = df[Fields.FIPS].apply(lambda v: f"{v:0>{2 if v < 100 else 5}}")

        extra_fields = set(df.columns) - set(Fields)
        missing_fields = set(Fields) - set(df.columns)
        if extra_fields or missing_fields:
            self.log.warning(
                "columns from cmdc do not match Fields",
                extra_fields=extra_fields,
                missing_fields=missing_fields,
            )

        # TODO(tom): Factor out this rename and re-order code. It is stricter than
        # update_covid_data_scraper because this code expects every field in the source DataFrame
        # to appear in Fields.
        rename: MutableMapping[str, str] = {}
        for col in df.columns:
            field = Fields.get(col)
            if field and field.common_field:
                rename[field.value] = field.common_field.value

        # Copy only columns in `rename.keys()` to a new DataFrame and rename.
        df = df.loc[:, list(rename.keys())].rename(columns=rename)

        df[CommonFields.COUNTRY] = "USA"

        # Partition df by region type so states and counties can by merged with different
        # data to get their names.
        state_mask = df[CommonFields.FIPS].str.len() == 2
        states = df.loc[state_mask, :]
        counties = df.loc[~state_mask, :]

        fips_data = load_county_fips_data(self.county_fips_csv).set_index([CommonFields.FIPS])
        counties = counties.merge(
            fips_data[[CommonFields.STATE, CommonFields.COUNTY]],
            left_on=[CommonFields.FIPS],
            suffixes=(False, False),
            how="left",
            right_index=True,
        )
        no_match_counties_mask = counties.state.isna()
        if no_match_counties_mask.sum() > 0:
            self.log.warning(
                "Some counties did not match by fips",
                bad_fips=counties.loc[no_match_counties_mask][CommonFields.FIPS].tolist(),
            )
        counties = counties.loc[~no_match_counties_mask, :]
        counties[CommonFields.AGGREGATE_LEVEL] = "county"

        state_df = load_census_state(self.census_state_path).set_index(CommonFields.FIPS)
        states = states.merge(
            state_df[[CommonFields.STATE]],
            left_on=[CommonFields.FIPS],
            suffixes=(False, False),
            how="left",
            right_index=True,
        )
        states[CommonFields.AGGREGATE_LEVEL] = "state"

        df = pd.concat([states, counties])

        # Sort columns to match the order of CommonFields.
        common_order = {common: i for i, common in enumerate(CommonFields)}
        df = df.loc[:, sorted(df.columns, key=lambda c: common_order[c])]

        df = df.set_index([CommonFields.FIPS, CommonFields.DATE], verify_integrity=True)
        return df


if __name__ == "__main__":
    common_init.configure_structlog()
    transformer = CmdcTransformer.make_with_data_root(DATA_ROOT, os.environ.get("CMDC_KEY", None))
    write_df_as_csv(
        transformer.transform(),
        DATA_ROOT / "cases-cmdc" / "timeseries-common.csv",
        structlog.get_logger(),
    )
