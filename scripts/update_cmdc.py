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
from covidactnow.datapublic.common_df import (
    write_df_as_csv,
    sort_common_field_columns,
    only_common_columns,
)
from covidactnow.datapublic.common_fields import (
    GetByValueMixin,
    CommonFields,
    COMMON_FIELDS_TIMESERIES_KEYS,
)
from scripts.update_covid_data_scraper import FieldNameAndCommonField, load_county_fips_data
import cmdc

from scripts.update_test_and_trace import load_census_state

DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"


# Keep in sync with COMMON_FIELD_MAP in cmdc.py in the covid-data-model repo.
# Fields commented out with tag 20200616 were not found in the data used by update_cmdc_test.py.
class Fields(GetByValueMixin, FieldNameAndCommonField, Enum):
    LOCATION = "location", None  # Special transformation to FIPS
    DT = "dt", CommonFields.DATE

    NEGATIVE_TESTS_TOTAL = "negative_tests_total", CommonFields.NEGATIVE_TESTS
    POSITIVE_TESTS_TOTAL = "positive_tests_total", CommonFields.POSITIVE_TESTS

    # Not found 20200616 ACTIVE_TOTAL = "active_total", None
    CASES_TOTAL = "cases_total", CommonFields.CASES
    # Not found 20200616 CASES_CONFIRMED = "cases_confirmed", None
    # Not found 20200616 RECOVERED_TOTAL = "recovered_total", CommonFields.RECOVERED
    DEATHS_TOTAL = "deaths_total", CommonFields.DEATHS
    # Not found 20200616 DEATHS_CONFIRMED = "deaths_confirmed", None
    # Not found 20200616 DEATHS_SUSPECTED = "deaths_suspected", None

    HOSPITAL_BEDS_CAPACITY_COUNT = "hospital_beds_capacity_count", CommonFields.STAFFED_BEDS
    HOSPITAL_BEDS_IN_USE_COVID_CONFIRMED = "hospital_beds_in_use_covid_confirmed", None
    # Not found 20200616 HOSPITAL_BEDS_IN_USE_COVID_NEW = "hospital_beds_in_use_covid_new", None
    HOSPITAL_BEDS_IN_USE_COVID_SUSPECTED = "hospital_beds_in_use_covid_suspected", None
    HOSPITAL_BEDS_IN_USE_ANY = "hospital_beds_in_use_any", CommonFields.HOSPITAL_BEDS_IN_USE_ANY
    HOSPITAL_BEDS_IN_USE_COVID_TOTAL = (
        "hospital_beds_in_use_covid_total",
        CommonFields.CURRENT_HOSPITALIZED,
    )

    ICU_BEDS_CAPACITY_COUNT = "icu_beds_capacity_count", CommonFields.ICU_BEDS
    ICU_BEDS_IN_USE_COVID_CONFIRMED = "icu_beds_in_use_covid_confirmed", None
    ICU_BEDS_IN_USE_COVID_SUSPECTED = "icu_beds_in_use_covid_suspected", None
    ICU_BEDS_IN_USE_ANY = "icu_beds_in_use_any", CommonFields.CURRENT_ICU_TOTAL
    ICU_BEDS_IN_USE_COVID_TOTAL = (
        "icu_beds_in_use_covid_total",
        CommonFields.CURRENT_ICU,
    )

    # Not found 20200616 VENTILATORS_CAPACITY_COUNT = "ventilators_capacity_count", None
    VENTILATORS_IN_USE_COVID_TOTAL = (
        "ventilators_in_use_covid_total",
        CommonFields.CURRENT_VENTILATED,
    )
    # Not found 20200616 VENTILATORS_IN_USE_COVID_CONFIRMED = "ventilators_in_use_covid_confirmed", None
    # Not found 20200616 VENTILATORS_IN_USE_COVID_SUSPECTED = "ventilators_in_use_covid_suspected", None
    # Not found 20200616 VENTILATORS_IN_USE_ANY = (
    #    "ventilators_in_use_any",
    #    None,
    # )


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

        cmdc_client.covid_us()
        df = cmdc_client.fetch()
        # Transform FIPS from an int64 to a string of 2 or 5 chars. See
        # https://github.com/valorumdata/cmdc.py/issues/3
        df[CommonFields.FIPS] = df[Fields.LOCATION].apply(lambda v: f"{v:0>{2 if v < 100 else 5}}")

        # Already transformed from Fields to CommonFields
        already_transformed_fields = {CommonFields.FIPS}

        extra_fields = set(df.columns) - set(Fields) - already_transformed_fields
        missing_fields = set(Fields) - set(df.columns)
        if extra_fields or missing_fields:
            # If this warning happens in a test you may need to edit the sample data in test/data
            # to make sure all the expected fields appear in the sample.
            self.log.warning(
                "columns from cmdc do not match Fields",
                extra_fields=extra_fields,
                missing_fields=missing_fields,
            )

        # TODO(tom): Factor out this rename and re-order code. It is stricter than
        # update_covid_data_scraper because this code expects every field in the source DataFrame
        # to appear in Fields.
        rename: MutableMapping[str, str] = {f: f for f in already_transformed_fields}
        for col in df.columns:
            field = Fields.get(col)
            if field and field.common_field:
                if field.value in rename:
                    raise AssertionError("Field misconfigured")
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
                bad_fips=counties.loc[no_match_counties_mask, CommonFields.FIPS].unique().tolist(),
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

        df = sort_common_field_columns(df)

        bad_rows = (
            df[CommonFields.FIPS].isnull()
            | df[CommonFields.DATE].isnull()
            | df[CommonFields.STATE].isnull()
        )
        if bad_rows.any():
            self.log.warning(
                "Dropping rows with null in important columns", bad_rows=str(df.loc[bad_rows])
            )
            df = df.loc[~bad_rows]

        fl_data_rows = df[CommonFields.FIPS].str.match(r"^12")
        if fl_data_rows.any():
            self.log.warning("Dropping rows for FL", dropped_rows=str(df.loc[fl_data_rows]))
            df = df.loc[~fl_data_rows]

        df = df.set_index(COMMON_FIELDS_TIMESERIES_KEYS, verify_integrity=True)
        return df


if __name__ == "__main__":
    common_init.configure_logging()
    log = structlog.get_logger()
    transformer = CmdcTransformer.make_with_data_root(
        DATA_ROOT, os.environ.get("CMDC_API_KEY", None)
    )
    write_df_as_csv(
        only_common_columns(transformer.transform(), log),
        DATA_ROOT / "cases-cmdc" / "timeseries-common.csv",
        log,
    )
