from typing import Union, MutableMapping, Optional
import os
import pathlib
import sys
from enum import Enum
import pandas as pd

import structlog
import pydantic
from structlog._config import BoundLoggerLazyProxy

from covidactnow.datapublic import common_init
from covidactnow.datapublic import common_df
from covidactnow.datapublic.common_fields import (
    GetByValueMixin,
    CommonFields,
    COMMON_FIELDS_TIMESERIES_KEYS,
    FieldNameAndCommonField,
)
from scripts import helpers
import covidcountydata


DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"


# Keep in sync with COMMON_FIELD_MAP in covid_county_data.py in the covid-data-model repo.
# Fields commented out with tag 20200616 were not found in the data used by update_covid_county_data_test.py.
class Fields(GetByValueMixin, FieldNameAndCommonField, Enum):
    LOCATION = "location", None  # Special transformation to FIPS
    DT = "dt", CommonFields.DATE

    NEGATIVE_TESTS_TOTAL = "negative_tests_total", CommonFields.NEGATIVE_TESTS
    POSITIVE_TESTS_TOTAL = "positive_tests_total", CommonFields.POSITIVE_TESTS
    TESTS_TOTAL = "tests_total", CommonFields.TOTAL_TESTS

    ACTIVE_TOTAL = "active_total", None
    CASES_TOTAL = "cases_total", CommonFields.CASES
    CASES_CONFIRMED = "cases_confirmed", None
    CASES_SUSPECTED = "cases_suspected", None
    RECOVERED_TOTAL = "recovered_total", CommonFields.RECOVERED
    DEATHS_TOTAL = "deaths_total", CommonFields.DEATHS
    DEATHS_CONFIRMED = "deaths_confirmed", None
    DEATHS_SUSPECTED = "deaths_suspected", None

    HOSPITAL_BEDS_CAPACITY_COUNT = "hospital_beds_capacity_count", CommonFields.STAFFED_BEDS
    HOSPITAL_BEDS_IN_USE_COVID_CONFIRMED = "hospital_beds_in_use_covid_confirmed", None
    HOSPITAL_BEDS_IN_USE_COVID_NEW = "hospital_beds_in_use_covid_new", None
    HOSPITAL_BEDS_IN_USE_COVID_SUSPECTED = "hospital_beds_in_use_covid_suspected", None
    HOSPITAL_BEDS_IN_USE_ANY = "hospital_beds_in_use_any", CommonFields.HOSPITAL_BEDS_IN_USE_ANY
    HOSPITAL_BEDS_IN_USE_COVID_TOTAL = (
        "hospital_beds_in_use_covid_total",
        CommonFields.CURRENT_HOSPITALIZED,
    )
    NUM_HOSPITALS_REPORTING = "num_hospitals_reporting", None
    NUM_OF_HOSPITALS = "num_of_hospitals", None

    ICU_BEDS_CAPACITY_COUNT = "icu_beds_capacity_count", CommonFields.ICU_BEDS
    ICU_BEDS_IN_USE_COVID_CONFIRMED = "icu_beds_in_use_covid_confirmed", None
    ICU_BEDS_IN_USE_COVID_SUSPECTED = "icu_beds_in_use_covid_suspected", None
    ICU_BEDS_IN_USE_ANY = "icu_beds_in_use_any", CommonFields.CURRENT_ICU_TOTAL
    ICU_BEDS_IN_USE_COVID_TOTAL = (
        "icu_beds_in_use_covid_total",
        CommonFields.CURRENT_ICU,
    )

    VENTILATORS_IN_USE_ANY = "ventilators_in_use_any", None
    VENTILATORS_CAPACITY_COUNT = "ventilators_capacity_count", None
    VENTILATORS_IN_USE_COVID_TOTAL = (
        "ventilators_in_use_covid_total",
        CommonFields.CURRENT_VENTILATED,
    )
    VENTILATORS_IN_USE_COVID_CONFIRMED = "ventilators_in_use_covid_confirmed", None
    VENTILATORS_IN_USE_COVID_SUSPECTED = "ventilators_in_use_covid_suspected", None


class CovidCountyDataTransformer(pydantic.BaseModel):
    """Get the newest data from Valorum / Covid Modeling Data Collaborative and return a DataFrame
    of timeseries."""

    # API key, see https://github.com/valorumdata/covid_county_data.py#api-keys
    covid_county_data_key: Optional[str]

    # Path of a text file of state names, copied from census.gov
    census_state_path: pathlib.Path

    # FIPS for each county, by name
    county_fips_csv: pathlib.Path

    log: Union[structlog.BoundLoggerBase, BoundLoggerLazyProxy]

    class Config:
        arbitrary_types_allowed = True

    @staticmethod
    def make_with_data_root(
        data_root: pathlib.Path,
        covid_county_data_key: Optional[str],
        log: Union[structlog.BoundLoggerBase, BoundLoggerLazyProxy],
    ) -> "CovidCountyDataTransformer":
        return CovidCountyDataTransformer(
            covid_county_data_key=covid_county_data_key,
            census_state_path=data_root / "misc" / "state.txt",
            county_fips_csv=data_root / "misc" / "fips_population.csv",
            log=log,
        )

    def transform(self) -> pd.DataFrame:
        client = covidcountydata.Client(apikey=self.covid_county_data_key)

        client.covid_us()
        df = client.fetch()
        # Transform FIPS from an int64 to a string of 2 or 5 chars. See
        # https://github.com/valorumdata/covid_county_data.py/issues/3
        df[CommonFields.FIPS] = df[Fields.LOCATION].apply(lambda v: f"{v:0>{2 if v < 100 else 5}}")

        # Already transformed from Fields to CommonFields
        already_transformed_fields = {CommonFields.FIPS}

        df = helpers.rename_fields(df, Fields, already_transformed_fields, self.log)

        df[CommonFields.COUNTRY] = "USA"

        # Partition df by region type so states and counties can by merged with different
        # data to get their names.
        state_mask = df[CommonFields.FIPS].str.len() == 2
        states = df.loc[state_mask, :]
        counties = df.loc[~state_mask, :]

        fips_data = helpers.load_county_fips_data(self.county_fips_csv).set_index(
            [CommonFields.FIPS]
        )
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

        state_df = helpers.load_census_state(self.census_state_path).set_index(CommonFields.FIPS)
        states = states.merge(
            state_df[[CommonFields.STATE]],
            left_on=[CommonFields.FIPS],
            suffixes=(False, False),
            how="left",
            right_index=True,
        )
        states[CommonFields.AGGREGATE_LEVEL] = "state"

        # State level bed data is coming from HHS which tend to not match
        # numbers we're seeing from Covid Care Map.
        state_columns_to_drop = [
            CommonFields.ICU_BEDS,
            CommonFields.HOSPITAL_BEDS_IN_USE_ANY,
            CommonFields.STAFFED_BEDS,
            CommonFields.CURRENT_ICU_TOTAL,
        ]
        states = states.drop(state_columns_to_drop, axis="columns")

        df = pd.concat([states, counties])

        df = common_df.sort_common_field_columns(df)

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

        # Removing a string of misleading FL current_icu values.
        is_incorrect_fl_icu_dates = df[CommonFields.DATE].between("2020-05-14", "2020-05-20")
        is_fl_state = df[CommonFields.FIPS] == "12"
        df.loc[is_fl_state & is_incorrect_fl_icu_dates, CommonFields.CURRENT_ICU] = None

        df = df.set_index(COMMON_FIELDS_TIMESERIES_KEYS, verify_integrity=True)
        return df


if __name__ == "__main__":
    common_init.configure_logging()
    log = structlog.get_logger()
    transformer = CovidCountyDataTransformer.make_with_data_root(
        DATA_ROOT, os.environ.get("CMDC_API_KEY", None), log,
    )
    common_df.write_csv(
        common_df.only_common_columns(transformer.transform(), log),
        DATA_ROOT / "cases-covid-county-data" / "timeseries-common.csv",
        log,
    )
