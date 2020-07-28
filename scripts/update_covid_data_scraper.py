from enum import Enum
from typing import Union

import click
import pandas as pd
import requests
import structlog
from covidactnow.datapublic import common_init, common_df
from pydantic import BaseModel
import pathlib
from covidactnow.datapublic.common_fields import (
    CommonFields,
    GetByValueMixin,
    COMMON_FIELDS_TIMESERIES_KEYS,
)
from scripts.update_helpers import FieldNameAndCommonField, rename_fields
from scripts.update_test_and_trace import load_census_state
from structlog._config import BoundLoggerLazyProxy


DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"


TIMESERIES_CSV_URL = r"https://coronadatascraper.com/timeseries.csv.zip"


class Fields(GetByValueMixin, FieldNameAndCommonField, Enum):
    LOCATION_ID = "locationID", None
    SLUG = "slug", None
    NAME = "name", None
    LEVEL = "level", CommonFields.AGGREGATE_LEVEL
    CITY = "city", None
    COUNTY = "county", CommonFields.COUNTY
    STATE = "state", None
    COUNTRY = "country", CommonFields.COUNTRY
    LATITUDE = "lat", None
    LONGITUDE = "long", None
    POPULATION = "population", CommonFields.POPULATION
    AGGREGATE = "aggregate", None
    TZ = "tz", None
    CASES = "cases", None  # Special handling below
    DEATHS = "deaths", CommonFields.DEATHS
    RECOVERED = "recovered", None
    ACTIVE = "active", None
    TESTED = "tested", None
    HOSPITALIZED = "hospitalized", CommonFields.CUMULATIVE_HOSPITALIZED
    HOSPITALIZED_CURRENT = "hospitalized_current", None
    DISCHARGED = "discharged", None
    ICU = "icu", CommonFields.CUMULATIVE_ICU
    ICU_CURRENT = "icu_current", None
    # Not in Project Li output: GROWTH_FACTOR = "growthFactor", None
    DATE = "date", CommonFields.DATE


class CovidDataScraperTransformer(BaseModel):
    """Transforms the raw CovidDataScraper timeseries on disk to a DataFrame using CAN CommonFields."""

    # Source for raw CovidDataScraper timeseries.
    timeseries_csv_local_path: pathlib.Path

    # Path of a text file of state names, copied from census.gov
    census_state_path: pathlib.Path

    log: Union[structlog.BoundLoggerBase, BoundLoggerLazyProxy]

    class Config:
        arbitrary_types_allowed = True

    @staticmethod
    def make_with_data_root(
        data_root: pathlib.Path, log: structlog.BoundLoggerBase
    ) -> "CovidDataScraperTransformer":
        return CovidDataScraperTransformer(
            timeseries_csv_local_path=data_root / "cases-cds" / "timeseries.csv.zip",
            census_state_path=data_root / "misc" / "state.txt",
            log=log,
        )

    def fetch(self):
        self.timeseries_csv_local_path.write_bytes(requests.get(TIMESERIES_CSV_URL).content)

    def transform(self) -> pd.DataFrame:
        """Read data from disk and return a DataFrame using CAN CommonFields."""
        df = pd.read_csv(
            self.timeseries_csv_local_path, parse_dates=[Fields.DATE], low_memory=False
        )

        df_us_mask = df[Fields.COUNTRY] == "United States"

        df_counties = df[df_us_mask & (df[Fields.LEVEL] == "county")].copy()
        # Using str.slice instead of str.extract is really ugly but is easier than fixing the DataFrame assignment
        # errors I had with str.extract.
        bad_location_id = ~df_counties[Fields.LOCATION_ID].str.match(
            r"\Aiso1:us#iso2:us-..#fips:\d{5}\Z"
        )
        if bad_location_id.any():
            self.log.warning(
                "Dropping county rows with unexpected locationID",
                bad_location_id=df_counties.loc[bad_location_id, Fields.LOCATION_ID],
            )
            df_counties = df_counties[~bad_location_id]
        df_counties.loc[:, CommonFields.FIPS] = df_counties[Fields.LOCATION_ID].str.slice(start=24)
        df_counties[CommonFields.STATE] = (
            df_counties[Fields.LOCATION_ID].str.slice(start=16, stop=18).str.upper()
        )

        df_states = df[df_us_mask & (df[Fields.LEVEL] == "state")].copy()
        bad_location_id = ~df_states[Fields.LOCATION_ID].str.match(r"\Aiso1:us#iso2:us-..\Z")
        if bad_location_id.any():
            self.log.warning(
                "Dropping state rows with unexpected locationID",
                bad_location_id=df_states.loc[bad_location_id, Fields.LOCATION_ID],
            )
            df_states = df_states[~bad_location_id]

        df_states[CommonFields.STATE] = (
            df_states[Fields.LOCATION_ID].str.slice(start=16, stop=18).str.upper()
        )
        states_by_abbrev = load_census_state(self.census_state_path).set_index("state")
        df_states = df_states.merge(
            states_by_abbrev["fips"],
            how="left",
            left_on=CommonFields.STATE,
            right_index=True,
            suffixes=(False, False),
            copy=False,
        )

        df = pd.concat([df_counties, df_states])

        no_fips = df[CommonFields.FIPS].isna()
        if no_fips.any():
            self.log.error(
                "Rows without fips", no_fips=df.loc[no_fips, Fields.LOCATION_ID].value_counts(),
            )
            df = df.loc[~no_fips, :]

        # Use keep=False when logging so the output contains all duplicated rows, not just the first or last
        # instance of each duplicate.
        duplicates_mask = df.duplicated(COMMON_FIELDS_TIMESERIES_KEYS, keep=False)
        if duplicates_mask.any():
            self.log.error(
                "Removing duplicate timeseries points",
                duplicates=df.loc[
                    duplicates_mask, [Fields.LOCATION_ID, CommonFields.FIPS, Fields.DATE]
                ],
            )
            df = df.loc[~duplicates_mask, :]

        df[CommonFields.CASES] = pd.to_numeric(df[Fields.CASES])
        df[CommonFields.NEGATIVE_TESTS] = pd.to_numeric(df[Fields.TESTED]) - df[CommonFields.CASES]

        # Already transformed from Fields to CommonFields
        already_transformed_fields = {
            CommonFields.FIPS,
            CommonFields.STATE,
            CommonFields.CASES,
            CommonFields.NEGATIVE_TESTS,
        }

        df = rename_fields(df, Fields, already_transformed_fields, self.log)

        df[CommonFields.COUNTRY] = "USA"
        return df


@click.command()
@click.option("--fetch/--no-fetch", default=True)
def main(fetch: bool):
    common_init.configure_logging()
    log = structlog.get_logger(updater="CovidDataScraperTransformer")
    local_path = DATA_ROOT / "cases-cds" / "timeseries-common.csv"

    transformer = CovidDataScraperTransformer.make_with_data_root(DATA_ROOT, log)
    if fetch:
        transformer.fetch()
    common_df.write_csv(transformer.transform(), local_path, log)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
