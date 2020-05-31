from enum import Enum
from typing import Union, Optional
import pandas as pd
import numpy
import structlog
from covidactnow.datapublic import common_init
from pydantic import BaseModel
import pathlib
from covidactnow.datapublic.common_df import write_df_as_csv, strip_whitespace
from covidactnow.datapublic.common_fields import CommonFields
from scripts.update_test_and_trace import load_census_state
from structlog._config import BoundLoggerLazyProxy


DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"


class FieldNameAndCommonField(str):
    """Represents the original field/column name and CommonField it maps to or None if dropped."""

    def __new__(cls, field_name: str, common_field: Optional[CommonFields]):
        o = super(FieldNameAndCommonField, cls).__new__(cls, field_name)
        o.common_field = common_field
        return o


class Fields(FieldNameAndCommonField, Enum):
    CITY = "city", CommonFields.CASES
    COUNTY = "county", CommonFields.COUNTY
    STATE = "state", CommonFields.STATE
    COUNTRY = "country", CommonFields.COUNTRY
    POPULATION = "population", CommonFields.POPULATION
    LATITUDE = "lat", None
    LONGITUDE = "long", None
    URL = "url", None
    CASES = "cases", CommonFields.POSITIVE_TESTS
    DEATHS = "deaths", CommonFields.DEATHS
    RECOVERED = "recovered", None
    ACTIVE = "active", None
    TESTED = "tested", None
    GROWTH_FACTOR = "growthFactor", None
    DATE = "date", CommonFields.DATE
    AGGREGATE_LEVEL = "aggregate_level", None
    NEGATIVE_TESTS = "negative_tests", CommonFields.NEGATIVE_TESTS
    HOSPITALIZED = "hospitalized", CommonFields.CUMULATIVE_HOSPITALIZED
    ICU = "icu", CommonFields.CUMULATIVE_ICU


def load_county_fips_data(fips_csv: pathlib.Path) -> pd.DataFrame:
    df = pd.read_csv(fips_csv, dtype={"fips": str})
    df["fips"] = df.fips.str.zfill(5)
    return df


def fill_missing_county_with_city(row):
    """Fills in missing county data with city if available.
    """
    if pd.isnull(row.county) and not pd.isnull(row.city):
        if row.city == "New York City":
            return "New York"
        return row.city

    return row.county


class TransformCovidDataScraper(BaseModel):
    """Copies a CSV timeseries from Google Spreadsheets to the repo."""

    cds_source_path: pathlib.Path

    # Path of a text file of state names, copied from census.gov
    census_state_path: pathlib.Path

    county_fips_csv: pathlib.Path

    log: Union[structlog.BoundLoggerBase, BoundLoggerLazyProxy]

    class Config:
        arbitrary_types_allowed = True

    @staticmethod
    def make_with_data_root(data_root: pathlib.Path) -> "TransformCovidDataScraper":
        return TransformCovidDataScraper(
            cds_source_path=data_root / "cases-cds" / "timeseries.csv",
            census_state_path=data_root / "misc" / "state.txt",
            county_fips_csv=data_root / "misc" / "fips_population.csv",
            log=structlog.get_logger(),
        )

    def transform(self) -> pd.DataFrame:
        df = pd.read_csv(self.cds_source_path, parse_dates=[Fields.DATE], low_memory=False)
        df = strip_whitespace(df)

        data = remove_duplicate_city_data(df)

        state_df = load_census_state(self.census_state_path)
        state_df.set_index("state_name", inplace=True)
        US_STATE_ABBREV = state_df.loc[:, "state"].to_dict()

        # CDS state level aggregates are identifiable by not having a city or county.
        only_county = data["county"].notnull() & data["state"].notnull()
        county_hits = numpy.where(only_county, "county", None)
        only_state = (
            data[Fields.COUNTY].isnull() & data[Fields.CITY].isnull() & data[Fields.STATE].notnull()
        )
        only_country = (
            data[Fields.COUNTY].isnull()
            & data[Fields.CITY].isnull()
            & data[Fields.STATE].isnull()
            & data[Fields.COUNTRY].notnull()
        )

        state_hits = numpy.where(only_state, "state", None)
        county_hits[state_hits != None] = state_hits[state_hits != None]
        county_hits[only_country] = "country"
        data[Fields.AGGREGATE_LEVEL] = county_hits

        # Backfilling FIPS data based on county names.
        # The following abbrev mapping only makes sense for the US
        # TODO: Fix all missing cases
        data = data[data["country"] == "United States"]
        data[CommonFields.COUNTRY] = "USA"
        data[CommonFields.STATE] = data[Fields.STATE].apply(
            lambda x: US_STATE_ABBREV[x] if x in US_STATE_ABBREV else x
        )

        fips_data = load_county_fips_data(self.county_fips_csv)
        fips_data.set_index(["state", "county"], inplace=True)
        data = data.merge(
            fips_data[["fips"]],
            left_on=["state", "county"],
            suffixes=(False, False),
            how="left",
            right_index=True,
        )
        no_fips = data[CommonFields.FIPS].isna()
        if no_fips.sum() > 0:
            self.log.error(
                "Removing rows without fips id",
                no_fips=data.loc[no_fips, ["state", "county"]].to_dict(orient="records"),
            )
            data = data.loc[~no_fips]

        data.set_index(["date", "fips"], inplace=True)
        if data.index.has_duplicates:
            # Use keep=False when logging so the output contains all duplicated rows, not just the first or last
            # instance of each duplicate.
            self.log.error("Removing duplicates", duplicated=data.index.duplicated(keep=False))
            data = data.loc[~data.index.duplicated(keep=False)]
        data.reset_index(inplace=True)

        # ADD Negative tests
        data[Fields.NEGATIVE_TESTS] = data[Fields.TESTED] - data[Fields.CASES]

        data = data.rename(columns={f: f.common_field for f in Fields})
        unexpected_columns = set(data.columns) - set(CommonFields)
        if unexpected_columns:
            self.log.warning("Removing columns not in CommonFields", columns=unexpected_columns)
            data = data.drop(columns=list(unexpected_columns))
        data = data.reindex(
            columns=[CommonFields.FIPS]
            + [f.common_field for f in Fields if f.common_field in data.columns]
        )

        return data


def remove_duplicate_city_data(data):
    # City data before 3-23 was not duplicated, copy the city name to the county field.
    select_pre_march_23 = data.date < "2020-03-23"
    data.loc[select_pre_march_23, "county"] = data.loc[select_pre_march_23].apply(
        fill_missing_county_with_city, axis=1
    )
    # Don't want to return city data because it's duplicated in county
    return data.loc[select_pre_march_23 | ((~select_pre_march_23) & data["city"].isnull())].copy()


if __name__ == "__main__":
    common_init.configure_structlog()
    write_df_as_csv(
        TransformCovidDataScraper.make_with_data_root(DATA_ROOT).transform(),
        DATA_ROOT / "cases-cds" / "timeseries-common.csv",
        structlog.get_logger(),
    )
