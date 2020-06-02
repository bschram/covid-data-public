from enum import Enum
from typing import Union, Optional, Mapping, MutableMapping
import pandas as pd
import numpy
import structlog
from covidactnow.datapublic import common_init
from pydantic import BaseModel
import pathlib
from covidactnow.datapublic.common_df import write_df_as_csv, strip_whitespace
from covidactnow.datapublic.common_fields import CommonFields, GetByValueMixin
from scripts.update_test_and_trace import load_census_state
from structlog._config import BoundLoggerLazyProxy


DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"


class FieldNameAndCommonField(str):
    """Represents the original field/column name and CommonField it maps to or None if dropped."""

    def __new__(cls, field_name: str, common_field: Optional[CommonFields]):
        o = super().__new__(cls, field_name)
        o.common_field = common_field
        return o


class Fields(GetByValueMixin, FieldNameAndCommonField, Enum):
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


class CovidDataScraperTransformer(BaseModel):
    """Transforms the raw CovidDataScraper timeseries on disk to a DataFrame using CAN CommonFields."""

    # Source for raw CovidDataScraper timeseries.
    cds_source_path: pathlib.Path

    # Path of a text file of state names, copied from census.gov
    census_state_path: pathlib.Path

    # FIPS for each county, by name
    county_fips_csv: pathlib.Path

    log: Union[structlog.BoundLoggerBase, BoundLoggerLazyProxy]

    class Config:
        arbitrary_types_allowed = True

    @staticmethod
    def make_with_data_root(data_root: pathlib.Path) -> "CovidDataScraperTransformer":
        return CovidDataScraperTransformer(
            cds_source_path=data_root / "cases-cds" / "timeseries.csv",
            census_state_path=data_root / "misc" / "state.txt",
            county_fips_csv=data_root / "misc" / "fips_population.csv",
            log=structlog.get_logger(),
        )

    def transform(self) -> pd.DataFrame:
        """Read data from disk and return a DataFrame using CAN CommonFields."""
        # TODO(tom): When I have more confidence in our ability to do large re-factors switch to the CDS json struct
        # as seen in https://github.com/covid-projections/covid-data-public/compare/update-cds-source
        df = pd.read_csv(self.cds_source_path, parse_dates=[Fields.DATE], low_memory=False)
        # Code from here down is copied almost verbatim from
        # https://github.com/covid-projections/covid-data-model/blob/d1a104f/libs/datasets/sources/cds_dataset.py#L103-L152
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

        # Rename and sort columns in data to match CommonFields. I'm not very happy with this code.
        # It'd be cleaner if any column not in Fields caused a failure, but that might be annoying
        # to maintain. columns that don't appear in the input file but are added to `data` in the
        # above code are another annoying corner case; do they belong in Fields?

        # Columns not in Fields or CommonFields will be logged
        col_not_in_fields_or_common = []
        # Map from name in the input/added so far -> name in the output
        rename: MutableMapping[str, str] = {}
        for col in data.columns:
            field = Fields.get(col)
            if field is not None:
                if field.common_field is not None:
                    rename[field.value] = field.common_field.value
            elif CommonFields.get(col) is not None:
                rename[col] = col
            else:
                col_not_in_fields_or_common.append(col)
        # Sort contents of `rename` to match the order of CommonFields.
        common_order = {common: i for i, common in enumerate(CommonFields)}
        names_in, names_out = zip(*sorted(rename.items(), key=lambda f_c: common_order[f_c[1]]))
        # Copy only columns in `rename.keys()` to a new DataFrame and rename.
        data = data.loc[:, list(names_in)].rename(columns=rename)
        if col_not_in_fields_or_common:
            self.log.warning(
                "Removing columns not in CommonFields", columns=col_not_in_fields_or_common
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
    transformer = CovidDataScraperTransformer.make_with_data_root(DATA_ROOT)
    write_df_as_csv(
        transformer.transform(),
        DATA_ROOT / "cases-cds" / "timeseries-common.csv",
        structlog.get_logger(),
    )
