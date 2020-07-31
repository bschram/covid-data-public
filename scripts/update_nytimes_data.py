from typing import Dict, Tuple, List
import enum
import logging
import datetime
import shutil
import tempfile


import pathlib
import requests
import pandas as pd
import pydantic
import structlog
import click
from covidactnow.datapublic import common_init
from covidactnow.datapublic import common_df
from covidactnow.datapublic.common_fields import (
    GetByValueMixin,
    CommonFields,
    COMMON_FIELDS_TIMESERIES_KEYS,
    FieldNameAndCommonField,
)
from scripts.update_covid_data_scraper import FieldNameAndCommonField
from scripts import helpers

DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"
_logger = structlog.get_logger(__name__)


BACKFILLED_CASES = [
    # On 2020-07-24, CT reported a backfill of 440 additional positive cases.
    # https://portal.ct.gov/Office-of-the-Governor/News/Press-Releases/2020/07-2020/Governor-Lamont-Coronavirus-Update-July-24
    ("09", "2020-07-24", 440),
    # https://portal.ct.gov/Office-of-the-Governor/News/Press-Releases/2020/07-2020/Governor-Lamont-Coronavirus-Update-July-29
    ("09", "2020-07-29", 384),
]


def _calculate_county_adjustments(
    data: pd.DataFrame, date: str, backfilled_cases: int, state_fips: str
) -> Dict[str, int]:
    """Calculating number of cases to remove per county, weighted on number of new cases per county.

    Weighting on number of new cases per county gives a reasonable measure of where the backfilled
    cases ended up.

    Args:
        data: Input Data.
        date: Date of backfill.
        backfilled_cases: Number of backfilled cases.
        state_fips: FIPS code for state.

    Returns: Dictionary of estimated fips -> backfilled cases.
    """
    is_state = data[CommonFields.FIPS].str.match(f"{state_fips}[0-9][0-9][0-9]")
    is_not_unknown = data[CommonFields.FIPS] != f"{state_fips}999"
    if not (is_not_unknown & is_state).any():
        return {}

    fields = [CommonFields.DATE, CommonFields.FIPS, CommonFields.CASES]
    cases = (
        data.loc[is_state & is_not_unknown, fields]
        .set_index([CommonFields.FIPS, CommonFields.DATE])
        .sort_index()
    )
    cases = cases.diff().reset_index(level=1)
    cases_on_date = cases[cases.date == date]["cases"]
    # For states with more counties, rounding could lead to the sum of the counties diverging from
    # the backfilled cases count.
    return (cases_on_date / cases_on_date.sum() * backfilled_cases).round().to_dict()


def remove_backfilled_cases(
    data: pd.DataFrame, backfilled_cases: List[Tuple[str, str, int]]
) -> pd.DataFrame:
    """Removes reported backfilled cases from case totals.

    Args:
        data: Data
        backfilled_cases: List of backfilled case info.

    Returns: Updated data frame.
    """
    for state_fips, date, cases in backfilled_cases:
        adjustments = _calculate_county_adjustments(data, date, cases, state_fips)
        is_on_or_after_date = data[CommonFields.DATE] >= date
        for fips, count in adjustments.items():
            is_fips_data_after_date = is_on_or_after_date & (data[CommonFields.FIPS] == fips)
            data.loc[is_fips_data_after_date, CommonFields.CASES] -= int(count)

    return data


class Fields(GetByValueMixin, FieldNameAndCommonField, enum.Enum):
    DATE = "date", CommonFields.DATE
    COUNTY = "county", CommonFields.COUNTY
    STATE_FULL_NAME = "state", CommonFields.STATE_FULL_NAME
    FIPS = "fips", CommonFields.FIPS
    CASES = "cases", CommonFields.CASES
    DEATHS = "deaths", CommonFields.DEATHS


class NYTimesUpdater(pydantic.BaseModel):
    """Updates NYTimes data set"""

    COUNTY_CSV_FILENAME = "us-counties.csv"
    STATE_CSV_FILENAME = "us-states.csv"
    VERSION_FILENAME = "version.txt"

    NYTIMES_MASTER_API_URL = "https://api.github.com/repos/nytimes/covid-19-data/branches/master"
    NYTIMES_RAW_BASE_URL = "https://raw.githubusercontent.com/nytimes/covid-19-data/master"

    raw_data_root: pathlib.Path

    timeseries_output_path: pathlib.Path

    state_census_path: pathlib.Path

    class Config:
        arbitrary_types_allowed = True

    @classmethod
    def make_with_data_root(cls, data_root: pathlib.Path) -> "NYTimesUpdater":
        return cls(
            raw_data_root=data_root / "cases-nytimes",
            timeseries_output_path=data_root / "cases-nytimes" / "timeseries-common.csv",
            state_census_path=data_root / "misc" / "state.txt",
        )

    # This is the only one being accessed currently, also available are us-states.csv and us.csv
    _DATA_TARGET_FILES = [COUNTY_CSV_FILENAME, STATE_CSV_FILENAME]

    def _get_repo_archive_url(self, git_sha: str) -> str:
        return f"https://github.com/nytimes/covid-19-data/archive/{git_sha}.zip"

    @property
    def county_url(self):
        return f"{self.NYTIMES_RAW_BASE_URL}/{self.COUNTY_CSV_FILENAME}"

    @property
    def state_url(self):
        return f"{self.NYTIMES_RAW_BASE_URL}/{self.STATE_CSV_FILENAME}"

    @property
    def county_path(self) -> pathlib.Path:
        return self.raw_data_root / self.COUNTY_CSV_FILENAME

    @property
    def state_path(self) -> pathlib.Path:
        return self.raw_data_root / self.STATE_CSV_FILENAME

    def get_master_commit_sha(self) -> str:
        r = requests.get(self.NYTIMES_MASTER_API_URL)
        return r.json()["commit"]["sha"]

    def write_version_file(self, git_sha) -> None:
        stamp = datetime.datetime.utcnow().isoformat()
        version_path = self.raw_data_root / "version.txt"
        with version_path.open("w+") as vf:
            vf.write(f"{git_sha}\n")
            vf.write(f"Updated on {stamp}")

    def update_source_data(self):
        git_sha = self.get_master_commit_sha()
        _logger.info(f"Updating version file with nytimes revision {git_sha}")
        state_data = requests.get(self.state_url).content
        self.state_path.write_bytes(state_data)

        county_data = requests.get(self.county_url).content
        self.county_path.write_bytes(county_data)
        self.write_version_file(git_sha)

    def load_state_and_county_data(self) -> pd.DataFrame:
        """Loads state and county data in one dataset, renaming fields to common field names. """
        _logger.info("Updating NYTimes dataset.")
        # Able to use common_df here because the NYTimes raw files include fips and date.
        county_data = common_df.read_csv(self.county_path).reset_index()
        county_data = helpers.rename_fields(county_data, Fields, set(), _logger)
        county_data[CommonFields.AGGREGATE_LEVEL] = "county"

        # Able to use common_df here because the NYTimes raw files include fips and date.
        state_data = common_df.read_csv(self.state_path).reset_index()
        state_data = helpers.rename_fields(state_data, Fields, set(), _logger)
        state_data[CommonFields.AGGREGATE_LEVEL] = "state"

        return pd.concat([county_data, state_data])

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        census_data = helpers.load_census_state(self.state_census_path).set_index("state_name")

        # Renaming Virgin Islands to match full name from census_data
        data[CommonFields.STATE_FULL_NAME] = data[CommonFields.STATE_FULL_NAME].replace(
            "Virgin Islands", "U.S. Virgin Islands"
        )
        data[CommonFields.STATE] = data[CommonFields.STATE_FULL_NAME].map(census_data["state"])
        data[CommonFields.COUNTRY] = "USA"

        # Rename new york city to new york county and assign it to New York County FIPS
        data.loc[
            data[CommonFields.COUNTY] == "New York City", CommonFields.COUNTY
        ] = "New York County"
        data.loc[data[CommonFields.COUNTY] == "New York County", CommonFields.FIPS] = "36061"

        data = remove_backfilled_cases(data, BACKFILLED_CASES)

        no_fips = data[CommonFields.FIPS].isna()
        if no_fips.any():
            _logger.error(
                "Rows without fips", no_fips=data.loc[no_fips, CommonFields.COUNTY].value_counts(),
            )
            data = data.loc[~no_fips, :]

        return data


@click.command()
@click.option("--fetch/--no-fetch", default=True)
def main(fetch: bool):
    common_init.configure_logging()
    transformer = NYTimesUpdater.make_with_data_root(DATA_ROOT)
    if fetch:
        _logger.info("Fetching new data.")
        transformer.update_source_data()

    data = transformer.load_state_and_county_data()
    data = transformer.transform(data)
    common_df.write_csv(data, transformer.timeseries_output_path, _logger)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
