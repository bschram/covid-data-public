import csv
from datetime import datetime
from enum import Enum
from typing import Union, Optional, Mapping, MutableMapping
import pandas as pd
import numpy
import requests
import structlog
from covidactnow.datapublic import common_init
from pydantic import BaseModel, HttpUrl
import pathlib
from covidactnow.datapublic.common_df import write_df_as_csv, strip_whitespace
from covidactnow.datapublic.common_fields import CommonFields, GetByValueMixin
from scripts.update_covid_data_scraper import FieldNameAndCommonField, load_county_fips_data
from scripts.update_test_and_trace import load_census_state
from structlog._config import BoundLoggerLazyProxy


DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"


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


class TransformUsaFacts(BaseModel):
    source_url: HttpUrl

    source_path: pathlib.Path

    county_fips_csv: pathlib.Path

    log: Union[structlog.BoundLoggerBase, BoundLoggerLazyProxy]

    class Config:
        arbitrary_types_allowed = True

    @staticmethod
    def make_with_data_root(data_root: pathlib.Path) -> "TransformCovidDataScraper":
        return TransformUsaFacts(
            source_url="https://usafactsstatic.blob.core.windows.net/public/data/covid-19/covid_confirmed_usafacts.csv",
            source_path=data_root / "cases-usafacts" / "covid_confirmed_usafacts.csv",
            county_fips_csv=data_root / "misc" / "fips_population.csv",
            log=structlog.get_logger(),
        )

    def fetch(self):
        self.source_path.write_bytes(requests.get(self.source_url).content)

    def yield_rows(self):
        date_map = {}
        # Skip the BOM, thank you https://stackoverflow.com/a/59340346
        for row in csv.DictReader(open(self.source_path, newline="", encoding="utf-8-sig")):
            if not date_map:
                for key in row.keys():
                    try:
                        key_date = datetime.strptime(key, "%m/%d/%y")
                        date_map[key] = key_date.date().isoformat()
                    except ValueError:
                        pass
            for source_date, iso_date in date_map.items():
                yield {
                    CommonFields.FIPS: row["countyFIPS"],
                    CommonFields.COUNTY: row["County Name"],
                    CommonFields.STATE: row["State"],
                    CommonFields.CASES: int(row[source_date]),
                    CommonFields.DATE: iso_date
                }

    def transform(self) -> pd.DataFrame:
        df = pd.DataFrame(self.yield_rows())
        df[CommonFields.FIPS] = df[CommonFields.FIPS].str.zfill(5)

        fips_data = load_county_fips_data(self.county_fips_csv)
        fips_data.set_index(["state", "county"], inplace=True)

        df = df.merge(
            fips_data[["fips"]],
            left_on=["state", "county"],
            suffixes=("", "_r"),
            how="left",
            right_index=True,
        )

        fips_mismatch_mask = df["fips"] != df["fips_r"]
        fips_mismatch = df.loc[fips_mismatch_mask, :]
        fips_mismatch.set_index(["fips", "county"])
        grouped = fips_mismatch.groupby(by=["fips", "county"], sort=False)
        aggregated = grouped.agg({'date': 'max', 'cases': 'max'})
        aggregated['count_rows'] = grouped.size()
        aggregated = aggregated.sort_values('cases', ascending=False)

        df = df.loc[~fips_mismatch_mask, [CommonFields.FIPS, CommonFields.DATE, CommonFields.CASES]]
        return df


if __name__ == "__main__":
    common_init.configure_structlog()
    transform = TransformUsaFacts.make_with_data_root(DATA_ROOT)
    # transform.fetch()
    write_df_as_csv(transform.transform(), DATA_ROOT / "cases-usafacts" / "timeseries-common.csv", structlog.get_logger())

