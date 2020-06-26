import csv
import logging
import pathlib
from itertools import dropwhile
from os import PathLike
from typing import Union, Mapping

import requests
import structlog
from pydantic import BaseModel
from dateutil.parser import parse
import pandas as pd
import re

from structlog._config import BoundLoggerLazyProxy

from covidactnow.datapublic import common_df, common_init
from covidactnow.datapublic.common_fields import CommonFields
from scripts.update_covid_data_scraper import load_county_fips_data

SOURCE_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTzkytQW_yyyjLU_cKZYYf8ARa9nngLp9VWSUOpiXNha7rTOrdJxYW7Ryurfzjw-e05KkJv8inMe5S-/pub?gid=0&single=true&output=csv"
DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"
_logger = logging.getLogger(__name__)

COUNTY_SUFFIX = " County"


class CsvCopy(BaseModel):
    """Reads a CSV from Google Spreadsheets, patches the date format and writes it the local disk."""

    source_url: str

    # FIPS for each county, by name
    county_fips_csv: pathlib.Path

    log: Union[structlog.BoundLoggerBase, BoundLoggerLazyProxy]

    class Config:
        arbitrary_types_allowed = True  # For PathLike

    @staticmethod
    def make_with_data_root(data_root: pathlib.Path) -> "CsvCopy":
        return CsvCopy(
            source_url=SOURCE_URL,
            county_fips_csv=data_root / "misc" / "fips_population.csv",
            log=structlog.get_logger(),
        )

    def _yield_lines(self):
        self.log.info("Fetching URL", url=self.source_url)
        response = requests.get(self.source_url)

        for i, line in dropwhile(
            lambda i_line: "date" not in i_line[1].lower(),
            enumerate(response.iter_lines(decode_unicode=True)),
        ):
            if not line:
                self.log.warning("Skipping empty line", line_num=i)
                continue
            yield line

    def _yield_rows(self, county_to_fips: Mapping[str, str]):
        for row in csv.DictReader(self._yield_lines()):
            raw_date = row["date"]
            if not raw_date:
                self.log.warning(f"Skipping row without date")
                continue

            date_match = re.fullmatch(r"(\d+)/(\d+)", raw_date)

            if not (4 <= int(date_match.group(1)) <= 12):
                # Quick fix is changing sheet date format to YYYY-MM-DD
                self.log.error("Unexpected month. Is it already January?!", raw_date=raw_date)
                continue
            row["date"] = parse(raw_date).date().isoformat()

            for drop_attribute in ("fips_code", "state_code"):
                if drop_attribute in row:
                    del row[drop_attribute]

            raw_county_name = row.pop("county_name")
            if raw_county_name in county_to_fips:
                county_name = raw_county_name
            elif raw_county_name + COUNTY_SUFFIX in county_to_fips:
                county_name = raw_county_name + COUNTY_SUFFIX
            else:
                self.log.error(
                    "Imported county name not found in FIPS data", raw_county_name=raw_county_name
                )
                continue

            for key, value in list(
                row.items()
            ):  # Copy items to avoid modifying row while iterating
                if key in {CommonFields.FIPS, CommonFields.DATE}:
                    continue
                try:
                    float(value)
                except ValueError:
                    self.log.error(
                        "Dropping value not a float",
                        raw_county_name=raw_county_name,
                        raw_date=raw_date,
                        variable=key,
                        value=value,
                    )
                    del row[key]

            row[CommonFields.COUNTY] = county_name
            row[CommonFields.FIPS] = county_to_fips[county_name]
            yield row

    def transform(self):
        county_to_fips = (
            load_county_fips_data(self.county_fips_csv)
            .loc[lambda x: x[CommonFields.STATE] == "NV"]
            .set_index([CommonFields.COUNTY])
            .loc[:, CommonFields.FIPS]
            .to_dict()
        )

        df = pd.DataFrame.from_records(self._yield_rows(county_to_fips))
        return df


if __name__ == "__main__":
    common_init.configure_logging()

    log = structlog.get_logger()
    common_df.write_csv(
        CsvCopy.make_with_data_root(DATA_ROOT).transform(),
        DATA_ROOT / "states" / "nv" / "nha_hospitalization_county.csv",
        log,
    )
