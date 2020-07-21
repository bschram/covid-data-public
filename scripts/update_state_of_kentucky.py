"""
DISABLED 2020-07-21
This script returned an error once and the data hasn't been updated in months. There
is no sense risking another error of fixing the problem when it isn't producing useful
data.

See https://covidactnow.slack.com/archives/C011Z21ST8V/p1595333904000500
"""


import csv
import logging
import pathlib
from itertools import chain

import requests
from pydantic import BaseModel, HttpUrl
from datetime import date
import pandas as pd

SOURCE_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQQCt08RDxh3ROrnUPFv6aWPkZSmYkiLE88W49yOMdHKemAAhueAZxe3LKMz1Sob6V0OWZx4PHx0ed8/pub?gid=383671270&single=true&output=csv"
DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"

# Map from field name in
# https://github.com/covid-projections/covid-data-model/blob/master/libs/datasets/common_fields.py
# to static value that appears in each row of the output CSV.
STATIC_COLUMN_VALUES = {
    "country": "USA",
    "state": "KY",
    "aggregate_level": "state",
    "fips": "21",
}

# Map from name in the source to field name in
# https://github.com/covid-projections/covid-data-model/blob/master/libs/datasets/common_fields.py
IMPORTED_DATA_COLUMNS = {
    "Date": "date",
    "Total number of ICU units": "current_icu_total",
    # ventilator_capacity is not currently in common_fields.py but is found elsewhere in the model repo.
    "Total number of ventilators": "ventilator_capacity",
    "Number of ICU units currently in use by COVID patients": "current_icu",
    "Number of ventilators in use by COVID patients": "current_ventilated",
}


class UpdateStateOfKentucky(BaseModel):
    """Copies a CSV timeseries from Google Spreadsheets to the repo."""

    # URL for a CSV that contains timeseries values numbers.
    source_url: HttpUrl
    # Path of the output CSV
    output_path: pathlib.Path

    def yield_dict_per_state_date(self):
        """Yield all rows in the source_url."""

        response = requests.get(self.source_url)

        for source_row in csv.DictReader(response.iter_lines(decode_unicode=True)):
            this_row = {
                output_field: source_row[source_field]
                for source_field, output_field in IMPORTED_DATA_COLUMNS.items()
            }
            this_row.update(STATIC_COLUMN_VALUES)
            yield this_row

    def update(self):
        result = pd.DataFrame.from_records(
            self.yield_dict_per_state_date(),
            columns=chain(STATIC_COLUMN_VALUES.keys(), IMPORTED_DATA_COLUMNS.values()),
        )
        result.to_csv(self.output_path, index=False, date_format="%Y-%m-%d")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    UpdateStateOfKentucky(
        source_url=SOURCE_URL, output_path=DATA_ROOT / "states" / "ky.csv",
    ).update()
