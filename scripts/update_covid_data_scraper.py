import csv
import logging
import pathlib
from itertools import chain

import requests
from pydantic import BaseModel, HttpUrl
import pandas as pd


DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"


# Map from name in the source to field name in
# https://github.com/covid-projections/covid-data-model/blob/master/libs/datasets/common_fields.py
COUNTRY_REPLACEMENTS = {
    "United States": "USA",
}
TIMESERIES_URL = "https://coronadatascraper.com/timeseries.json"
LOCATIONS_URL = "https://coronadatascraper.com/locations.json"


def build_base_record(location):
    fips = None
    if "fips" in location.get("countyId", ""):
        fips = location["countyId"].replace("fips:", "")

    country = COUNTRY_REPLACEMENTS.get(location["country"], location["country"])
    base = {
        "state": location.get("stateId", '').replace('iso2:US-', ''),
        "country": country,
        "level": location["level"],
        "population": location.get("population"),
        "aggregate": location.get("aggregate"),
        "fips": fips,
    }

    if "coordinates" in location:
        base["latitude"] = location["coordinates"][0]
        base["longitude"] = location["coordinates"][1]
    return base


class UpdateCovidDataScraper(BaseModel):

    # URL for a CSV that contains timeseries values numbers.
    timeseries_url: HttpUrl
    # URL for a JSON that contains location metadata for entries in timeseries data.
    location_metadata_url: HttpUrl
    # Path of the output CSV
    output_path: pathlib.Path

    def yield_dict_per_row(self, metadata_by_location_id):
        """Yield all rows in the source_url."""

        timeseries = requests.get(self.timeseries_url).json()

        for date, data_by_location in timeseries.items():
            for location_id, values in data_by_location.items():
                metadata = metadata_by_location_id[location_id]
                record = dict(date=date, **values, **metadata,)
                yield record

    def update(self):
        locations = requests.get(self.location_metadata_url).json()
        metadata_by_location_id = {str(i): build_base_record(data) for i, data in enumerate(locations)}

        records = self.yield_dict_per_row(metadata_by_location_id)
        result = pd.DataFrame.from_records(records)
        result.to_csv(self.output_path, index=False, date_format="%Y-%m-%d")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    UpdateCovidDataScraper(
        timeseries_url=TIMESERIES_URL,
        location_metadata_url=LOCATIONS_URL,
        output_path=DATA_ROOT / "cases-cds" / "timeseries.csv",
    ).update()
