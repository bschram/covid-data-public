import csv
import logging
import pathlib
import requests
from pydantic import BaseModel, HttpUrl, DirectoryPath, FilePath
from datetime import date
import pandas as pd
import re

# Cam gave Tom this URL in a DM in https://testandtrace.slack.com/
# The sheet name is "Data for CovidActNow"; I'm concerned that it isn't updated as part of their regular data push.
SOURCE_URL = "https://docs.google.com/spreadsheets/d/11_o7IH6puGS7ftgq0m3-ATCvZylKepHV4hZX_axjBCg/export?format=csv"
DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"

# Map from name is it appears in TestAndTrace data to name in state.txt
STATE_NAME_REMAP = {
    "Virgin Islands": "U.S. Virgin Islands",
    "Northern Mariana": "Northern Mariana Islands",
}


class TestAndTraceSyncer(BaseModel):
    """Copies a CSV from Google Spreadsheets to the repo, then merges all the CSVs into a single timeseries output."""

    # URL for a CSV that contains TestAndTrace numbers. It is not a timeseries; the counts for each state are modified
    # when the group finds new information.
    source_url: HttpUrl
    # Path of a text file of state names, copied from census.gov
    census_state_path: FilePath
    # Directory in which local copies of source_url are placed with file names YYYY-MM-DD.csv
    gsheets_copy_directory: DirectoryPath
    # Path of a CSV generated from files in local_copy_directory.
    state_timeseries_path: pathlib.Path
    # Today's date, used when copying the current source_url contents.
    date_today: date

    def yield_dict_per_state_date(self):
        """Yield all rows in all the dated local CSV files that have a # of Contact Tracers."""

        # By default pandas will parse the numeric values in the STATE column as ints but FIPS are two character codes.
        state_df = pd.read_csv(
            self.census_state_path, delimiter="|", dtype={"STATE": str}
        )
        state_df.rename(
            columns={"STUSAB": "state", "STATE": "fips", "STATE_NAME": "state_name"},
            inplace=True,
        )
        state_df.set_index("state_name", inplace=True)

        for file_path in self.gsheets_copy_directory.iterdir():
            date_from_filename = re.fullmatch(
                r"(\d{4}-\d{2}-\d{2})\.csv", file_path.name
            ).group(1)
            for row in csv.DictReader(open(file_path, newline="")):
                contact_tracers_count = row.get("# of Contact Tracers", "")
                if contact_tracers_count is "":
                    continue
                state_name = STATE_NAME_REMAP.get(row["State"], row["State"])
                state_row = state_df.loc[state_name]
                yield dict(
                    fips=state_row.fips,
                    state=state_row.state,
                    date=date_from_filename,
                    contact_tracers_count=contact_tracers_count,
                )

    def update(self):
        todays_filename = self.date_today.isoformat() + ".csv"
        todays_file_path = self.gsheets_copy_directory / todays_filename
        todays_file_path.write_bytes(requests.get(self.source_url).content)

        result = pd.DataFrame.from_records(
            self.yield_dict_per_state_date(),
            columns=["fips", "state", "date", "contact_tracers_count"],
        )
        result.to_csv(self.state_timeseries_path, index=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    TestAndTraceSyncer(
        source_url=SOURCE_URL,
        census_state_path=DATA_ROOT / "misc" / "state.txt",
        gsheets_copy_directory=DATA_ROOT / "test-and-trace" / "gsheet-copy",
        state_timeseries_path=DATA_ROOT / "test-and-trace" / "state_data.csv",
        date_today=date.today(),
    ).update()
