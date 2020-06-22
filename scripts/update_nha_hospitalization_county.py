import csv
import logging
import pathlib
from itertools import dropwhile
from os import PathLike
import requests
from pydantic import BaseModel
from dateutil.parser import parse
import re

SOURCE_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTzkytQW_yyyjLU_cKZYYf8ARa9nngLp9VWSUOpiXNha7rTOrdJxYW7Ryurfzjw-e05KkJv8inMe5S-/pub?gid=0&single=true&output=csv"
DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"
_logger = logging.getLogger(__name__)


class CsvCopy(BaseModel):
    """Reads a CSV from Google Spreadsheets, patches the date format and writes it the local disk."""

    source_url: str
    destination_path: PathLike

    class Config:
        arbitrary_types_allowed = True  # For PathLike

    def update(self):
        _logger.info(f"Copying {self.source_url} to {self.destination_path}")
        response = requests.get(self.source_url)
        patched_rows = []

        for i, row in dropwhile(
            lambda i_row: i_row[1][0].lower() != "date",
            enumerate(csv.reader(response.iter_lines(decode_unicode=True))),
        ):
            if not row:
                _logger.warning(f"Skipping empty row {i}")
                continue
            if not row[0]:
                _logger.warning(f"Skipping row {i} without value in column 0")
                continue

            date_match = re.fullmatch(r"(\d+)/(\d+)", row[0])

            if date_match:
                if not (4 <= int(date_match.group(1)) <= 12):
                    raise ValueError(
                        f"Unexpected month in {row[0]}. Is it already January?!"
                        f"Quick fix is changing sheet date format to YYYY-MM-DD."
                    )
                row[0] = parse(row[0]).date().isoformat()

            patched_rows.append(row)

        with open(self.destination_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(patched_rows)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    CsvCopy(
        source_url=SOURCE_URL,
        destination_path=DATA_ROOT / "misc" / "nha_hospitalization_county.csv",
    ).update()
