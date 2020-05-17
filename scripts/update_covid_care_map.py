import logging
import datetime
import pathlib
import pytz
import requests

DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"
_logger = logging.getLogger(__name__)


class CovidCareMapUpdater(object):
    """Updates the covid care map data."""

    COUNTY_DATA_URL = (
        "https://raw.githubusercontent.com/covidcaremap/covid19-healthsystemcapacity/"
        "master/data/published/us_healthcare_capacity-county-CovidCareMap.csv"
    )
    STATE_DATA_URL = (
        "https://raw.githubusercontent.com/covidcaremap/covid19-healthsystemcapacity/"
        "master/data/published/us_healthcare_capacity-state-CovidCareMap.csv"
    )
    COVID_CARE_MAP_ROOT = DATA_ROOT / "covid-care-map"

    @property
    def output_path(self) -> pathlib.Path:
        return self.COVID_CARE_MAP_ROOT / "healthcare_capacity_data_county.csv"

    @property
    def state_output_path(self) -> pathlib.Path:
        return self.COVID_CARE_MAP_ROOT / "healthcare_capacity_data_state.csv"

    @property
    def version_path(self) -> pathlib.Path:
        return self.COVID_CARE_MAP_ROOT / "version.txt"

    @staticmethod
    def _stamp():
        pacific = pytz.timezone("UTC")
        d = datetime.datetime.now(pacific)
        return d.strftime("%A %b %d %I:%M:%S %p %Z")

    def update(self):
        _logger.info("Updating Covid Care Map data.")
        response = requests.get(self.COUNTY_DATA_URL)
        self.output_path.write_bytes(response.content)
        response = requests.get(self.STATE_DATA_URL)
        self.state_output_path.write_bytes(response.content)

        version_path = self.version_path
        version_path.write_text(f"Updated at {self._stamp()}\n")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    CovidCareMapUpdater().update()
