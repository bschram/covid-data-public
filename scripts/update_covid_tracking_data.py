import logging
import datetime
import pathlib
import pytz
import requests
import pandas as pd
DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"
_logger = logging.getLogger(__name__)


class CovidTrackingDataUpdater(object):
    """Updates the covid tracking data."""

    HISTORICAL_STATE_DATA_URL = "http://covidtracking.com/api/states/daily"
    COVID_TRACKING_ROOT = DATA_ROOT / "covid-tracking"

    @property
    def output_path(self) -> pathlib.Path:
        return self.COVID_TRACKING_ROOT / "covid_tracking_states.csv"

    @property
    def version_path(self) -> pathlib.Path:
        return self.COVID_TRACKING_ROOT / "version.txt"

    @staticmethod
    def _stamp():
        #  String of the current date and time.
        #  So that we're consistent about how we mark these
        pacific = pytz.timezone('US/Pacific')
        d = datetime.datetime.now(pacific)
        return d.strftime('%A %b %d %I:%M:%S %p %Z')

    def update(self):
        _logger.info("Updating Covid Tracking data.")
        response = requests.get(self.HISTORICAL_STATE_DATA_URL)
        data = response.json()
        df = pd.DataFrame(data)
        df.to_csv(self.output_path, index=False)

        version_path = self.version_path
        version_path.write_text(f"Updated at {self._stamp()}\n")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    CovidTrackingDataUpdater().update()
