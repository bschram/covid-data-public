import logging
import datetime
import pathlib
import shutil
import tempfile
import requests

from dataset_updater_base import DatasetUpdaterBase

DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"
_logger = logging.getLogger(__name__)


class NYTimesCasesUpdater(DatasetUpdaterBase):
    """Updates NYTimes data set"""

    CSV_FILENAME = "us-counties.csv"
    VERSION_FILENAME = "version.txt"
    _NYTIMES_MASTER_URL = "https://github.com/nytimes/covid-19-data/archive/master.zip"
    _NYTIMES_MASTER_API_URL = "https://api.github.com/repos/nytimes/covid-19-data/branches/master"
    DATA_URL = "https://github.com/nytimes/covid-19-data/raw/master/{CSV_FILENAME}"

    NYTIMES_DATA_ROOT = DATA_ROOT / "cases-nytimes"

    # This is the only one being accessed currently, also available are us-states.csv and us.csv
    _DATA_TARGET_FILES = [
        "us-counties.csv"
    ]

    def get_master_commit_sha(self):
        r = requests.get(self._NYTIMES_MASTER_API_URL)
        return r.json()['commit']['sha']

    def write_version_file(self):
        sha = self.get_master_commit_sha()
        stamp = self._stamp()
        version_path = self.NYTIMES_DATA_ROOT / "version.txt"
        with version_path.open("w+") as vf:
            vf.write(f"{sha}\n")
            vf.write(f"Updated on {stamp}")
       
    def update(self) -> None:
        _logger.info("Updating NYTimes dataset.")
        tmp_dir = tempfile.TemporaryDirectory()
        p = pathlib.Path(tmp_dir.name)

        _logger.info("Copying data files")        
        self.clone_repo_to_dir(self._NYTIMES_MASTER_URL, str(p))
        repo_dir = p / "covid-19-data-master"
        for f in self._DATA_TARGET_FILES:
            _logger.infog("Copying file %s", f)
            shutil.copy(str(repo_dir / f), str(self.NYTIMES_DATA_ROOT / f))

        _logger.info("Updating version file.")
        self.write_version_file()
        _logger.info("Done, success!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    NYTimesCasesUpdater().update()
