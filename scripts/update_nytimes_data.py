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

    def _get_repo_archive_url(self, git_sha: str) -> str:
        return f"https://github.com/nytimes/covid-19-data/archive/{git_sha}.zip"
        
    def get_master_commit_sha(self) -> str:
        r = requests.get(self._NYTIMES_MASTER_API_URL)
        return r.json()['commit']['sha']

    def write_version_file(self, git_sha) -> None:
        stamp = self._stamp()
        version_path = self.NYTIMES_DATA_ROOT / "version.txt"
        with version_path.open("w+") as vf:
            vf.write(f"{git_sha}\n")
            vf.write(f"Updated on {stamp}")
       
    def update(self) -> None:
        _logger.info("Updating NYTimes dataset.")
        git_sha = self.get_master_commit_sha()
        _logger.info("Updating version file with nytimes revision %s.", git_sha)
        self.write_version_file(git_sha)
        repo_url = self._get_repo_archive_url(git_sha)

        with tempfile.TemporaryDirectory() as tmp_dir:
            p = pathlib.Path(tmp_dir)
            _logger.info("Copying data files")      
            self.clone_repo_to_dir(repo_url, str(p))
            repo_dir = p / f"covid-19-data-{git_sha}"
            for f in self._DATA_TARGET_FILES:
                _logger.info("Copying file %s", f)
                shutil.copy(str(repo_dir / f), str(self.NYTIMES_DATA_ROOT / f))


        _logger.info("Done, success!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    NYTimesCasesUpdater().update()
