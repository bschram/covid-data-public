"""
Updates all relevant files in the repo.
"""

import logging
import os
import requests
import shutil
import tempfile

import pandas as pd
import argparse

from covidactnow.datapublic.common_init import configure_logging
from scripts.dataset_updater_base import DatasetUpdaterBase

logger = logging.Logger("data update logger")

parser = argparse.ArgumentParser()
parser.add_argument(
    "-cds", "--cds", help="Update data from the Corona Data Scraper", action="store_true"
)
parser.add_argument(
    "-jhu", "--jhu", help="Update data from John Hopkins University", action="store_true"
)
args = parser.parse_args()


class CovidDatasetAutoUpdater(DatasetUpdaterBase):
    """Provides all functionality to auto-update the datasets in the data repository"""

    _JHU_MASTER_API = r"https://api.github.com/repos/CSSEGISandData/COVID-19/branches/master"
    _DATA_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), "..", "data"))
    _JHU_DATA_DIR = os.path.join(_DATA_DIR, "cases-jhu")
    _JHU_DAILY_REPORTS_DIR = os.path.join(_JHU_DATA_DIR, "csse_covid_19_daily_reports")

    _CDS_TIMESERIES = r"https://coronadatascraper.com/timeseries.csv"
    _CDS_DATA_DIR = os.path.join(_DATA_DIR, "cases-cds")

    def _get_jhu_repo_url(self, git_sha: str) -> str:
        return f"https://github.com/CSSEGISandData/COVID-19/archive/{git_sha}.zip"

    def update_jhu_data(self):
        git_sha = requests.get(self._JHU_MASTER_API).json()["commit"]["sha"]
        with open(os.path.join(self._JHU_DATA_DIR, "version.txt"), "w") as vf:
            vf.write("{}\n".format(git_sha))
            vf.write("Updated on {}".format(self._stamp()))

        with tempfile.TemporaryDirectory() as new_temp_dir:
            self.clone_repo_to_dir(self._get_jhu_repo_url(git_sha), new_temp_dir)
            repo_dir = os.path.join(new_temp_dir, f"COVID-19-{git_sha}")
            jhu_repo_daily_reports_dir = os.path.join(
                repo_dir, "csse_covid_19_data", "csse_covid_19_daily_reports"
            )
            # Copy the daily reports into the local directory
            for f in os.listdir(jhu_repo_daily_reports_dir):
                shutil.copyfile(
                    os.path.join(jhu_repo_daily_reports_dir, f),
                    os.path.join(self._JHU_DAILY_REPORTS_DIR, f),
                )

    def update_cds_data(self):
        pd.read_csv(self._CDS_TIMESERIES).to_csv(
            os.path.join(self._CDS_DATA_DIR, "timeseries.csv"), index=False
        )
        # Record the date and time of update in versions.txt
        with open(os.path.join(self._CDS_DATA_DIR, "version.txt"), "w") as log:
            log.write("Updated on {}".format(self._stamp()))

    def update_all_data_files(self):
        self.update_cds_data()
        self.update_jhu_data()


if __name__ == "__main__":
    configure_logging()
    update = CovidDatasetAutoUpdater()
    something_specified = False

    if args.cds:
        logger.info("Updating data from the Corona Data Scraper")
        update.update_cds_data()
        something_specified = True

    if args.jhu:
        logger.info("Updating data from John Hopkins University")
        update.update_jhu_data()
        something_specified = True

    if not something_specified:
        #  If nothing was specified, then we assume that the user wants all datasets updated
        logger.info("Updating all data sources")
        update.update_all_data_files()
