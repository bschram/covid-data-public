import enum

import click
import pandas as pd
import numpy as np
import structlog
import pathlib
import pydantic
import datetime

import zoltpy.util
from covidactnow.datapublic import common_init

# from covidactnow.datapublic import common_df
from covidactnow.datapublic.common_fields import CommonFields

DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"

_logger = structlog.get_logger(__name__)


class ForecastModel(enum.Enum):
    """"""

    ENSEMBLE = "COVIDhub-ensemble"
    BASELINE = "COVIDhub-baseline"
    GOOGLE = "Google_Harvard-CPF"


class ForecastHubUpdater(pydantic.BaseModel):
    """Updates Forecast Lab Data Set

    Parameters
    ----------
    model: ForecastModel
        The Project and Model to Download from Zoltar
    forecast_date: Str
        The timezero_date (Zoltar key) when the forecast was submitted. In the form of
        "YYYY-MM-DD" and is usually on a Monday. As of 9 Sept 2020 Talking with the maintainers
        about having a "latest" endpoint.

    """

    RAW_CSV_FILENAME = "raw.csv"
    FORECAST_DATE = "2020-09-07"

    model: ForecastModel

    raw_data_root: pathlib.Path

    timeseries_output_path: pathlib.Path

    @classmethod
    def make_with_data_root(
        cls, model: ForecastModel, data_root: pathlib.Path
    ) -> "ForecastHubUpdater":
        return cls(
            model=model,
            raw_data_root=data_root / "forecast-hub",
            timeseries_output_path=data_root / "forecast-hub" / "timeseries-common.csv",
        )

    @property
    def raw_path(self):
        return self.raw_data_root / self.RAW_CSV_FILENAME

    def write_version_file(self) -> None:
        stamp = datetime.datetime.utcnow().isoformat()
        version_path = self.raw_data_root / "version.txt"
        with version_path.open("w+") as vf:
            vf.write(f"Updated on {stamp}")

    def update_source_data(self):
        """
        See https://github.com/reichlab/zoltpy/tree/master for instructions.

        Note: Requires environment variables for Z_USERNAME and Z_PASSWORD with correct
        permissions.
        """
        _logger.info(f"Updating version file with ForecastHub revision")

        conn = zoltpy.util.authenticate()
        ensemble = zoltpy.util.download_forecast(
            conn, "COVID-19 Forecasts", self.model.value, self.FORECAST_DATE
        )
        df = zoltpy.util.dataframe_from_json_io_dict(ensemble)
        df.to_csv(self.raw_path, index=False)
        self.write_version_file()

    def load_source_data(self) -> pd.DataFrame:
        _logger.info("Updating ForecastHub Ensemble dataset.")
        data = pd.read_csv(self.raw_path)
        # data = helpers.rename_fields(data, Fields, set(), _logger)
        return data

    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        df = data.rename(columns={"unit": "region_id"}, inplace=False)
        df["forecast_date"] = pd.to_datetime(self.FORECAST_DATE)
        df["model_abbr"] = self.model.value

        # Target information is provided as strings of form "X wk ahead inc/cum death/cases"
        # Take the first split (X weeks) and calculate the datetime for the prediction
        df["target_date"] = df.apply(
            lambda x: x.forecast_date + pd.Timedelta(weeks=int(x.target.split(" ")[0])),
            axis="columns",
        )
        # Take the final split (death/cases) and use that as target type
        df["target_type"] = df.target.str.split(" ").str[-1]
        # Rename using the CommonFields values
        df["target_type"] = df["target_type"].replace(
            {"death": CommonFields.DEATHS.value, "case": CommonFields.CASES.value}
        )

        masks = [
            df["region_id"] != "US",  # Drop the national forecast
            df["class"] == "point",  # Only keep point forecasts
            df["target"].str.contains("inc"),
            # Only keep incident targets (drop cumulative targets)
            df["target_date"] <= pd.to_datetime(self.FORECAST_DATE) + pd.Timedelta(weeks=4)
            # Time Horizon
        ]
        mask = np.logical_and.reduce(masks)
        COLUMNS = [
            "model_abbr",
            "region_id",
            "forecast_date",
            "target_date",
            "target_type",
            "value",
        ]

        return df[mask][COLUMNS].reset_index()


@click.command()
@click.option("--fetch/--no-fetch", default=True)
def main(fetch: bool):
    common_init.configure_logging()
    transformer = ForecastHubUpdater.make_with_data_root(ForecastModel.ENSEMBLE, DATA_ROOT)
    if fetch:
        _logger.info("Fetching new data.")
        transformer.update_source_data()

    data = transformer.load_source_data()
    data = transformer.transform(data)
    # common_df.write_csv(data, transformer.timeseries_output_path, _logger)
    data.to_csv(transformer.timeseries_output_path, index=False)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
