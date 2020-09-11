import enum
from typing import Any

import click
import pandas as pd
import numpy as np
import structlog
import pathlib
import pydantic
import datetime

import zoltpy.util
from zoltpy.cdc_io import YYYY_MM_DD_DATE_FORMAT

from covidactnow.datapublic import common_init, common_df
from scripts import helpers


from covidactnow.datapublic.common_fields import (
    GetByValueMixin,
    CommonFields,
    FieldNameAndCommonField,
)

DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"

_logger = structlog.get_logger(__name__)


class ForecastModel(enum.Enum):
    """"""

    ENSEMBLE = "COVIDhub-ensemble"
    BASELINE = "COVIDhub-baseline"
    GOOGLE = "Google_Harvard-CPF"


class Fields(GetByValueMixin, FieldNameAndCommonField, enum.Enum):
    MODEL_ABBR = "model_abbr", CommonFields.MODEL_ABBR
    REGION = "unit", CommonFields.FIPS
    FORECAST_DATE = "forecast_date", CommonFields.FORECAST_DATE
    TARGET_DATE = "target_date", CommonFields.DATE
    QUANTILE = "quantile", CommonFields.QUANTILE
    WEEKLY_NEW_CASES = "case", CommonFields.WEEKLY_NEW_CASES
    WEEKLY_NEW_DEATHS = "death", CommonFields.WEEKLY_NEW_DEATHS


class ForecastHubUpdater(pydantic.BaseModel):
    """Updates Forecast Lab Data Set with the Latest Available Forecast
    """

    FORECAST_PROJECT_NAME = "COVID-19 Forecasts"
    RAW_CSV_FILENAME = "raw.csv"

    conn: Any  # A valid zoltpy connection

    model: ForecastModel  # The model to cache from Zoltar

    raw_data_root: pathlib.Path

    timeseries_output_path: pathlib.Path

    @classmethod
    def make_with_data_root(
        cls, model: ForecastModel, conn: Any, data_root: pathlib.Path,
    ) -> "ForecastHubUpdater":
        return cls(
            model=model,
            conn=conn,
            raw_data_root=data_root / "forecast-hub",
            timeseries_output_path=data_root / "forecast-hub" / "timeseries-common.csv",
        )

    @property
    def raw_path(self):
        return self.raw_data_root / self.RAW_CSV_FILENAME

    def write_version_file(self, forecast_date) -> None:
        stamp = datetime.datetime.utcnow().isoformat()
        version_path = self.raw_data_root / "version.txt"
        with version_path.open("w") as vf:
            vf.write(f"Updated on {stamp}\n")
            vf.write(f"Using forecast from {forecast_date}\n")

    def update_source_data(self):
        """
        See https://github.com/reichlab/zoltpy/tree/master for instructions.

        Note: Requires environment variables for Z_USERNAME and Z_PASSWORD with correct
        permissions.
        """
        _logger.info(f"Updating {self.model.name} from ForecastHub")
        latest_forecast_date = get_latest_forecast_date(
            self.conn, self.FORECAST_PROJECT_NAME, self.model.value
        )
        ensemble = zoltpy.util.download_forecast(
            self.conn, self.FORECAST_PROJECT_NAME, self.model.value, latest_forecast_date
        )
        df = zoltpy.util.dataframe_from_json_io_dict(ensemble)
        df["forecast_date"] = pd.to_datetime(latest_forecast_date)
        df["model_abbr"] = self.model.value
        df.to_csv(self.raw_path, index=False)
        self.write_version_file(forecast_date=latest_forecast_date)

    def load_source_data(self) -> pd.DataFrame:
        _logger.info("Updating ForecastHub Ensemble dataset.")
        data = pd.read_csv(
            self.raw_path, parse_dates=["forecast_date"], dtype={"unit": str}, low_memory=False
        )
        return data

    @staticmethod
    def transform(df: pd.DataFrame) -> pd.DataFrame:
        df["target_date"] = df.apply(
            lambda x: x.forecast_date + pd.Timedelta(weeks=int(x.target.split(" ")[0])),
            axis="columns",
        )
        # The targets have the form "X wk inc/cum cases/deaths"
        # Take the final split (death/cases) and use that as target type
        df["target_type"] = df.target.str.split(" ").str[-1]
        # Take the penultimate split (inc/cum) and use that as aggregation type
        df["target_summation"] = df.target.str.split(" ").str[-2]

        masks = [
            df["unit"] != "US",  # Drop the national forecast
            df["quantile"].notna(),  # Point forecasts are duplicate of quantile = 0.5
            df["target_summation"] == "inc",  # Only return incidence values
            # Some models return both incidence and cumulative values
            # Only keep incidence targets (drop cumulative targets)
            df["target_date"] <= df["forecast_date"] + pd.Timedelta(weeks=4)
            # Time Horizon - Only keep up to 4 week forecasts.
            # Almost all forecasts only provide 4 wks.
        ]
        mask = np.logical_and.reduce(masks)

        # The raw data is in long form and we need to pivot this to create a column for
        # WEEKLY_NEW_CASES and WEEKLY_NEW_DEATHS. "target_type" has either death or cases. "value"
        # has the predicted value. The rest of the columns create a unique index. For right now only
        # one model and one forecast_date are being served, but we need to maintain the option of
        # multiple values.
        COLUMNS = [
            Fields.MODEL_ABBR,
            Fields.REGION,
            Fields.FORECAST_DATE,
            Fields.TARGET_DATE,
            "target_type",
            Fields.QUANTILE,
            "value",
        ]
        df = df[mask][COLUMNS].copy()
        df = df.set_index(
            [
                Fields.MODEL_ABBR,
                Fields.REGION,
                Fields.FORECAST_DATE,
                Fields.TARGET_DATE,
                Fields.QUANTILE,
            ]
        )
        pivot = df.pivot(columns="target_type")
        pivot = pivot.droplevel(level=0, axis=1).reset_index()
        # This cleans up a MultiIndex Column that is an artifact of the pivot in preparation for a
        # standard csv dump.

        # Rename and remove any columns without a CommonField
        data = helpers.rename_fields(pivot, Fields, set(), _logger)

        # Need to make the quantiles into a wide form for easier downstream processing
        # Mangling the column names into f"weekly_new_{cases/deaths}_{quantile}". This
        # would be a good candidate to handle in long/tidy-form and we could remove both pivots.
        # Using common_field because this is done after helpers.rename_fields
        wide_df = data.set_index(
            [
                Fields.REGION.common_field,
                Fields.TARGET_DATE.common_field,
                Fields.MODEL_ABBR.common_field,
                Fields.FORECAST_DATE.common_field,
            ]
        ).pivot(columns=Fields.QUANTILE.common_field)
        wide_df.columns = wide_df.columns = [
            x[0] + "_" + str(x[1]) for x in wide_df.columns.to_flat_index()
        ]
        wide_df = wide_df.reset_index()
        return wide_df


def get_latest_forecast_date(conn, project_name: str, model_abbr: str) -> str:
    """
    Return the date string 'YYYY-MM-DD' of the latest submitted forecast for a given model in a
    given zoltar project

    https://github.com/reichlab/zoltpy/issues/42

    Looking to have this live in Zoltpy so I am keeping this out of the class.

    Return the str date representation of the latest forecast if available, else the empty string.
    """

    project = [project for project in conn.projects if project.name == project_name][0]
    model = [model for model in project.models if model.abbreviation == model_abbr][0]

    timezero_dates = [
        datetime.datetime.strptime(forecast.timezero.timezero_date, YYYY_MM_DD_DATE_FORMAT).date()
        for forecast in model.forecasts
    ]
    if timezero_dates:
        latest_forecast_date = str(max(timezero_dates))
        _logger.info(f"Latest forecast for {model_abbr} is {latest_forecast_date}")
        return latest_forecast_date
    else:
        _logger.info(f"No forecasts found for {model_abbr} in {project_name}")
        return ""


@click.command()
@click.option("--fetch/--no-fetch", default=True)
def main(fetch: bool):
    common_init.configure_logging()
    connection = zoltpy.util.authenticate()
    transformer = ForecastHubUpdater.make_with_data_root(
        ForecastModel.ENSEMBLE, connection, DATA_ROOT
    )
    if fetch:
        _logger.info("Fetching new data.")
        transformer.update_source_data()

    data = transformer.load_source_data()
    data = transformer.transform(data)
    common_df.write_csv(data, transformer.timeseries_output_path, _logger)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter
