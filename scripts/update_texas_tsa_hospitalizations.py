import enum
import pathlib
import pandas as pd
import datetime
import dateutil.parser
import pydantic
import structlog
from covidactnow.datapublic import common_fields
from covidactnow.datapublic.common_fields import CommonFields
from covidactnow.datapublic import common_init

DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"
TSA_HOSPITALIZATIONS_URL = (
    "https://www.dshs.texas.gov/coronavirus/TexasCOVID-19HospitalizationsOverTimebyTSA.xlsx"
)


class Fields(common_fields.GetByValueMixin, common_fields.FieldNameAndCommonField, enum.Enum):

    TSA_REGION_ID = "TSA ID", None
    TSA_AREA = "TSA AREA", None
    DATE = "date", CommonFields.DATE
    CURRENT_HOSPITALIZED = CommonFields.CURRENT_HOSPITALIZED, CommonFields.CURRENT_HOSPITALIZED


class TexasTraumaServiceAreaHospitalizationsUpdater(pydantic.BaseModel):
    """Updates latest Trauma Service Area hospitalizations."""

    output_csv: pathlib.Path

    class Config:
        arbitrary_types_allowed = True

    @staticmethod
    def make_with_data_root(
        data_root: pathlib.Path,
    ) -> "TexasTraumaServiceAreaHospitalizationsUpdater":
        return TexasTraumaServiceAreaHospitalizationsUpdater(
            output_csv=data_root / "states" / "tx" / "tx_tsa_hospitalizations.csv"
        )

    def update(self):
        data = pd.read_excel(TSA_HOSPITALIZATIONS_URL, header=2)
        index = [Fields.TSA_REGION_ID, Fields.TSA_AREA]

        # Fixing erroneous data on 08/08/2020.  The column is being interpreted as
        # a datetime, so converting back to string to keep consistent with rest of columns.
        matched_datetime = datetime.datetime.fromisoformat("2020-08-08 00:00:00")
        data = data.rename({matched_datetime: "2020-08-08"}, axis="columns")

        data = (
            data.set_index(index)
            .stack()
            .reset_index()
            .rename({"level_2": Fields.DATE, 0: Fields.CURRENT_HOSPITALIZED}, axis=1)
        )
        data[Fields.DATE] = data[Fields.DATE].str.lstrip("Hospitalizations ")
        data[Fields.DATE] = data[Fields.DATE].apply(
            lambda x: dateutil.parser.parse(x).date().isoformat()
        )
        # Drop all state level values
        data = data.loc[data[Fields.TSA_REGION_ID].notnull(), :]
        data[Fields.TSA_REGION_ID] = data[Fields.TSA_REGION_ID].apply(lambda x: x.rstrip("."))
        return data


if __name__ == "__main__":
    common_init.configure_logging()
    log = structlog.get_logger()
    updater = TexasTraumaServiceAreaHospitalizationsUpdater.make_with_data_root(DATA_ROOT)
    data = updater.update()
    data.to_csv(updater.output_csv, index=False)
    log.info("Updated TSA Hospitalizations", output_csv=str(updater.output_csv))
