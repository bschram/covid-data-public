import enum
import pathlib
import pandas as pd
import pydantic
import structlog
from covidactnow.datapublic import common_fields
from covidactnow.datapublic.common_fields import CommonFields
from covidactnow.datapublic import common_init
from covidactnow.datapublic import census_data_helpers
from covidactnow.datapublic import common_df


DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"
TSA_HOSPITALIZATIONS_URL = (
    "https://www.dshs.texas.gov/coronavirus/TexasCOVID-19HospitalizationsOverTimebyTSA.xlsx"
)


class Fields(common_fields.GetByValueMixin, common_fields.FieldNameAndCommonField, enum.Enum):
    DATE = "date", CommonFields.DATE
    FIPS = CommonFields.FIPS, CommonFields.FIPS
    CURRENT_HOSPITALIZED = CommonFields.CURRENT_HOSPITALIZED, CommonFields.CURRENT_HOSPITALIZED


def build_hospitalizations_spread_by_population(hosp_by_tsa_date, census_data, tsa_to_fips):
    pop_ratio_field = "population_ratio"
    tsa_to_fips[pop_ratio_field] = (
        tsa_to_fips.merge(
            census_data[[CommonFields.FIPS, CommonFields.POPULATION]], on=CommonFields.FIPS,
        )
        .groupby("tsa_region")[CommonFields.POPULATION]
        .apply(lambda x: x / x.sum())
    )

    df = hosp_by_tsa_date.merge(
        tsa_to_fips[["tsa_region", Fields.FIPS, pop_ratio_field, CommonFields.STATE]],
        left_on="TSA ID",
        right_on="tsa_region",
    )
    df[Fields.CURRENT_HOSPITALIZED] = (
        df[Fields.CURRENT_HOSPITALIZED] * df[pop_ratio_field]
    ).round()

    return df[[Fields.DATE, Fields.FIPS, Fields.CURRENT_HOSPITALIZED, CommonFields.STATE]]


class TexasFipsHospitalizationsUpdater(pydantic.BaseModel):
    """Spreads TSA hospitalization data accross fips, spreading by fips population."""

    hospitalizations_by_tsa_csv: pathlib.Path

    county_fips_csv: pathlib.Path

    tsa_to_fips_csv: pathlib.Path

    class Config:
        arbitrary_types_allowed = True

    @staticmethod
    def make_with_data_root(data_root: pathlib.Path,) -> "TexasFipsHospitalizationsUpdater":
        return TexasFipsHospitalizationsUpdater(
            hospitalizations_by_tsa_csv=data_root / "states" / "tx" / "tx_tsa_hospitalizations.csv",
            county_fips_csv=data_root / "misc" / "fips_population.csv",
            tsa_to_fips_csv=data_root / "states" / "tx" / "tx_tsa_region_fips_map.csv",
        )

    def update(self):
        hosp_by_tsa_date = pd.read_csv(self.hospitalizations_by_tsa_csv, dtype={Fields.FIPS: str})
        census_data = census_data_helpers.load_county_fips_data(self.county_fips_csv)
        tsa_to_fips = pd.read_csv(self.tsa_to_fips_csv, dtype={Fields.FIPS: str})
        output = build_hospitalizations_spread_by_population(
            hosp_by_tsa_date, census_data.data, tsa_to_fips
        )
        output[CommonFields.AGGREGATE_LEVEL] = "county"
        output[CommonFields.COUNTRY] = "USA"
        return output


if __name__ == "__main__":
    common_init.configure_logging()
    log = structlog.get_logger()
    updater = TexasFipsHospitalizationsUpdater.make_with_data_root(DATA_ROOT)
    data = updater.update()
    output_csv = DATA_ROOT / "states" / "tx" / "tx_fips_hospitalizations.csv"
    common_df.write_csv(data, output_csv, log)
