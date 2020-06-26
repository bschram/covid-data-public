import pathlib
import re
import pandas as pd
import pydantic
import structlog
from covidactnow.datapublic import census_data_helpers
from covidactnow.datapublic import common_init


DATA_ROOT = pathlib.Path(__file__).parent.parent / "data"


class CountyNotFoundInCensusData(Exception):
    pass


class TexasTraumaServiceAreaFipsTransformer(pydantic.BaseModel):
    # Path to raw output of tsa -> county names from Texas law page
    # https://texreg.sos.state.tx.us/public/readtac$ext.TacPage?sl=T&app=9&p_dir=P&p_rloc=111068&p_tloc=14943&p_ploc=1&pg=3&p_tac=&ti=25&pt=1&ch=157&rl=122
    raw_tsa_scraped_path: pathlib.Path

    # FIPS for each county, by name
    county_fips_csv: pathlib.Path

    class Config:
        arbitrary_types_allowed = True

    @staticmethod
    def make_with_data_root(data_root: pathlib.Path):
        return TexasTraumaServiceAreaFipsTransformer(
            county_fips_csv=data_root / "misc" / "fips_population.csv",
            raw_tsa_scraped_path=data_root / "states" / "tx" / "tx_tsa_to_county_map.txt",
        )

    def transform(self) -> pd.DataFrame:
        state = "TX"
        tsa_regions = self.raw_tsa_scraped_path.read_text()

        census_data = census_data_helpers.load_county_fips_data(self.county_fips_csv)

        data = []
        for line in tsa_regions.split("\n"):
            if not line:
                continue

            area, county_names = re.match(r".+Area ([A-Z]) - (.*)[;.]", line).groups()
            counties = county_names.split(", ")
            for county in counties:
                # TODO(chris): Find better way match county to fips.  I believe there are some
                # python packages that do a lot of the heavy lifting.
                if county == "Raines":
                    county = "Rains"
                if county == "Dewitt":
                    county = "DeWitt"

                county = county + " County"
                county_data = census_data.get_county_data(state, county)
                if not county_data:
                    raise CountyNotFoundInCensusData()

                data.append({"fips": county_data["fips"], "state": state, "tsa_region": area})

        return pd.DataFrame(data)


if __name__ == "__main__":
    common_init.configure_logging()
    log = structlog.get_logger()

    transformer = TexasTraumaServiceAreaFipsTransformer.make_with_data_root(DATA_ROOT)
    output_csv = DATA_ROOT / "states" / "tx" / "tx_tsa_region_fips_map.csv"
    output = transformer.transform()
    output.to_csv(output_csv, index=False)

    log.info(f"Successfully wrote TSA -> FIPS map", output_file=str(output_csv))
