import pydantic
import pathlib
import pandas as pd


class CensusData(pydantic.BaseModel):

    data: pd.DataFrame

    class Config:
        arbitrary_types_allowed = True

    def get_county_data(self, state, county_name):
        matching = self.data[(self.data.state == state) & (self.data.county == county_name)]

        if len(matching) != 1:
            return None

        return matching.to_dict(orient="records")[0]


def load_county_fips_data(fips_csv: pathlib.Path) -> pd.DataFrame:
    df = pd.read_csv(fips_csv, dtype={"fips": str})
    df["fips"] = df.fips.str.zfill(5)
    return CensusData(data=df)
