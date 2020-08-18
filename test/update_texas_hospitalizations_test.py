import io
import pandas as pd
from covidactnow.datapublic.common_fields import CommonFields
from scripts.update_texas_tsa_hospitalizations import TexasTraumaServiceAreaHospitalizationsUpdater


def test_fixing_dates():
    data_buf = io.StringIO(
        "TSA ID,TSA AREA,2020-04-12.x,2020-04-12.y,2020-04-13\n"
        "A.,Amarillo,13,13,14\n"
        "B.,Lubbock,13,13,15\n"
    )
    data = pd.read_csv(data_buf)
    results = TexasTraumaServiceAreaHospitalizationsUpdater.parse_data(
        data, CommonFields.CURRENT_HOSPITALIZED
    )
    data_buf = io.StringIO(
        "TSA ID,TSA AREA,date,current_hospitalized\n"
        "A,Amarillo,2020-04-12,13\n"
        "A,Amarillo,2020-04-13,14\n"
        "B,Lubbock,2020-04-12,13\n"
        "B,Lubbock,2020-04-13,15\n"
    )

    expected = pd.read_csv(data_buf)
    pd.testing.assert_frame_equal(results, expected)
