from covidactnow.datapublic.common_fields import CommonFields


def test_import_worked():
    assert CommonFields.DATE == "date"
