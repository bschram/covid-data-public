import pytest
import structlog

from scripts.update_aws_lake import AwsDataLakeTransformer, DATA_ROOT, AwsDataLakeCopier


@pytest.mark.skip(
    reason="test depends on having a copy of the AWS source data in the working copy, but we removed it to save space"
)
def test_load_something_from_google_survey():
    with structlog.testing.capture_logs() as logs:
        copier = AwsDataLakeCopier.make_with_data_root(DATA_ROOT)
        sources = dict(copier.get_sources())
    assert logs == []

    with structlog.testing.capture_logs() as logs:
        transformer = AwsDataLakeTransformer.make_with_data_root(DATA_ROOT)
        df = transformer.transform(sources["google-survey"], structlog.get_logger())

    assert [l["event"] for l in logs] == [
        "Dropping rows that did not merge by geo_value",
        "Loaded dataframe",
    ]
    assert not df.empty
    assert df.at[("06075", "2020-05-01"), "smoothed_cli"] > 0
    assert df.at[("45", "2020-05-01"), "smoothed_cli"] > 0
